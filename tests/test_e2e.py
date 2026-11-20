"""End-to-end tests: real origin + proxy processes, real HTTP.

Run from the repo root:  python3 -m pytest tests/ -v
Requires: fastapi, uvicorn, aiohttp (app deps) + httpx, pytest (test deps).
"""

from __future__ import annotations

import hashlib
import os
import socket
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

ROOT = Path(__file__).resolve().parent.parent
MIB = 1024 * 1024
PKG_BIG = "pkg-32m.bin"
PKG_SMALL = "pkg-1m.bin"


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def wait_health(url: str, proc: subprocess.Popen, timeout: float = 30.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"server exited early with code {proc.returncode}")
        try:
            r = httpx.get(url, timeout=2.0)
            if r.status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.1)
    raise RuntimeError(f"server at {url} did not become healthy")


def start_server(appdir: str, port: int, env_extra: dict, log: Path,
                 control: Path) -> subprocess.Popen:
    env = dict(os.environ)
    env.update(env_extra)
    proc = subprocess.Popen(
        [sys.executable, str(ROOT / "tests" / "run_server.py"),
         str(ROOT / appdir), str(port), str(control)],
        env=env,
        stdout=log.open("ab"),
        stderr=subprocess.STDOUT,
    )
    wait_health(f"http://127.0.0.1:{port}/healthz", proc)
    return proc


def stop(proc: subprocess.Popen, control: Path) -> None:
    """Ask the server to exit via its control file (signals are blocked here)."""
    if proc.poll() is None:
        control.touch(exist_ok=True)
        try:
            proc.wait(10)
        except subprocess.TimeoutExpired:
            try:
                proc.kill()
            except PermissionError:
                pass
            try:
                proc.wait(5)
            except subprocess.TimeoutExpired:
                pass


def client(timeout_read: float = 120.0) -> httpx.Client:
    return httpx.Client(timeout=httpx.Timeout(5.0, read=timeout_read, write=30.0, pool=5.0))


def download_bytes(base: str, path: str) -> bytes:
    with client() as c:
        r = c.get(f"{base}/packages/{path}")
        r.raise_for_status()
        return r.content


def sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def origin_stats(origin_url: str) -> dict:
    with client() as c:
        return c.get(f"{origin_url}/control/stats").json()


def origin_reset(origin_url: str) -> None:
    with client() as c:
        c.post(f"{origin_url}/control/reset")


def set_behavior(origin_url: str, delay_ms: int = 0, truncate: int | None = None,
                 chunk_size: int = 256 * 1024) -> None:
    with client() as c:
        r = c.post(
            f"{origin_url}/control/behavior",
            json={
                "chunk_delay_ms": delay_ms,
                "truncate_after_bytes": truncate,
                "chunk_size": chunk_size,
            },
        )
        r.raise_for_status()


@pytest.fixture(scope="session")
def origin(tmp_path_factory):
    pkg_dir = tmp_path_factory.mktemp("pkgs")
    log_dir = tmp_path_factory.mktemp("logs")
    log = log_dir / "origin.log"
    control = log_dir / "origin.stop"
    port = free_port()
    proc = start_server("origin", port, {"PKG_DIR": str(pkg_dir)}, log, control)
    yield SimpleNamespace(url=f"http://127.0.0.1:{port}", port=port, dir=pkg_dir, log=log)
    stop(proc, control)


class ProxyRunner:
    def __init__(self, origin_port: int, cache_dir: Path, log: Path, control: Path):
        self.origin_port = origin_port
        self.cache_dir = cache_dir
        self.log = log
        self.control = control
        self.proc: subprocess.Popen | None = None
        self.url: str = ""

    def start(self) -> None:
        port = free_port()
        if self.control.exists():
            self.control.unlink()
        self.proc = start_server(
            "proxy",
            port,
            {
                "ORIGIN_BASE_URL": f"http://127.0.0.1:{self.origin_port}",
                "CACHE_DIR": str(self.cache_dir),
            },
            self.log,
            self.control,
        )
        self.url = f"http://127.0.0.1:{port}"

    def restart(self) -> None:
        stop(self.proc, self.control)
        self.start()

    def stop(self) -> None:
        if self.proc:
            stop(self.proc, self.control)

    def rss_bytes(self) -> int:
        with open(f"/proc/{self.proc.pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS"):
                    return int(line.split()[1]) * 1024
        raise RuntimeError("VmRSS not found")


@pytest.fixture()
def proxy(origin, tmp_path):
    runner = ProxyRunner(origin.port, tmp_path / "cache", tmp_path / "proxy.log",
                         tmp_path / "proxy.stop")
    runner.start()
    yield runner
    runner.stop()


# --------------------------------------------------------------------- #
# tests                                                                  #
# --------------------------------------------------------------------- #

def test_basic_download_then_cache_hit(proxy, origin):
    origin_reset(origin.url)
    body1 = download_bytes(proxy.url, PKG_SMALL)
    stats = origin_stats(origin.url)
    assert stats["paths"][PKG_SMALL] == 1
    assert sha256(body1) == stats["packages"][PKG_SMALL]["sha256"]

    with client() as c:
        r2 = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
    assert r2.headers["x-cache"] == "HIT"
    assert r2.content == body1
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 1  # no second fetch


def test_concurrent_miss_single_fill(proxy, origin):
    set_behavior(origin.url, delay_ms=80, chunk_size=64 * 1024)  # ~1.2s fill window
    try:
        origin_reset(origin.url)
        with ThreadPoolExecutor(8) as ex:
            bodies = list(ex.map(lambda _: download_bytes(proxy.url, PKG_SMALL), range(8)))
        assert origin_stats(origin.url)["total_hits"] == 1
        assert all(b == bodies[0] for b in bodies)
        assert len(bodies[0]) == MIB
    finally:
        set_behavior(origin.url)


def test_first_chunk_arrives_before_full_package(proxy, origin):
    # 32MiB at 100ms per 256KiB chunk -> full fill takes ~13s.
    set_behavior(origin.url, delay_ms=100, chunk_size=256 * 1024)
    try:
        t0 = time.monotonic()
        with client(timeout_read=60).stream("GET", f"{proxy.url}/packages/{PKG_BIG}") as r:
            first = next(r.iter_bytes(256 * 1024))
            ttfb = time.monotonic() - t0
            assert len(first) > 0
        assert ttfb < 3.0, f"first chunk took {ttfb:.2f}s"
    finally:
        set_behavior(origin.url)
        with client() as c:
            c.post(f"{proxy.url}/purge/{PKG_BIG}")


def test_slow_client_blocks_nobody(proxy, origin):
    # Fill pace ~20ms/256KiB -> ~2.6s for 32MiB; slow client crawls far slower.
    set_behavior(origin.url, delay_ms=20, chunk_size=256 * 1024)
    try:
        origin_reset(origin.url)
        result: dict = {}

        def slow_reader():
            with client(timeout_read=300).stream(
                "GET", f"{proxy.url}/packages/{PKG_BIG}"
            ) as r:
                got = 0
                for chunk in r.iter_bytes(64 * 1024):
                    got += len(chunk)
                    time.sleep(0.2)
                    if got >= 5 * 64 * 1024:
                        break
                result["slow_got"] = got

        t = threading.Thread(target=slow_reader)
        t.start()
        time.sleep(0.3)  # slow client attaches first
        t0 = time.monotonic()
        fast = download_bytes(proxy.url, PKG_BIG)
        fast_dur = time.monotonic() - t0
        t.join(60)

        assert len(fast) == 32 * MIB
        assert fast_dur < 15, f"fast client dragged to {fast_dur:.1f}s by slow client"
        assert origin_stats(origin.url)["total_hits"] == 1  # shared single fill

        # Fill completed in the background despite the slow client leaving.
        with client() as c:
            r = c.get(f"{proxy.url}/packages/{PKG_BIG}")
        assert r.headers["x-cache"] == "HIT"
        assert sha256(r.content) == origin_stats(origin.url)["packages"][PKG_BIG]["sha256"]
    finally:
        set_behavior(origin.url)


def test_purge_refills_and_serves_new_content(proxy, origin):
    origin_reset(origin.url)
    body1 = download_bytes(proxy.url, PKG_SMALL)
    sha1 = sha256(body1)

    with client() as c:
        new = c.post(f"{origin.url}/control/content/{PKG_SMALL}").json()
    assert new["sha256"] != sha1

    # Still cached: old content, no new origin fetch.
    assert sha256(download_bytes(proxy.url, PKG_SMALL)) == sha1
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 1

    with client() as c:
        pr = c.post(f"{proxy.url}/purge/{PKG_SMALL}")
    assert pr.status_code == 200 and pr.json()["was_cached"] is True

    # First download after purge re-fills exactly once; followers share it.
    with ThreadPoolExecutor(6) as ex:
        bodies = list(ex.map(lambda _: download_bytes(proxy.url, PKG_SMALL), range(6)))
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 2
    assert all(sha256(b) == new["sha256"] for b in bodies)


def test_purge_during_fill_keeps_old_download_alive(proxy, origin):
    set_behavior(origin.url, delay_ms=60, chunk_size=64 * 1024)  # ~1s fill
    try:
        origin_reset(origin.url)
        result: dict = {}

        def reader_a():
            with client(timeout_read=60).stream(
                "GET", f"{proxy.url}/packages/{PKG_SMALL}"
            ) as r:
                chunks = []
                for chunk in r.iter_bytes(64 * 1024):
                    chunks.append(chunk)
                    time.sleep(0.03)
                result["a"] = b"".join(chunks)

        ta = threading.Thread(target=reader_a)
        ta.start()
        time.sleep(0.3)  # A is mid-fill on generation 1

        with client() as c:
            pr = c.post(f"{proxy.url}/purge/{PKG_SMALL}")
        assert pr.json()["was_cached"] is True

        body_b = download_bytes(proxy.url, PKG_SMALL)  # triggers generation 2
        ta.join(30)

        assert origin_stats(origin.url)["paths"][PKG_SMALL] == 2  # old fill + new fill
        assert len(result["a"]) == MIB  # old download completed in full
        assert sha256(result["a"]) == sha256(body_b)
    finally:
        set_behavior(origin.url)


def test_upstream_disconnect_fails_all_and_caches_nothing(proxy, origin):
    set_behavior(origin.url, delay_ms=50, chunk_size=64 * 1024, truncate=200_000)
    try:
        origin_reset(origin.url)

        def expect_failure():
            with pytest.raises(httpx.HTTPError):
                download_bytes(proxy.url, PKG_SMALL)

        with ThreadPoolExecutor(4) as ex:
            list(ex.map(lambda _: expect_failure(), range(4)))

        assert origin_stats(origin.url)["total_hits"] == 1  # single shared fill
        with client() as c:
            status = c.get(f"{proxy.url}/cache/status").json()
        assert PKG_SMALL not in status
        leftovers = list(proxy.cache_dir.glob(f"{PKG_SMALL}*"))
        assert leftovers == [], f"half package left behind: {leftovers}"
    finally:
        set_behavior(origin.url)

    # Origin healthy again -> retry succeeds and caches.
    body = download_bytes(proxy.url, PKG_SMALL)
    assert len(body) == MIB
    assert origin_stats(origin.url)["total_hits"] == 2


def test_restart_keeps_complete_cache_and_cleans_partials(proxy, origin):
    origin_reset(origin.url)
    body1 = download_bytes(proxy.url, PKG_SMALL)
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 1

    stray = proxy.cache_dir / f"{PKG_SMALL}.part-deadbeef"
    stray.write_bytes(b"x" * 100)
    orphan_meta = proxy.cache_dir / "ghost.meta"
    orphan_meta.write_text("{}")

    proxy.restart()

    with client() as c:
        r = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
    assert r.headers["x-cache"] == "HIT"
    assert r.content == body1
    assert origin_stats(origin.url)["paths"][PKG_SMALL] == 1  # served from disk cache
    assert not stray.exists()
    assert not orphan_meta.exists()


def test_memory_does_not_grow_with_package_size(proxy, origin):
    rss0 = proxy.rss_bytes()
    body = download_bytes(proxy.url, PKG_BIG)
    assert len(body) == 32 * MIB
    time.sleep(0.5)
    rss1 = proxy.rss_bytes()
    delta = rss1 - rss0
    print(f"\nproxy RSS delta while streaming 32MiB: {delta / MIB:.1f} MiB")
    # Buffering the whole package would cost >=32MiB; streaming stays far below.
    assert delta < 24 * MIB


def test_missing_package_returns_404_and_no_negative_cache(proxy, origin):
    origin_reset(origin.url)
    with client() as c:
        r1 = c.get(f"{proxy.url}/packages/nope.bin")
        r2 = c.get(f"{proxy.url}/packages/nope.bin")
    assert r1.status_code == 404 and r2.status_code == 404
    assert origin_stats(origin.url)["paths"]["nope.bin"] == 2


def test_path_traversal_rejected(proxy):
    with client() as c:
        r = c.get(f"{proxy.url}/packages/..%2f..%2fetc%2fpasswd")
    assert r.status_code in (400, 404)


# --------------------------------------------------------------------- #
# per-build billing                                                     #
# --------------------------------------------------------------------- #

def download_with_build(base: str, path: str, build_id: str,
                        timeout_read: float = 120.0) -> httpx.Response:
    with client(timeout_read=timeout_read) as c:
        r = c.get(f"{base}/packages/{path}", headers={"X-Build-ID": build_id})
        r.raise_for_status()
        return r


def settle(proxy_url: str, build_id: str) -> httpx.Response:
    with client() as c:
        return c.post(f"{proxy_url}/billing/settle/{build_id}")


def settle_until_final(proxy_url: str, build_id: str, timeout: float = 15.0) -> dict:
    """Settle, tolerating 202-processing responses until the bill is final."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        r = settle(proxy_url, build_id)
        if r.status_code == 200:
            return r.json()
        assert r.status_code == 202, f"unexpected settle response: {r.status_code}"
        last = r.json()
        assert last["status"] == "processing"
        time.sleep(0.2)
    raise AssertionError(f"settlement for {build_id} never finalized: {last}")


def test_billing_untagged_downloads_are_not_billed(proxy, origin):
    origin_reset(origin.url)
    body = download_bytes(proxy.url, PKG_SMALL)
    assert len(body) == MIB
    with client() as c:
        assert c.get(f"{proxy.url}/billing/builds").json() == {}
        assert c.get(f"{proxy.url}/billing/builds/never-existed").status_code == 404


def test_billing_miss_and_hit_accounting_then_frozen_bill(proxy, origin):
    origin_reset(origin.url)
    r1 = download_with_build(proxy.url, PKG_SMALL, "build-x")  # MISS: fill billed
    assert r1.headers["x-cache"] == "MISS"
    r2 = download_with_build(proxy.url, PKG_SMALL, "build-x")  # HIT: downstream only
    assert r2.headers["x-cache"] == "HIT"

    # Untagged traffic is served but never billed.
    download_bytes(proxy.url, PKG_SMALL)
    with client() as c:
        builds = c.get(f"{proxy.url}/billing/builds").json()
    assert list(builds) == ["build-x"]

    bill = settle_until_final(proxy.url, "build-x")
    assert bill["status"] == "final"
    assert bill["packages"][PKG_SMALL] == {
        "downstream_bytes": 2 * MIB,  # one MISS + one HIT download
        "origin_bytes": MIB,          # only the MISS pulled from the origin
    }
    assert bill["totals"] == {"downstream_bytes": 2 * MIB, "origin_bytes": MIB}

    # Every later call returns the identical bill.
    assert settle(proxy.url, "build-x").json() == bill

    # Sealed: new tagged downloads are rejected, untagged still served.
    with client() as c:
        rj = c.get(f"{proxy.url}/packages/{PKG_SMALL}", headers={"X-Build-ID": "build-x"})
        assert rj.status_code == 409
        ru = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
        assert ru.status_code == 200 and len(ru.content) == MIB


def test_billing_settle_processing_then_final_and_sealed(proxy, origin):
    set_behavior(origin.url, delay_ms=60, chunk_size=64 * 1024)  # ~1s fill
    try:
        origin_reset(origin.url)
        result: dict = {}

        def reader():
            result["body"] = download_with_build(proxy.url, PKG_SMALL, "build-slow").content

        t = threading.Thread(target=reader)
        t.start()
        time.sleep(0.3)  # download accepted, streaming in progress

        r1 = settle(proxy.url, "build-slow")
        assert r1.status_code == 202
        assert r1.json()["status"] == "processing"
        assert r1.json()["downloads_in_flight"] >= 1

        # First settle sealed the build even while processing.
        with client() as c:
            rj = c.get(
                f"{proxy.url}/packages/{PKG_SMALL}", headers={"X-Build-ID": "build-slow"}
            )
            assert rj.status_code == 409
            # Untagged downloads are unaffected by the seal.
            ru = c.get(f"{proxy.url}/packages/{PKG_SMALL}")
            assert ru.status_code == 200 and len(ru.content) == MIB

        t.join(30)
        assert len(result["body"]) == MIB

        bill = settle_until_final(proxy.url, "build-slow")
        assert bill["packages"][PKG_SMALL]["downstream_bytes"] == MIB
        assert bill["packages"][PKG_SMALL]["origin_bytes"] == MIB
        assert settle(proxy.url, "build-slow").json() == bill
    finally:
        set_behavior(origin.url)


def test_billing_shared_fill_initiator_disconnects_midway(proxy, origin):
    """The required scenario: a shared origin fill billed to the initiating
    build, whose own download disconnects midway through the fill."""
    set_behavior(origin.url, delay_ms=100, chunk_size=64 * 1024)  # ~1.6s fill
    try:
        origin_reset(origin.url)
        result: dict = {}

        def initiator():
            with client(timeout_read=60) as c:
                with c.stream(
                    "GET",
                    f"{proxy.url}/packages/{PKG_SMALL}",
                    headers={"X-Build-ID": "build-init"},
                ) as r:
                    got = 0
                    for chunk in r.iter_bytes(64 * 1024):
                        got += len(chunk)
                        if got >= 3 * 64 * 1024:
                            break  # disconnect after ~3 chunks
                    result["init_got"] = got

        def follower():
            # Attaches to the same in-flight fill and reads it to the end.
            result["follower"] = download_with_build(
                proxy.url, PKG_SMALL, "build-follow"
            ).content

        ta = threading.Thread(target=initiator)
        tb = threading.Thread(target=follower)
        ta.start()
        time.sleep(0.15)  # build-init opens the fill first
        tb.start()
        ta.join(30)  # initiator disconnects after ~3 chunks, fill keeps going

        assert result["init_got"] >= 3 * 64 * 1024

        # The initiator is gone but its fill is still running: processing.
        r1 = settle(proxy.url, "build-init")
        assert r1.status_code == 202
        assert r1.json()["status"] == "processing"
        assert r1.json()["fills_in_flight"] == 1

        tb.join(30)
        assert len(result["follower"]) == MIB
        assert origin_stats(origin.url)["total_hits"] == 1  # one shared fill

        bill_a = settle_until_final(proxy.url, "build-init")
        pa = bill_a["packages"][PKG_SMALL]
        # The whole shared fill is billed to the initiating build, including
        # the bytes the origin sent after the initiator disconnected.
        assert pa["origin_bytes"] == MIB
        # Its own downstream stopped where the connection broke.
        assert 3 * 64 * 1024 <= pa["downstream_bytes"] < MIB

        bill_b = settle_until_final(proxy.url, "build-follow")
        pb = bill_b["packages"][PKG_SMALL]
        # The follower pays only its own downstream, no origin bytes.
        assert pb["origin_bytes"] == 0
        assert pb["downstream_bytes"] == MIB
    finally:
        set_behavior(origin.url)


def test_billing_failed_fill_still_counts_bytes(proxy, origin):
    """Origin aborts mid-body: bytes read/delivered before the break count."""
    set_behavior(origin.url, delay_ms=30, chunk_size=64 * 1024, truncate=200_000)
    try:
        origin_reset(origin.url)
        with pytest.raises(httpx.HTTPError):
            download_with_build(proxy.url, PKG_SMALL, "build-fail")

        bill = settle_until_final(proxy.url, "build-fail")
        p = bill["packages"][PKG_SMALL]
        assert p["origin_bytes"] == 200_000  # exactly what the proxy read
        assert 0 < p["downstream_bytes"] <= 200_000  # delivered before failing
        assert p["downstream_bytes"] < MIB
    finally:
        set_behavior(origin.url)
