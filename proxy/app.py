"""Single-process HTTP caching proxy for fixed-origin package downloads.

FastAPI (HTTP surface) + aiohttp (origin client). One uvicorn worker, all
state in this process; completed packages live on disk under CACHE_DIR.

Design notes
------------
* Single-flight: one fill task per path generation; concurrent misses attach
  to the same fill and read the growing ``.part-<gen>`` file from offset 0.
* Slow clients never block the fill or other clients: the fill only writes to
  disk; every reader tails the file at its own pace. Per-client memory is one
  read buffer, independent of package size.
* Purge unregisters the current generation. In-flight fills keep feeding
  their existing readers but are never re-registered; old files are unlinked
  once the last reader detaches.
* Origin failure fails every attached download and never leaves a cached
  half-package. Startup deletes leftover ``.part-*`` files and keeps complete
  packages.
* Per-build billing: downloads tagged with ``X-Build-ID`` accrue downstream
  body bytes (handed to the HTTP server) per package path; a fill's origin
  body bytes are billed per path to the build whose request initiated that
  fill. ``POST /billing/settle/{build_id}`` seals the build (new tagged
  downloads get 409), reports "processing" while its downloads or fills are
  still in flight, and freezes a final bill once all have finished. Bills
  are kept in this process only.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import aiohttp
from fastapi import FastAPI, Header
from fastapi.responses import JSONResponse, PlainTextResponse, StreamingResponse

ORIGIN_BASE_URL = os.environ.get("ORIGIN_BASE_URL", "http://127.0.0.1:9000").rstrip("/")
CACHE_DIR = Path(os.environ.get("CACHE_DIR", "./cache")).resolve()
CHUNK = 256 * 1024  # bytes per origin read / client write / disk read

_SEGMENT_RE = re.compile(r"\A[A-Za-z0-9._~!$&'()*+,;=:@-]+\Z")


def validate_key(raw: str) -> str:
    """Map a URL path to a safe relative cache key; reject traversal."""
    if not raw or raw.endswith("/"):
        raise ValueError("empty package path")
    parts = raw.split("/")
    for seg in parts:
        if seg in ("", ".", ".."):
            raise ValueError(f"illegal path segment: {seg!r}")
        if not _SEGMENT_RE.match(seg):
            raise ValueError(f"illegal characters in segment: {seg!r}")
    return "/".join(parts)


def _unlink_quiet(path: Path) -> None:
    try:
        path.unlink()
    except OSError:
        pass


class FillFailed(Exception):
    """Raised inside a client stream when the origin fill failed."""


# --------------------------------------------------------------------- #
# per-build billing                                                     #
# --------------------------------------------------------------------- #

MAX_BUILD_ID_LEN = 128


class BuildState:
    """Live counters for one build; the bill is frozen on final settlement."""

    def __init__(self) -> None:
        self.sealed = False            # settlement requested: reject new downloads
        self.downloads_in_flight = 0   # accepted downloads not finished yet
        self.fills_in_flight = 0       # origin fills initiated by this build
        self.downstream: dict[str, int] = {}  # path -> body bytes handed to clients
        self.origin: dict[str, int] = {}      # path -> body bytes read from origin
        self.bill: dict | None = None         # frozen bill once finalized

    def snapshot(self) -> dict:
        return {
            "sealed": self.sealed,
            "finalized": self.bill is not None,
            "downloads_in_flight": self.downloads_in_flight,
            "fills_in_flight": self.fills_in_flight,
            "downstream_bytes": dict(self.downstream),
            "origin_bytes": dict(self.origin),
        }


class Billing:
    """In-process per-build traffic accounting.

    * downstream: package body bytes the proxy hands to the HTTP server for
      a download tagged with X-Build-ID. Bytes already handed over count even
      if the transfer breaks midway.
    * origin: package body bytes the proxy actually reads from the origin.
      A shared fill is billed only to the build whose request initiated it;
      requests that attach later accrue only their own downstream bytes.
    * Untagged downloads are served normally and never billed.
    """

    def __init__(self) -> None:
        self.builds: dict[str, BuildState] = {}

    def _state(self, build_id: str) -> BuildState:
        st = self.builds.get(build_id)
        if st is None:
            st = BuildState()
            self.builds[build_id] = st
        return st

    # ---- download path (all sync: atomicity comes from the event loop) ---- #

    def try_begin_download(self, build_id: str) -> bool:
        """Admit a tagged download unless the build is sealed for settlement."""
        st = self._state(build_id)
        if st.sealed:
            return False
        st.downloads_in_flight += 1
        return True

    def end_download(self, build_id: str) -> None:
        self.builds[build_id].downloads_in_flight -= 1

    def add_downstream(self, build_id: str, key: str, n: int) -> None:
        st = self.builds[build_id]
        st.downstream[key] = st.downstream.get(key, 0) + n

    # ---- origin fill path ------------------------------------------------- #

    def begin_fill(self, build_id: str | None) -> None:
        if build_id is not None:
            self._state(build_id).fills_in_flight += 1

    def end_fill(self, build_id: str | None) -> None:
        if build_id is not None:
            self.builds[build_id].fills_in_flight -= 1

    def add_origin(self, build_id: str | None, key: str, n: int) -> None:
        if build_id is not None:
            st = self.builds[build_id]
            st.origin[key] = st.origin.get(key, 0) + n

    # ---- settlement -------------------------------------------------------- #

    def settle(self, build_id: str) -> tuple[dict | None, BuildState]:
        """Seal the build. Returns (bill, state); bill is None while downloads
        or fills initiated by this build are still in flight. Once finalized
        the same frozen bill object is returned on every call."""
        st = self._state(build_id)
        st.sealed = True
        if st.bill is not None:
            return st.bill, st
        if st.downloads_in_flight > 0 or st.fills_in_flight > 0:
            return None, st
        st.bill = self._freeze(build_id, st)
        return st.bill, st

    @staticmethod
    def _freeze(build_id: str, st: BuildState) -> dict:
        paths = sorted(set(st.downstream) | set(st.origin))
        packages = {
            p: {
                "downstream_bytes": st.downstream.get(p, 0),
                "origin_bytes": st.origin.get(p, 0),
            }
            for p in paths
        }
        return {
            "build_id": build_id,
            "status": "final",
            "packages": packages,
            "totals": {
                "downstream_bytes": sum(st.downstream.values()),
                "origin_bytes": sum(st.origin.values()),
            },
        }


billing = Billing()


class Entry:
    """One generation of a cached object.

    state: filling -> complete          (registered, file renamed into place)
                 \-> closed             (purged mid-fill; fill finished, file
                                          kept only until readers drain)
                 \-> failed             (origin error; part file removed)
    """

    def __init__(self, key: str) -> None:
        self.key = key
        self.gen = uuid.uuid4().hex[:12]
        self.state = "filling"
        self.purged = False
        self.readers = 0
        self.written = 0          # bytes flushed to the part file so far
        self.size = -1            # expected total bytes (-1 = not known yet)
        self.error: str | None = None
        self.origin_status: int | None = None
        self.headers_known = False
        self.meta: dict = {}
        self.cond = asyncio.Condition()
        self.fill_task: asyncio.Task | None = None

    @property
    def final_path(self) -> Path:
        return CACHE_DIR / self.key

    @property
    def part_path(self) -> Path:
        return CACHE_DIR / f"{self.key}.part-{self.gen}"

    @property
    def meta_path(self) -> Path:
        return CACHE_DIR / f"{self.key}.meta"

    def info(self) -> dict:
        return {
            "state": self.state,
            "purged": self.purged,
            "readers": self.readers,
            "written": self.written,
            "size": self.size,
            "gen": self.gen,
            "error": self.error,
        }


class Cache:
    def __init__(self) -> None:
        self.entries: dict[str, Entry] = {}
        self.lock = asyncio.Lock()
        self.session: aiohttp.ClientSession | None = None

    # ------------------------------------------------------------------ #
    # registry                                                            #
    # ------------------------------------------------------------------ #

    def _entry_from_disk(self, key: str) -> Entry | None:
        """Adopt a complete on-disk package (e.g. after a restart)."""
        final = CACHE_DIR / key
        try:
            st = final.stat()
        except OSError:
            return None
        if not final.is_file():
            return None
        e = Entry(key)
        e.state = "complete"
        e.size = st.st_size
        e.written = st.st_size
        e.headers_known = True
        try:
            e.meta = json.loads(e.meta_path.read_text())
        except (OSError, ValueError):
            e.meta = {}
        e.meta.setdefault("content_type", "application/octet-stream")
        self.entries[key] = e
        return e

    async def attach(self, key: str, owner_build: str | None = None) -> Entry:
        """Return the current generation for key, starting a fill if needed.

        A newly started fill is initiated by owner_build: every origin body
        byte it reads is billed to that build (None = untagged, not billed).
        """
        async with self.lock:
            e = self.entries.get(key)
            if e is None:
                e = self._entry_from_disk(key)
            if e is None:
                e = Entry(key)
                self.entries[key] = e
                billing.begin_fill(owner_build)
                e.fill_task = asyncio.create_task(self._fill_tracked(e, owner_build))
            e.readers += 1
            return e

    async def detach(self, e: Entry) -> None:
        async with self.lock:
            e.readers -= 1
            self._maybe_reap_locked(e)

    async def purge(self, key: str) -> bool:
        """Unregister the current generation; files go away with last reader."""
        async with self.lock:
            e = self.entries.pop(key, None)
            if e is not None:
                e.purged = True
                self._maybe_reap_locked(e)
                return True
            # Not in memory (e.g. never requested since restart): clean disk.
            existed = False
            final = CACHE_DIR / key
            for p in (final, CACHE_DIR / f"{key}.meta"):
                if p.exists():
                    _unlink_quiet(p)
                    existed = True
            try:
                for p in final.parent.glob(final.name + ".part-*"):
                    _unlink_quiet(p)
                    existed = True
            except OSError:
                pass
            return existed

    def _maybe_reap_locked(self, e: Entry) -> None:
        """Delete files once no readers remain and the fill is over."""
        if e.readers > 0 or e.state == "filling":
            return
        if e.state == "failed":
            _unlink_quiet(e.part_path)  # never keep a half package
        elif e.purged:  # complete or closed
            _unlink_quiet(e.part_path)
            _unlink_quiet(e.final_path)
            _unlink_quiet(e.meta_path)

    # ------------------------------------------------------------------ #
    # origin fill                                                         #
    # ------------------------------------------------------------------ #

    async def _fill_tracked(self, e: Entry, owner_build: str | None) -> None:
        """Run the fill; the initiating build's fill slot is released on any
        outcome (complete, closed, failed), so settlement can finalize."""
        try:
            await self._fill(e, owner_build)
        finally:
            billing.end_fill(owner_build)

    async def _fill(self, e: Entry, owner_build: str | None = None) -> None:
        assert self.session is not None
        url = f"{ORIGIN_BASE_URL}/packages/{e.key}"
        try:
            async with self.session.get(url) as resp:
                e.origin_status = resp.status
                if resp.status != 200:
                    e.error = f"origin responded with status {resp.status}"
                    await self._finish(e, "failed")
                    return
                cl = resp.content_length
                e.size = cl if cl is not None else -1
                e.meta = {
                    "content_type": resp.headers.get("Content-Type")
                    or "application/octet-stream",
                    "sha256": resp.headers.get("X-Package-Sha256"),
                    "fetched_at": time.time(),
                }
                e.part_path.parent.mkdir(parents=True, exist_ok=True)
                f = open(e.part_path, "wb")
                try:
                    # Part file exists before readers are allowed to open it.
                    e.headers_known = True
                    async with e.cond:
                        e.cond.notify_all()
                    written = 0
                    async for chunk in resp.content.iter_chunked(CHUNK):
                        await asyncio.to_thread(f.write, chunk)
                        await asyncio.to_thread(f.flush)
                        written += len(chunk)
                        e.written = written
                        # Bill origin body bytes to the initiating build as
                        # they are read; bytes read before any later failure
                        # or disconnect still count.
                        billing.add_origin(owner_build, e.key, len(chunk))
                        async with e.cond:
                            e.cond.notify_all()
                finally:
                    await asyncio.to_thread(f.close)
                if e.size != -1 and written != e.size:
                    raise aiohttp.ClientPayloadError(
                        f"short read from origin: {written}/{e.size} bytes"
                    )
            async with self.lock:
                e.size = written if e.size == -1 else e.size
                e.meta["size"] = written
                if e.purged:
                    # Purged mid-fill: keep the part file for current readers
                    # only; never register this generation.
                    e.state = "closed"
                else:
                    os.replace(e.part_path, e.final_path)  # atomic publish
                    try:
                        e.meta_path.write_text(json.dumps(e.meta))
                    except OSError:
                        pass
                    e.state = "complete"
                async with e.cond:
                    e.cond.notify_all()
                self._maybe_reap_locked(e)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - any origin/IO error fails the fill
            e.error = f"{type(exc).__name__}: {exc}"
            await self._finish(e, "failed")

    async def _finish(self, e: Entry, state: str) -> None:
        async with self.lock:
            e.state = state
            if state == "failed" and self.entries.get(e.key) is e:
                del self.entries[e.key]  # next GET retries the origin
            async with e.cond:
                e.cond.notify_all()
            self._maybe_reap_locked(e)

    # ------------------------------------------------------------------ #
    # client streaming                                                    #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _open_current(e: Entry):
        try:
            return open(e.part_path, "rb")
        except FileNotFoundError:
            return open(e.final_path, "rb")

    async def stream(self, e: Entry):
        """Yield the package from byte 0, tailing the fill file if needed."""
        offset = 0
        f = await asyncio.to_thread(self._open_current, e)
        try:
            while True:
                async with e.cond:
                    await e.cond.wait_for(
                        lambda: e.written > offset or e.state != "filling"
                    )
                if e.state == "failed":
                    raise FillFailed(e.error or "origin fill failed")
                if e.state == "filling":
                    limit = CHUNK
                else:  # complete | closed: drain exactly to the final size
                    limit = min(CHUNK, e.size - offset)
                    if limit <= 0:
                        break
                data = await asyncio.to_thread(f.read, limit)
                if not data:
                    if e.state == "filling":
                        continue  # defensive; written>offset implies data
                    break
                offset += len(data)
                yield data
        finally:
            try:
                f.close()
            finally:
                await self.detach(e)


cache = Cache()


def cleanup_cache_dir() -> None:
    """On boot: drop partial fills and orphan meta, keep complete packages."""
    for p in CACHE_DIR.rglob("*"):
        if not p.is_file():
            continue
        name = p.name
        if ".part-" in name:
            _unlink_quiet(p)
        elif name.endswith(".meta"):
            final = p.with_name(name[: -len(".meta")])
            if not final.exists():
                _unlink_quiet(p)


@asynccontextmanager
async def lifespan(app: FastAPI):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cleanup_cache_dir()
    timeout = aiohttp.ClientTimeout(total=None, connect=10, sock_connect=10, sock_read=300)
    cache.session = aiohttp.ClientSession(timeout=timeout)
    try:
        yield
    finally:
        await cache.session.close()


app = FastAPI(title="pkg-cache-proxy", version="1.0.0", lifespan=lifespan)


@app.get("/healthz")
async def healthz():
    return {"ok": True}


@app.get("/packages/{path:path}")
async def get_package(path: str, x_build_id: str | None = Header(default=None)):
    """Download a complete package by path (streamed from byte 0).

    Tag the request with X-Build-ID to bill its traffic to that build:
    downstream body bytes are charged to the build, and if this request
    initiates the origin fill, the fill's origin bytes are charged too.
    Untagged requests download normally and are not billed.
    """
    try:
        key = validate_key(path)
    except ValueError as exc:
        return PlainTextResponse(str(exc), status_code=400)

    build_id = (x_build_id or "").strip() or None
    if build_id is not None and len(build_id) > MAX_BUILD_ID_LEN:
        return PlainTextResponse("X-Build-ID too long", status_code=400)

    tagged = build_id is not None
    if tagged and not billing.try_begin_download(build_id):
        return PlainTextResponse(
            f"build {build_id!r} is sealed for settlement; "
            "new downloads with this build id are rejected",
            status_code=409,
        )

    try:
        entry = await cache.attach(key, owner_build=build_id)
        hit = entry.state == "complete"
        try:
            if not entry.headers_known:
                async with entry.cond:
                    await entry.cond.wait_for(
                        lambda: entry.headers_known or entry.state == "failed"
                    )
        except asyncio.CancelledError:
            await cache.detach(entry)
            raise

        if entry.state == "failed":
            await cache.detach(entry)
            if tagged:
                billing.end_download(build_id)
            status = (
                entry.origin_status
                if entry.origin_status and entry.origin_status != 200
                else 502
            )
            return PlainTextResponse(
                f"origin fetch failed: {entry.error}", status_code=status
            )
    except BaseException:
        if tagged:
            billing.end_download(build_id)
        raise

    async def body():
        try:
            async for chunk in cache.stream(entry):
                if tagged:
                    # Handed to the HTTP server: counts even if the client
                    # disconnects right after this chunk.
                    billing.add_downstream(build_id, key, len(chunk))
                yield chunk
        finally:
            if tagged:
                billing.end_download(build_id)

    headers = {"X-Cache": "HIT" if hit else "MISS"}
    if entry.size >= 0:
        headers["Content-Length"] = str(entry.size)
    sha256 = entry.meta.get("sha256")
    if sha256:
        headers["X-Package-Sha256"] = sha256
    return StreamingResponse(
        body(),
        media_type=entry.meta.get("content_type", "application/octet-stream"),
        headers=headers,
    )


@app.post("/purge/{path:path}")
async def purge_package(path: str):
    """Purge one path. The next download re-fills from the origin; downloads
    already in progress keep reading their old generation to completion."""
    try:
        key = validate_key(path)
    except ValueError as exc:
        return PlainTextResponse(str(exc), status_code=400)
    was_cached = await cache.purge(key)
    return {"path": key, "purged": True, "was_cached": was_cached}


@app.get("/cache/status")
async def cache_status():
    async with cache.lock:
        return {k: e.info() for k, e in cache.entries.items()}


# --------------------------------------------------------------------- #
# billing API                                                           #
# --------------------------------------------------------------------- #


@app.post("/billing/settle/{build_id}")
async def settle_build(build_id: str):
    """Settle one build's traffic bill.

    The first call seals the build: new downloads tagged with this build id
    are rejected (409). While the build still has accepted downloads in
    flight or origin fills it initiated unfinished, the call returns
    "processing" (202). Once everything has finished, the bill is frozen and
    every later call returns the identical bill. Bills live in this process.
    """
    bill, st = billing.settle(build_id)
    if bill is None:
        return JSONResponse(
            status_code=202,
            content={
                "build_id": build_id,
                "status": "processing",
                "downloads_in_flight": st.downloads_in_flight,
                "fills_in_flight": st.fills_in_flight,
            },
        )
    return bill


@app.get("/billing/builds")
async def list_builds():
    """Live (non-mutating) view of all known builds and their counters."""
    return {bid: st.snapshot() for bid, st in billing.builds.items()}


@app.get("/billing/builds/{build_id}")
async def get_build(build_id: str):
    """Live counters for one build; includes the frozen bill if finalized."""
    st = billing.builds.get(build_id)
    if st is None:
        return PlainTextResponse("unknown build id", status_code=404)
    snap = st.snapshot()
    if st.bill is not None:
        snap["bill"] = st.bill
    return snap
