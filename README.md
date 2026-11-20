# pkg-cache：单进程 HTTP 包缓存代理 + 可控本地源站

面向构建工程的依赖包缓存代理：固定本地源站、按路径 GET 完整包，减少重复
回源流量；源包更新后按路径清除缓存。代理由 **FastAPI + aiohttp** 实现，
单进程单 worker 运行。支持按构建号（`X-Build-ID`）结算依赖包流量。

## 快速开始

```bash
docker compose up --build
```

* 代理：http://localhost:8080 （下载 + 结算 API，均可在 `/docs` 里操作）
* 源站：http://localhost:8081 （控制 API + `/docs`，自带 32MiB 包 `pkg-32m.bin` 与 1MiB 包 `pkg-1m.bin`）

## 使用

```bash
# 下载（首次 MISS 回源，之后 HIT 命中磁盘缓存）
curl -OJ http://localhost:8080/packages/pkg-32m.bin

# 按路径清除（也可打开 http://localhost:8080/docs 在 Swagger UI 里执行）
curl -X POST http://localhost:8080/purge/pkg-32m.bin

# 查看代理内部状态
curl http://localhost:8080/cache/status
```

## 按构建结算

CI 下载时带上 `X-Build-ID` 头，流量即计入该构建；未标记的请求照常下载、
流量不入账。两类字节均按包路径累计，传输中断前已发生的字节照计：

* **下行**：代理交给 HTTP 服务端的包正文字节（客户端断开前已交付的计入）。
* **回源**：代理从源站实际读到的包正文字节。每次共享回源只归发起它的
  构建；后来加入同一回源的请求只计自己的下行。

```bash
# 构建 demo-1 的下载（MISS 计下行+回源，HIT 只计下行）
curl -OJ -H 'X-Build-ID: demo-1' http://localhost:8080/packages/pkg-1m.bin

# 查看各构建实时计数（只读，不封存）
curl http://localhost:8080/billing/builds

# 结算 demo-1
curl -X POST http://localhost:8080/billing/settle/demo-1
```

结算语义：

* 首次调用即**封存**该构建：之后带该构建号的新下载返回 `409`（未标记的
  下载不受影响）。
* 该构建还有已受理的下载、或由它发起的回源未结束时，返回 `202` +
  `{"status": "processing", ...}`，稍后再调。
* 全部结束后返回 `200` + 最终账单（按包路径的下行/回源字节及合计）；
  之后每次调用都返回同一份账单。账单只保存在代理进程内，重启即清空。

在 http://localhost:8080/docs 可直接演示：`GET /packages/{path}` 展开后
在 `x-build-id` 头参数里填构建号下载，再调 `POST /billing/settle/{build_id}`
结算。

源站控制（换内容 / 延迟 / 断流 / 回源次数）：

```bash
# 查看回源次数与当前包 sha256
curl http://localhost:8081/control/stats

# 更换包内容（重新生成随机字节，sha256 改变；代理在清除前仍发旧包）
curl -X POST http://localhost:8081/control/content/pkg-32m.bin

# 每个 256KiB 块延迟 100ms（模拟慢源站）
curl -X POST http://localhost:8081/control/behavior \
  -H 'Content-Type: application/json' \
  -d '{"chunk_delay_ms":100,"truncate_after_bytes":null,"chunk_size":262144}'

# 发送 200000 字节后断流（模拟上游故障）
curl -X POST http://localhost:8081/control/behavior \
  -H 'Content-Type: application/json' \
  -d '{"chunk_delay_ms":0,"truncate_after_bytes":200000,"chunk_size":65536}'

# 清零回源计数
curl -X POST http://localhost:8081/control/reset
```

## 行为要点

* **单飞回源**：同路径并发未命中只回源一次；所有下载（包括中途加入的）
  都从字节 0 开始逐块收到原字节，首块在源站首块落盘后即可读出，不等整包。
* **慢客户端隔离**：填充只写磁盘，各读者按自己的节奏尾随 `.part-<代>` 文件；
  单客户端内存为一个读缓冲，不随包长增长（实测 32MiB 下载代理 RSS 增量约 4MiB）。
* **按路径清除**：清除返回后首个新下载重新回源，后续共用新填充/缓存；
  已开始的旧下载继续读完旧代文件；旧填充完成也不会重新登记；
  旧文件在最后一个读者结束后回收。
* **失败传播**：上游断流/非 200 使该代所有下载失败（客户端读到不完整
  响应），半包立即删除、绝不入缓存；下一个请求自动重试回源。
* **重启**：`proxy-cache` 卷保留完整包；启动时清理 `.part-*` 残件与孤儿
  meta。404 不做负缓存。
* **按构建计费**：见上节。回源归属在填充发起时确定，发起下载中途断开
  不影响归属——回源继续读到的字节仍归该构建；结算会等它结束。

## 交付说明

### 已实现

* `X-Build-ID` 标记下载；未标记请求照常服务、不计入任何构建。
* 下行/回源两项字节按（构建， 包路径）累计；中断前已交付/已读取的字节照计。
* 共享回源只归发起构建；后加入者只计自己的下行。
* `POST /billing/settle/{build_id}`：首次调用封存构建（新标记下载 409）；
  有已受理下载或已发起回源未结束时返回 202 处理中；全部结束后冻结账单，
  之后每次调用返回同一结果。账单保存在进程内。
* 只读观察口：`GET /billing/builds`、`GET /billing/builds/{build_id}`。
* `/docs` 可演示下载（header 参数填构建号）与结算。

### 已验证（`python3 -m pytest tests/ -v`，16 项全过）

* 未标记下载不产生任何计费记录；MISS 计下行+回源、HIT 只计下行。
* 结算后重复调用返回同一账单；封存后标记下载 409、未标记下载正常。
* 下载/回源进行中结算返回 202，结束后转 200。
* **共享回源 + 发起下载中途断开**：发起构建断开约 3 个块后离开，回源
  继续至完成；其账单回源字节为整包（1MiB）、下行仅断开前部分；跟随构建
  下行整包、回源为 0；源站只被回源一次。
* 源站断流：失败前已读 200000 字节回源、已交付的下行均计入账单。
* 既有缓存行为（单飞、清除、重启、内存有界等）全部回归通过。
* 另以真实进程手工冒烟验证 `/docs`、OpenAPI 及上述 curl 流程。

### 未完成 / 说明

* 账单按需求只存进程内，代理重启即丢失（缓存包仍在磁盘）。
* 本沙箱无 docker，未实际执行 `docker compose up`；compose 配置未改动，
  两个服务以相同命令手工起进程验证过。
* 构建号在 URL 路径中传递，含 `/` 的构建号需自行转义或避免使用。

## 本地自测（无 Docker 时）

```bash
pip install -r proxy/requirements.txt -r origin/requirements.txt
pip install httpx pytest
python3 -m pytest tests/ -v
```

测试以真实子进程启动源站与代理，覆盖：单飞、首块延迟、慢客户端隔离、
清除语义（含填充中清除）、断流不缓存半包、重启保留/清理、内存有界、
404 与路径穿越。`tests/run_server.py` 是测试专用启动器（本沙箱禁止
进程间信号，用控制文件代替 SIGTERM 停服），交付应用本身不含测试钩子。
