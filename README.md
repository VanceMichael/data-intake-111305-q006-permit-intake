# 受理窗口 IPP 打印作业的状态采集与对账

这是资料接收程序的基础版。它请求来源接口，保留响应状态、头部、原始内容与每次观察的时间，能够从 JSON、CSV 或门户嵌入的数据块读回来源记录。当前不生成面向业务的有效版本，也不自动执行门户提交；重复观察会分别保留，原始内容按摘要复用。

## 运行环境

使用 Python 3.11 及以上，业务运行依赖均为标准库；SQLite 由 Python 自带。时区资料使用运行环境内的系统数据库。运行过程不安装依赖，也不下载数据。本地来源服务器和接收程序在同一进程环境中即可运行。

构建工具及其传递依赖精确锁定在 `requirements-build.txt`。首次准备开发环境时先执行下面命令；之后测试、构建和演示均可离线运行，不在运行命令中安装包。

```sh
python3 -m pip install -r requirements-build.txt
```

```sh
python3 -m unittest discover -s tests -v
python3 -m build --wheel --no-isolation
python3 -m compileall -q permit_intake tests
python3 -m permit_intake demo
python3 -m permit_intake demo-jobs
```

演示从空的临时目录启动本地来源，真实完成一次 HTTP 获取及归档，并输出原样记录；结束自动清理。本地测试、编译和演示合计一般不超过十秒，可直接展示，不依赖历史日志。`demo-jobs` 在同一临时库里走完整轮询序列，输出作业对账分区（已处理/待核/无法关联的属性组）。

## 常驻来源与接收

```sh
python3 -m permit_intake serve
```

终端会输出本次分配的回环地址。在另一终端将下面地址替换为实际地址：

```sh
python3 -m permit_intake capture --base-url http://127.0.0.1:实际端口 --archive .runtime/raw.sqlite3 --source local-source --path /feed
```

归档库只追加来源观察，不用最后一条响应覆盖先前响应。`archive.py` 管理原始内容及完整性，`client.py` 执行一次请求，`decoding.py` 负责来源格式解码，`origin.py` 提供协议演练。业务解释可与原始接收独立演进。

## 作业采集与对账

原始归档之外，作业证据库（单独的 SQLite）保存可关联、可重放的作业资料：

```sh
python3 -m permit_intake poll --base-url http://127.0.0.1:实际端口 \
  --archive .runtime/raw.sqlite3 --store .runtime/jobs.sqlite3 \
  --gateway window-a --path /feed
python3 -m permit_intake report --store .runtime/jobs.sqlite3
python3 -m permit_intake replay --archive .runtime/raw.sqlite3 --store .runtime/jobs-rebuilt.sqlite3
```

- `ipp.py` 解释 value-tag 语义，区分带值、`no-value`、`unknown`、`unsupported` 与属性未返回（`missing`）。
- `jobstore.py` 按属性组落库，作业键为（设备 UUID、重启世代、job-id）；同 UUID 多 URI 记为别名，同 URI 换 UUID 记为设备替换；世代只由 printer-up-time 证据划分。原始组 JSON、逐属性行、每次送达与每轮抓取健康状况均留痕。
- `ingest.py` 把一次轮询双写到归档库与作业库，失败获取只留痕、不产生“队列空”的解释；`replay_archive` 可从归档幂等重建作业库。
- `reconcile.py` 只读生成对账报告：completed 终态且具备时间证据的进入已处理；取消/中止、消失但无终态、获取不完整等全部进入待核并附原因码与证据引用，不输出布尔成功值、不触发重打。

口径细节见 `docs/source-protocol.md` 的“作业采集层口径”。

## 开发资料

来源协议见 `docs/source-protocol.md`，领域观察关系见 `tests/test_source_contract.py`，作业采集口径见 `tests/test_job_collection.py`。`fixtures/business_cases.json` 保存业务侧关注的交错情况及可观察口径；`fixtures/job_cases.json` 是覆盖这些情况的可轮询场景（属性组边界、uptime 回退重启、同 URI 换 UUID、四种取值口径、无法关联的属性组）。所有条目、机构、文件内容和门户回执均为本地合成资料，不包含真实用户身份。

## Wheel 分发

构建产物位于 `dist/`，包含 `permit_intake` 和命名空间内的合成来源 JSON。安装 wheel 后，即使当前目录不是源码目录，也可执行 `python3 -m permit_intake demo`。提供 `serve --scenario` 时仍以指定文件为准，找不到指定文件会直接报错，不回退到默认资料。
