# 受理窗口 IPP 打印作业的状态采集与对账

这是资料接收程序的基础版。它请求来源接口，保留响应状态、头部、原始内容与每次观察的时间，能够从 JSON、CSV 或门户嵌入的数据块读回来源记录。当前不生成面向业务的有效版本，也不自动执行门户提交；重复观察会分别保留，原始内容按摘要复用。

在原始接收之上，`ledger.py` 提供作业资料采集层：把归档的 IPP 属性信封整理成可核实的作业台账，为受理窗口出具办件回执。该层只读解释已归档内容，不抓取、不投递文档、不重打、不操作门户。

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
```

演示从空的临时目录启动本地来源，真实完成一次 HTTP 获取及归档，并输出原样记录；结束自动清理。本地测试、编译和演示合计一般不超过十秒，可直接展示，不依赖历史日志。

## 常驻来源与接收

```sh
python3 -m permit_intake serve
```

终端会输出本次分配的回环地址。在另一终端将下面地址替换为实际地址：

```sh
python3 -m permit_intake capture --base-url http://127.0.0.1:实际端口 --archive .runtime/raw.sqlite3 --source local-source --path /feed
```

归档库只追加来源观察，不用最后一条响应覆盖先前响应。`archive.py` 管理原始内容及完整性，`client.py` 执行一次请求，`decoding.py` 负责来源格式解码，`origin.py` 提供协议演练。业务解释可与原始接收独立演进。

## 作业台账与办件回执

在归档库旁建一个台账库，把已归档的抓取摄入为作业资料；摄入幂等，可重复执行，进程重启后从未处理的 capture 继续：

```sh
python3 -m permit_intake ingest --archive .runtime/raw.sqlite3 --ledger .runtime/jobs.sqlite3
python3 -m permit_intake report --archive .runtime/raw.sqlite3 --ledger .runtime/jobs.sqlite3
```

`report` 可加 `--device 设备身份` 或 `--job-id 号码` 过滤。每张回执给出评估（`processed` / `in_progress` / `unverified`）、逐次观察的状态与原因、可核实的处理时间（含推算依据）、候选原因和证据 capture 摘要；待核办件在 `unverified_receipts` 单独列出，无法归属到作业的观察在 `unattributed_observations` 列出。采集口径：

- 按属性组理解作业：每个 `job-attributes` 组是一条作业观察，一个响应里多个 job-id 的属性不摊平到同一行；`no-value`、`unknown` 与属性未返回分别保留；
- 设备以 `printer_uuid` 识别，同一 uuid 的多个 URI 是别名；uuid 变化视为设备替换，即使 URI 相同；`printer_up_time` 回退视为重启并开启新纪元，作业号只在纪元内唯一，重启前后的同号作业不合并；
- 作业从查询结果消失只记录候选原因（留存到期、查询范围或权限变化、本次获取不完整），不自动认定完成；缺终态证据的办件保持待核；
- 多个窗口的轮询在归档层按到达顺序交错，台账按 capture 顺序重放；同一记录的重复投递只记为重复证据，不会伪造重启纪元；每个 capture 的摄入是一个事务，中途崩溃可安全重跑。

## 开发资料

来源协议见 `docs/source-protocol.md`，领域观察关系见 `tests/test_source_contract.py`。`fixtures/business_cases.json` 保存业务侧关注的交错情况及可观察口径；基础版测试验证的是原始采集和协议，不代表业务处理已经具备这些能力。所有条目、机构、文件内容和门户回执均为本地合成资料，不包含真实用户身份。

## Wheel 分发

构建产物位于 `dist/`，包含 `permit_intake` 和命名空间内的合成来源 JSON。安装 wheel 后，即使当前目录不是源码目录，也可执行 `python3 -m permit_intake demo`。提供 `serve --scenario` 时仍以指定文件为准，找不到指定文件会直接报错，不回退到默认资料。
