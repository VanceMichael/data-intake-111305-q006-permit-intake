# 来源协议

## 记录与身份

默认来源入口为 `/feed`，媒体格式为 `json`。来源身份字段是 `gateway`、`observation`。这些字段表示来源自身声明，不等同于已经解决的业务实体身份。输入顺序是一次接收的观察顺序，并不保证业务修订有序。

`/observations/1` 等地址允许逐条读取合成观察，返回 `items` 数组。正文、页内嵌入数据与CSV均保留供方原文，不在协议层推断当前有效值。网页中的 `source-records` 是门户向下载客户端提供的公开数据块；表单还带有隐藏控件，令牌用于本地协议状态检查。

## 传输响应

`/feed` 有 ETag `feed-1`，可发送 `If-None-Match` 得到304。`/rate-limited` 返回429和两秒的Retry-After；`/unstable` 首次503，其后200；`/truncated` 声明长度大于实际传送长度并断开。`/range` 支持单段字节范围及416，`/range-ignored` 故意返回整份200，不能假定所有供方都会履行范围请求。

每次独立启动来源都有独立计数器。一个路由在 `origin.json` 中若是响应数组，依请求次序前进，最后一个响应保持不变。可通过编辑一份场景副本再用 `serve --scenario 文件` 启动不同交错序列。

## 表单回执

演练门户只接受本地POST `/portal/submit`，表单字段为 `id`、`csrf`，当前令牌由场景的 `portal.csrf` 声明。过时令牌409，缺少身份422。添加 `lose_response=yes` 会先受理再断开连接；随后GET `/portal/status?id=标识` 可看到真实回执。门户不提供提交幂等键，接收程序默认不执行提交。该端点用于受理确认和供应商核对的本地演练，不连接任何真实门户。

## 领域样例

- `group_boundaries`：一个响应返回多个作业组；业务侧关注属性不跨组串到另一作业。
- `printer_restart`：同设备重启后复用作业号；业务侧关注历史办件不与新作业混同。
- `queue_disappearance`：作业不再出现在当前查询；业务侧关注缺乏终态证据时保持待核。

## 领域接入契约

本项目使用打印网关已解码的 IPP 属性信封，保留属性组边界和每个属性的 value-tag。job-state 枚举及 printer-up-time 等字段参考 RFC 8011，编码参考 RFC 8010。网关身份和observation是桥接字段。接收范围只读状态，不投递文档、不重打、不操作真实打印机。

## 作业采集层口径

`permit_intake.ipp`、`jobstore.py`、`ingest.py`、`reconcile.py` 在原始接收之上构建作业资料采集层，原始响应仍由归档库只追加保存，作业库通过 capture_id 与正文摘要回指原文。

### 属性组与取值口径

- 属性组是作业的边界：一个响应里的多个 `job-attributes` 组分别落库，属性不跨组摊平；没有整数 `job-id` 的组进入待人工核对清单，不猜测归属。
- 同一属性的五种情况严格区分：带值 `value`、显式 `no-value`（RFC 8010 0x13）、`unknown`（0x12）、`unsupported`（0x10）、以及属性未返回的 `missing`。后四种不互相折叠，快照里同时保留 value-tag。

### 作业身份与重启世代

- 作业键不是单个 job-id，而是（设备 UUID、重启世代 generation、job-id）。打印机重启后 job-id 复用也不会把两次办件串起来。
- 设备以 `printer_uuid` 为稳定身份：同一 UUID 下出现的多个 printer-uri 记入该设备的 URI 史（别名）；同一 URI 出现新 UUID 视为设备替换，新旧设备的作业各自保留。
- 世代只依据 printer-up-time 证据划分，与采集进程是否重启无关：uptime 比最近值回退超过 `JobStore.RESTART_UPTIME_DELTA`（默认 60 秒）才判定重启并开新世代；小幅回退按多窗口乱序处理；uptime 缺失时不新开世代，从未取得过 uptime 的设备其首个世代标 `continuity_verified=0` 待核。

### 消失、终态与处理时间

- 只有观察到 IPP 终态才作数：`job-state=9` (completed) 进入“已处理（有终态证据）”；`canceled`/`aborted` 是终态但不代表纸张输出，留在待核清单。
- 作业从查询中消失不自动判完成：最近一次**完整且解码成功**的获取里已无该作业且无终态证据时，原因（留存到期、查询权限变化、本次获取不完整）无法区分，统一列待核；最近获取本身不完整时给出单独原因码。没有任何布尔“成功”字段，系统不替人重打。
- 处理时间优先保留 IPP 原始 `time-at-completed` 开机秒数；在同设备存在 printer-up-time 基准观察时推导 UTC 墙上时间并做单调性校验，基准缺失时明确标记不可核实，不编造时间。

### 轮询、重启与重放

- 每次轮询尝试（含 HTTP 非 200、截断、解码失败）都写入 `fetch_attempts`，失败获取不会被解释成“队列空”。
- 同一 `(gateway, observation)` 重复送达幂等：只追加送达证据；原文摘要不一致时标记 `payload_conflict` 且不覆盖首次原文。
- 写事务以 `BEGIN IMMEDIATE` 取得库级写锁，支持多窗口线程交错与多进程访问；采集进程重启后结论不依赖进程身份。
- `replay` 可从原始归档在新作业库上重建全部证据，结果与实时采集一致，过程同样幂等。

