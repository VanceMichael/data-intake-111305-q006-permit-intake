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

## 采集层口径

`ledger.py` 在归档之上重放信封：设备按 printer_uuid 识别（多 URI 为别名，uuid 变化为替换），printer_up_time 回退开启新纪元，job-id 只在纪元内唯一。属性取值区分 value / out_of_band（no-value、unknown 等）/ absent 三种在场状态。作业消失只记录候选原因（retention_expired_or_history_scope、query_scope_or_permission、listing_scope_undeclared、fetch_incomplete），不产生完成结论。回执的评估分 processed / in_progress / unverified 三档，处理时间由 time-at-* 与抓取到达时间推算并保留推算依据；所有结论附证据 capture 摘要，可按 digest 从归档库取回原始内容核对。
