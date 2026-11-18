# 报表下载—采集—入库 关联协议

## 1. 问题与不变量

报表门户允许**相同文件名**的多次下载；浏览器通知（下载事件）与后台生成通知（SSE
ready 事件）可能**乱序、重复或丢失**。因此系统必须满足：

1. **绝不**以文件名、下载完成顺序、或"最近一次点击"来配对任务与文件。
2. 每个文件入库前必须同时核对三要素，缺一即隔离（quarantine）：
   - **筛选条件**（filter）：下载时声明的条件与门户记录的任务条件一致；
   - **生成任务**（task）：文件归属的任务存在、未取消、未被新任务取代；
   - **内容**（content）：文件体的 SHA-256 与清单声明及门户生成时的哈希一致。
3. 任务取消后到达的迟到文件**不得**覆盖任何已有结果，只能进入隔离区并标注原因。
4. 文件名冲突时，库内路径以 `task_id / content_sha` 寻址，同名文件互不覆盖。
5. 所有状态持久化到 SQLite（WAL），进程崩溃/浏览器退出后靠对账（reconcile）恢复。
6. 通知与文件传输是两条独立通道，任何一方先到都能正确收敛（幂等 + 对账扫描）。

## 2. 身份与令牌

- 门户为每次生成请求创建 `task_id`（UUIDv4），并签发不透明 `token`：
  `token = HMAC_SHA256(secret, f"{task_id}|{filter_canonical}|issued_at")` 的 hex。
- 下载 URL 携带 `?task=<task_id>&token=<token>`，响应头同时回送
  `X-Task-Id` / `X-Task-Token`，浏览器侧清单从这三处取，互相比对。
- token 不携带任何状态；任务状态（含取消、取代链）只以门户 DB 为准。
- 浏览器为每次实际下载生成 `download_id`（UUIDv4），作为采集器侧的去重键。
- 文件清单（manifest）与文件体一起传输；清单是 JSON，字段：

```json
{
  "manifest_version": 1,
  "download_id": "uuid",
  "task_id": "uuid",
  "token": "hmac hex",
  "portal_url": "http://127.0.0.1:8080",
  "filter": {"report": "sales", "region": "north", "period": "2026-11"},
  "declared_filename": "report.csv",
  "size_bytes": 1234,
  "sha256": "…",
  "download_started_at": "…Z",
  "download_finished_at": "…Z"
}
```

## 3. 通道与时序

### 3.1 两条通道

- **事件通道**：浏览器把 `downloads.onCreated / onChanged(complete/interrupted)`
  类事件 POST 到采集器 `/api/events`；门户 SSE `/api/events/stream` 推送
  `task.created / task.ready / task.cancelled`。事件只登记，不配对、不触发入库。
- **文件通道**：下载完成后浏览器把 `manifest + 文件体` multipart POST 到
  采集器 `/api/envelopes`。采集器先落临时目录（原子 rename），再入待入库队列。

### 3.2 通知先于文件写完

- multipart 上传先写 `spool/<download_id>.part`，全部接收并 fsync 后
  `rename` 到 `incoming/<download_id>.bin`，然后才写 envelope 记录。
- 事件先到而 envelope 未到时，envelope 处于 `awaiting_file`；采集器
  对账线程扫描 spool 中的残留 `.part`（浏览器退出/连接中断）并标记 `orphan_partial`。
- 入库 worker 只处理 `ready` 的 envelope；任何大小为 0 / 仍在增长的文件不会被取走。

### 3.3 乱序与重复

- 事件、envelope 都以 `download_id` 为唯一键，重复提交返回同一记录（幂等）。
- worker 对同一 `download_id` 的处理是事务性的；SSE 重复 ready 事件只刷新状态。
- 门户取消任务后立即新建任务时，门户记录 `superseded_by` 链；旧任务的文件即使
  内容哈希正确，也在第 3.4 步第 2 关被拒（`task_cancelled` / `task_superseded`）。

## 4. 入库 worker 的三要素核对（按顺序短路）

1. **任务关**：`GET /api/tasks/{id}/verify?token=…` 由门户校验 token 合法性，
   返回任务状态（`active|cancelled|superseded|unknown`）与取代链。非 active 即隔离。
2. **筛选关**：门户返回权威 `filter`，与 manifest.filter 做规范化 JSON 比较，
   不一致隔离（`filter_mismatch`）。
3. **内容关**：worker 现场重算 SHA-256，与 manifest.sha256、门户
   `content_sha256` 三方一致才通过；不一致隔离（`content_hash_mismatch`）。

通过后：

- 落库路径 `library/<task_id>/<sha256[:16]>-<declared_filename>`，
  已存在则跳过写入（内容寻址，天然防覆盖）；
- worker ledger 在同一 SQLite 事务里写 `ingested` 记录并对采集器 ACK；
- 采集器收到 ACK 才把 envelope 置为 `acked`。

## 5. 故障与恢复

| 故障 | 表现 | 恢复机制 |
|---|---|---|
| 通知先到，文件未写完 | spool .part、事件无 envelope | 对账线程扫描；envelope 到达即流转 |
| 浏览器退出（上传前） | 下载目录有完整文件，采集器无记录 | 文件尾部含 manifest 尾块（见 §6），采集器 reconcile 接口/扫描重建 envelope |
| 浏览器退出（上传中断） | spool 残留 .part | 标记 orphan_partial，超时进死信，页面可查 |
| 入库成功但 ACK 丢失 | 队列租约过期，envelope 重新被取 | worker ledger 唯一约束去重，第二次直接 ACK（at-least-once + 幂等） |
| 门户在下载中途取消 | verify 返回 cancelled | 文件进隔离区，原因 task_cancelled，不写 library |
| 通知乱序/重复 | 同 id 多事件 | 唯一键 + 状态机单调迁移，重复事件 no-op |

## 6. 自描述文件尾块（浏览器退出恢复用）

真实下载文件（CSV/JSON）末尾追加一行注释：

```
# __MANIFEST__=<base64url(json manifest)>
```

- 报表格式为 CSV，首行表头、末行注释不影响数据解析；
- 采集器扫描孤立文件时读取尾块即可恢复 task_id / filter / sha256（注意：
  尾块里的 sha256 是**不含尾块的原始报表体**哈希，worker 核对前先剥离尾块）；
- 剥尾块后的字节哈希才参与内容关核对。

## 7. 状态机

- task（门户）：`created → ready → (cancelled | superseded)`，ready 后仍可取消。
- envelope（采集器）：`awaiting_file → ready → leased → acked`；
  旁路 `orphan_partial`、`dead_letter`。
- ingest（worker ledger）：`pending → ingested | quarantined | dead_letter`。

## 8. 虚构门户的可控点（故障注入）

门户与采集器均提供页面/API 开关，不改动代码即可复现：

- 延迟/重复/丢弃 SSE ready 通知；
- 下载响应慢速分块（制造"通知先于写完"）；
- 生成后替换报表内容（制造内容哈希不一致）；
- 采集器丢弃 ACK（模拟入库成功无回执）；
- 虚拟浏览器在下载前/上传前崩溃（模拟浏览器退出）。
