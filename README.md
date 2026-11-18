# 报表下载—采集—入库 关联系统

一个从空目录实现的、纯标准库（Python 3.11）+ Node.js 的演示项目，解决报表门户中
**同名文件重复下载、浏览器通知与后台生成通知乱序**条件下的可靠关联与入库问题。

## 核心问题与约束

门户一次能生成多个**同名**报表，浏览器下载通知和后台生成通知可能乱序、重复或丢失。
因此系统强制：

- **绝不**凭文件名、下载顺序或"最近一次点击"配对；
- 每份文件入库前同时核对**筛选条件、生成任务、内容哈希**三要素；
- 任务取消后到达的迟到文件只能进隔离区，**不能覆盖**新结果；
- 通知先于文件写完、浏览器退出、入库成功但 ACK 丢失，都能自动收敛。

完整设计见 [`docs/protocol.md`](docs/protocol.md)。

## 架构

```
┌──────────────┐   SSE task.created/ready/cancelled   ┌──────────────────────┐
│  虚构门户     │ ◀──────────────────────────────────▶ │  浏览器侧             │
│ portal:8080  │      下载 ?task=&token= (真实文件)     │  extension/ (MV3)     │
│  SQLite+SSE  │ ◀─────────────────────────────────── │  vbrowser/ (Node 无头) │
└──────┬───────┘                                       └──────────┬───────────┘
       │  verify(task,token,filter) 权威三要素                    │ 双通道
       ▼                                                          ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ 采集器 collector:8090                                                       │
│  事件通道 /api/events（仅登记）  文件通道 /api/envelopes（spool→原子 rename）│
│  SQLite 队列 + 租约 + 对账扫描                                              │
└───────────────────────────────────┬──────────────────────────────────────┘
                            lease / ack / nack
                                     ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ 入库 worker:8091                                                           │
│  三要素核对 → 内容寻址落库 library/<task>/<sha>-<name>，失败进 quarantine   │
│  独立 ledger（幂等），只读台账 API 供页面查询来源/状态/恢复去向              │
└──────────────────────────────────────────────────────────────────────────┘
```

## 目录

| 路径 | 作用 |
|---|---|
| `portal/` | 虚构报表门户：任务、HMAC token、真实 CSV 下载、SSE、故障注入 |
| `collector/` | 浏览器采集器 + 持久化入库队列、spool、对账、租约 |
| `worker/` | 入库 worker：三要素核对、内容寻址落库、隔离、幂等台账 |
| `extension/` | 真实 Chrome MV3 扩展（`core.js` 与无头浏览器共用） |
| `vbrowser/` | 无头虚拟浏览器（Node），真实落盘下载 + 崩溃/恢复控制 API |
| `common/py/httpkit.py` | 标准库 HTTP 路由、SSE、multipart、哈希工具 |
| `tests/` | 跨语言尾块往返测试 + 9 个端到端故障场景（28 条断言） |
| `docs/protocol.md` | 关联协议、状态机、故障恢复矩阵 |

## 快速开始

```bash
bash scripts/start-all.sh          # 启动 4 个进程
# 门户 UI:     http://127.0.0.1:8080/ui
# 采集器 UI:   http://127.0.0.1:8090/ui
# 台账 API:    http://127.0.0.1:8091/ledger
# 虚拟浏览器:  http://127.0.0.1:8095/status

python3 -I tests/e2e_scenarios.py  # 跑全部故障场景（需先启动）
bash scripts/stop-all.sh
```

运行数据都在 `runtime/`（已 gitignore）：SQLite、spool、incoming、library、
quarantine、浏览器下载目录与日志。

## 关联机制（为什么不会配错）

1. **身份**：门户为每个任务签发 `task_id` + 不透明 HMAC `token`，下载 URL、响应头、
   manifest 三处携带并互相比对。浏览器为每次实际下载生成 `download_id` 作为去重键。
2. **两条独立通道**：下载事件（created/completed/interrupted）走事件通道只登记；
   manifest+文件体走文件通道，先写 `spool/<id>.part` 再 `os.replace` 原子落
   `incoming/<id>.bin`。任一通道先到都收敛，重复提交幂等。
3. **入库三关**（worker，短路）：
   - 任务关：门户 `verify` 校验 token 与任务状态（cancelled/superseded/unknown 即隔离）；
   - 筛选关：manifest 的 filter 与门户权威 filter 规范化比对；
   - 内容关：剥离 manifest 尾块后重算 SHA-256，与 manifest、门户哈希三方一致才入库。
4. **内容寻址**：库内路径 `library/<task_id>/<sha16>-<filename>`，同名文件天然不覆盖。
5. **自描述尾块**：下载文件末尾追加 `# __MANIFEST__=<base64url>`，浏览器在上传前退出后，
   重启扫描下载目录即可重建 envelope，不依赖文件名。

## 故障注入开关（页面可控）

| 开关 | 复现的情况 | 系统行为 |
|---|---|---|
| SSE 延迟/重复/丢弃 | 通知乱序、重复、缺失 | 去重 + 对账，文件仍可下载入库 |
| 下载分块延迟 / 阻塞门 | 通知先于文件写完、在途取消 | spool 原子落盘；迟到文件进隔离 |
| 下载内容替换 | 内容哈希不一致 | 内容关失败 → quarantine 留存证据 |
| 采集器丢弃 ACK | 入库成功无回执 | 租约过期重投，ledger 幂等去重 |
| spool 延迟 | 上传中断 | `.part` 对账标 orphan_partial |
| 浏览器上传前崩溃 | 浏览器退出 | 自描述文件 + 重启扫描恢复 |

## 真实 Chrome 扩展

`extension/` 是可直接在 `chrome://extensions` 以"加载已解压的扩展程序"安装的 MV3 扩展，
与无头虚拟浏览器 import 同一份 `core.js`，保证两条执行路径行为一致。
