# Local Webhook Relay（本地 Webhook 投递中继）

基于 **FastAPI + SQLite** 的本地 Webhook 投递中继服务：创建带签名密钥的订阅，提交事件后为每个订阅生成独立的持久化投递任务，由后台并发工作器可靠投递，失败按指数退避自动重试。

## 功能特性

- **订阅管理**：创建/查询/删除订阅（目标 URL + secret）
- **订阅暂停/恢复**：`POST /subscriptions/{id}/pause` 暂停后不再领取该订阅的新任务（已 `in_progress` 的尝试正常完成），暂停期间提交的事件照常入库并保持 `pending`；`/resume` 恢复后按原 `next_attempt_at` 与 `task_id` 顺序继续投递，不批量改写调度时间；暂停状态持久化在 SQLite 中，重启后保持
- **签名密钥平滑轮换**：`POST /subscriptions/{id}/rotate-secret` 原子切换订阅密钥；宽限期（0～86400 秒）内每次投递同时携带按新密钥生成的 `X-Webhook-Signature` 与按旧密钥生成的 `X-Webhook-Signature-Previous`，期满后只发新签名；宽限期内重复轮换返回 `409`，轮换状态持久化、重启后保持，重试按每次实际发送时的状态签名
- **按订阅投递熔断器**：同一订阅连续最终失败达到 `BREAKER_FAILURE_THRESHOLD` 次即原子熔断（`open`），熔断期间事件照常入库但工作器不再领取；冷却 `BREAKER_COOLDOWN_SECONDS` 秒后仅放行最早待投递任务作为唯一 half-open 探针——成功则关闭并清零计数，失败则重新熔断并刷新冷却时间；状态持久化在 SQLite 中，重启后保持
- **HMAC 受保护入站接口**：`POST /ingest` 使用 `INGEST_SECRET` 校验 `X-Webhook-Signature`（覆盖时间戳、ID 与原始 JSON），并强制 ±300 秒时间窗
- **入站幂等与防重放**：已接收的 `X-Webhook-Id` 持久化在 SQLite 中；重复 ID + 相同内容返回首次接收结果且不重复投递，相同 ID 不同内容返回 `409`
- **事件投递**：一次提交可指定多个订阅，每个订阅生成独立任务
- **事件级撤销**：`POST /events/{id}/cancel` 在单个 SQLite 原子事务中仅撤销尚未被工作器认领（`pending`）的投递任务；已在投递中（`in_progress`）或已结束（`delivered`/`failed`）的任务保持原结果。被撤销任务进入终态 `cancelled`：不再重试、不参与熔断计数、不阻塞同订阅后续任务；响应返回各类数量，重复撤销幂等，不存在的事件返回 `404`，撤销状态在 `GET /events/{id}` 中明确展示
- **按订阅严格有序**：同一订阅的任务按 `task_id` 升序（即提交顺序）依次投递——前一条任务处于 `pending`/`in_progress` 时后续任务不会被认领，前一条 `delivered` 或 `failed` 后才放行下一条；不同订阅之间互不影响，仍可由多个工作器并行投递
- **持久化与断点续投**：任务存储在 SQLite 中，服务重启后自动恢复未完成任务（包括崩溃时处于 `in_progress` 状态的任务），有序约束在重启后同样生效
- **并发安全**：多个工作器通过数据库级原子认领（SELECT + UPDATE 在同一临界区内）保证同一轮尝试不会被重复领取
- **可靠重试**：非 2xx、连接错误、超时均视为失败，按 **1s → 2s → 4s** 退避重试，最多 **4 次**尝试后标记为失败
- **幂等去重**：`(event_id, subscription_id)` 唯一约束，重复提交不会产生重复任务
- **HMAC-SHA256 签名**：每个投递请求携带签名头，接收方可校验来源与完整性
- **状态查询**：按事件查询各目标的尝试次数、下次执行时间、最终结果及完整尝试历史

## 快速开始

### Docker（推荐，一键启动）

```bash
docker compose up --build
```

一条命令完成镜像构建与启动：服务监听容器内 **8000** 端口，`docker-compose.yml` 通过 `"8000:8000"` 映射到宿主机同一端口，启动后访问 **http://localhost:8000**（交互式 API 文档：http://localhost:8000/docs，健康检查：http://localhost:8000/healthz）。
SQLite 数据库保存在名为 `webhook-data` 的卷（容器内 `/data/webhook_relay.db`）中，容器重启后任务不丢失，老版本数据库在启动时自动迁移。
熔断阈值与冷却时间等配置均可在 `docker-compose.yml` 的 `environment` 中调整（见文末配置表）。

`/ingest` 的密钥默认使用 compose 内置的开发值 `dev-ingest-secret-change-me`，生产环境请通过环境变量覆盖：

```bash
INGEST_SECRET="$(openssl rand -hex 32)" docker compose up --build
```

### 本地运行

```bash
pip install -r requirements.txt
export INGEST_SECRET="dev-ingest-secret-change-me"   # 启用 POST /ingest
uvicorn app.main:app --host 0.0.0.0 --port 8000     # 监听 http://localhost:8000
```

### 一分钟冒烟（端口 8000）

```bash
# 1) 建订阅  2) 提交事件  3) 撤销尚未投递的任务  4) 查询撤销后的状态
SID=$(curl -s -X POST http://localhost:8000/subscriptions \
  -H 'Content-Type: application/json' \
  -d '{"target_url": "http://localhost:9001/hook", "secret": "my-secret"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')

curl -s -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d "{\"event_id\": \"evt-demo\", \"subscription_ids\": [\"$SID\"], \"payload\": {\"msg\": \"hi\"}}"

curl -s -X POST http://localhost:8000/events/evt-demo/cancel
curl -s http://localhost:8000/events/evt-demo
```

## API 使用示例

### 1. 创建订阅

```bash
curl -s -X POST http://localhost:8000/subscriptions \
  -H 'Content-Type: application/json' \
  -d '{"target_url": "http://localhost:9001/hook", "secret": "my-secret"}'
```

```json
{"id": "44a79d7077ce", "target_url": "http://localhost:9001/hook", "created_at": "2026-09-17T11:11:24+00:00", "paused": false, "paused_at": null, "rotating": false, "previous_secret_expires_at": null, "state": "closed", "consecutive_failures": 0, "next_probe_at": null}
```

### 2. 提交入站事件（HMAC 保护，POST /ingest）

`/ingest` 面向外部事件源，使用共享密钥 `INGEST_SECRET` 做 HMAC-SHA256 认证。
每个请求必须携带三个头部：

| 头部 | 说明 |
|------|------|
| `X-Webhook-Timestamp` | 发送时刻 Unix 时间戳（秒），与服务端时差不得超过 **±300 秒** |
| `X-Webhook-Id` | 事件唯一 ID（最长 128 字符），用于幂等去重 |
| `X-Webhook-Signature` | `sha256=<HMAC-SHA256(secret, timestamp + "." + id + "." + 原始请求体)>` |

请求体为原始 JSON，**必须**包含 `subscription_ids`（非空数组，1–100 个），其余字段原样转发给投递目标：

```bash
SECRET="dev-ingest-secret-change-me"
TS=$(date +%s)
ID="evt-ingest-001"
BODY='{"subscription_ids": ["44a79d7077ce"], "type": "order.paid", "order_id": 42}'
SIG="sha256=$(printf '%s' "${TS}.${ID}.${BODY}" | openssl dgst -sha256 -hmac "$SECRET" | sed 's/^.*= //')"

curl -s -X POST http://localhost:8000/ingest \
  -H 'Content-Type: application/json' \
  -H "X-Webhook-Timestamp: $TS" \
  -H "X-Webhook-Id: $ID" \
  -H "X-Webhook-Signature: $SIG" \
  -d "$BODY"
```

Python 生成签名示例：

```python
import hashlib, hmac, time

secret = "dev-ingest-secret-change-me"
ts = str(int(time.time()))
webhook_id = "evt-ingest-001"
body = b'{"subscription_ids": ["44a79d7077ce"], "type": "order.paid"}'
signed = ts.encode() + b"." + webhook_id.encode() + b"." + body
sig = "sha256=" + hmac.new(secret.encode(), signed, hashlib.sha256).hexdigest()
```

验签与幂等行为：

- **首次验证成功**：`201 Created`，按 `subscription_ids` 创建投递任务，响应与 `POST /events` 相同（`created` / `duplicates`）。
- **重复 ID + 相同原始 body**：`200 OK`，返回与首次完全一致的接收结果，**不会重复创建投递任务**（含并发重试：多个相同请求同时到达也只有一个 `201`）。
- **相同 ID 但 body 不同**：`409 Conflict`，不产生任何任务。
- **签名错误**（含签名未覆盖时间戳/ID/原始 body）：`401 Unauthorized`。
- **时间戳超出 ±300 秒窗口**或无法解析：`401 Unauthorized`。
- **缺少必需头部**、空 body、body 非 JSON：`400 Bad Request`；body 缺少 `subscription_ids`：`422`。
- **包含不存在的订阅 ID**：`400 Bad Request`，且该事件不会持久化。
- 服务端未配置 `INGEST_SECRET` 时端点失败关闭，返回 `503`。

入站事件的 `X-Webhook-Id` 即事件 ID，可用 `GET /events/{id}` 查询投递状态；投递给订阅目标的出站请求体就是 `/ingest` 收到的**原始 JSON**（出站签名使用该订阅自己的 secret，见下文）。

### 3. 提交事件（内部接口 POST /events）

```bash
curl -s -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d '{
        "event_id": "evt-001",
        "subscription_ids": ["44a79d7077ce", "94d3c624dad2"],
        "payload": {"msg": "hello", "n": 1}
      }'
```

```json
{
  "event_id": "evt-001",
  "created": [
    {"task_id": 1, "subscription_id": "44a79d7077ce", "status": "pending"},
    {"task_id": 2, "subscription_id": "94d3c624dad2", "status": "pending"}
  ],
  "duplicates": []
}
```

- 重复提交相同 `event_id` + 订阅：返回 `200`，已存在的任务出现在 `duplicates` 中，不会重复投递
- 相同 `event_id` 但 payload 不同：返回 `409 Conflict`
- 包含不存在的订阅 ID：返回 `400`

### 4. 查询事件状态

```bash
curl -s http://localhost:8000/events/evt-001
```

```json
{
  "event_id": "evt-001",
  "created_at": "2026-09-17T11:11:24+00:00",
  "targets": [
    {
      "task_id": 2,
      "subscription_id": "94d3c624dad2",
      "target_url": "http://localhost:9002/hook",
      "status": "delivered",
      "attempts": 3,
      "max_attempts": 4,
      "next_attempt_at": null,
      "last_status_code": 200,
      "last_error": null,
      "result": "delivered",
      "history": [
        {"attempt_no": 1, "status_code": 500, "error": "non-2xx response: 500", ...},
        {"attempt_no": 2, "status_code": 500, "error": "non-2xx response: 500", ...},
        {"attempt_no": 3, "status_code": 200, "error": null, ...}
      ]
    }
  ]
}
```

`status` 取值：`pending`（等待执行）/ `in_progress`（投递中）/ `delivered`（成功）/ `failed`（最终失败）/ `cancelled`（随事件撤销，从未投递）。
`next_attempt_at` 仅在 `pending` 时有值，即下次执行时间。

事件是否被撤销过在顶层明确展示：`cancelled`（布尔，只要有一个任务为 `cancelled` 即为 `true`）与 `cancelled_at`（最近一次实际撤销动作的 ISO 时间，从未撤销为 `null`）；`counts` 给出该事件各状态任务数量（`pending` / `in_progress` / `delivered` / `failed` / `cancelled`），被撤销的任务还带有各自的 `cancelled_at`：

```json
{
  "event_id": "evt-001",
  "created_at": "2026-09-17T11:11:24+00:00",
  "cancelled": true,
  "cancelled_at": "2026-09-18T09:00:00+00:00",
  "counts": {"pending": 0, "in_progress": 0, "delivered": 1, "failed": 0, "cancelled": 1},
  "targets": [
    {"task_id": 1, "subscription_id": "44a79d7077ce", "status": "delivered", "cancelled_at": null, ...},
    {"task_id": 2, "subscription_id": "94d3c624dad2", "status": "cancelled", "cancelled_at": "2026-09-18T09:00:00+00:00", ...}
  ]
}
```

注意：若同一订阅存在更早的未完成任务，后续任务会因有序调度而保持 `pending`（被前序任务阻塞），状态查询会如实展示这些被阻塞的任务。

### 5. 撤销事件投递（POST /events/{id}/cancel）

撤销一个事件**尚未被工作器认领**的投递任务：

```bash
curl -s -X POST http://localhost:8000/events/evt-001/cancel
```

```json
{
  "event_id": "evt-001",
  "cancelled": 1,
  "cancelled_at": "2026-09-18T09:00:00+00:00",
  "counts": {"pending": 0, "in_progress": 1, "delivered": 0, "failed": 0, "cancelled": 1}
}
```

行为约定：

- **原子只撤未认领任务**：撤销在单个 SQLite 事务中完成，与工作器的认领操作互斥；只有撤销瞬间仍为 `pending` 的任务会被置为终态 `cancelled`（包括退避等待重试中的任务）。已被工作器认领（`in_progress`）的 HTTP 投递不会被中断，正常完成并记录真实结果；已结束的 `delivered` / `failed` 任务结果保持不变。
- **返回各类数量**：响应的 `counts` 始终包含全部五种状态的任务数量；`cancelled`（顶层字段）是**本次调用新撤销**的任务数（首次为实际数量，重复调用为 `0`，可用于判断是否幂等命中）。
- **不重试、不计熔断**：`cancelled` 是终态，工作器认领查询只选择 `pending`，故撤销后的任务永远不会再被投递或重试；撤销不经过终态记账逻辑，既不增加也不清零任何订阅熔断器计数。
- **不阻塞同订阅后续任务**：按订阅有序投递只把 `pending` / `in_progress` 的较早任务视为阻塞项，`cancelled` 立即成为终态，同订阅的下一条任务即刻可被认领。
- **幂等**：对同一事件重复调用返回 `200`；若期间没有新的待撤销任务，`cancelled` 为 `0`、`counts` 保持稳定（撤销期间在投递的任务可能恰好结束，其结果如实体现在 `counts` 中，已撤销任务不会变化）。
- **404**：撤销或查询不存在的事件均返回 `404 Not Found`（仅存在于 `ingested_events` 但未建事件的情况不会发生：验签通过的入站事件必同时写入 `events`）。
- **状态可见**：撤销后 `GET /events/{id}` 顶层 `cancelled=true` 并给出事件级 `cancelled_at` 与 `counts`，每个被撤销任务的 `status` 为 `cancelled` 且带有任务级 `cancelled_at`。
- **与暂停的区别**：暂停（`/subscriptions/{id}/pause`）作用于订阅、保留所有任务待恢复后继续投递；撤销作用于单个事件、把该事件的待投递任务永久终止。二者可以组合：暂停期间撤销事件，恢复后该事件的被撤销任务不会补投。
- **重启恢复边界**：服务崩溃时残留的 `in_progress` 任务会在启动时恢复为 `pending`（视作尚未认领），因此恢复后对其事件再次撤销可以将其撤销；未撤销则照常重新投递。

### 6. 暂停与恢复订阅投递

```bash
curl -s -X POST http://localhost:8000/subscriptions/44a79d7077ce/pause
```

```json
{"id": "44a79d7077ce", "target_url": "http://localhost:9001/hook", "created_at": "2026-09-17T11:11:24+00:00", "paused": true, "paused_at": "2026-09-18T08:30:00+00:00", "rotating": false, "previous_secret_expires_at": null, "state": "closed", "consecutive_failures": 0, "next_probe_at": null}
```

```bash
curl -s -X POST http://localhost:8000/subscriptions/44a79d7077ce/resume
```

行为约定：

- **暂停生效边界**：暂停在任务认领时生效——已提交暂停后，工作器不再领取该订阅的任何新任务；暂停前已被认领（`in_progress`）的投递会正常执行完成，暂停不会中断进行中的 HTTP 请求。
- **暂停期间继续入库**：通过 `POST /events` 或 `POST /ingest` 提交的事件照常持久化，任务保持 `pending`（含因退避等待重试的任务，其 `next_attempt_at` 不变），可在 `GET /events/{id}` 中查询到。
- **恢复语义**：恢复只清除订阅上的暂停标记，**不批量改写**任何任务的 `next_attempt_at`；工作器按原到期时间、并在同一订阅内按 `task_id` 严格顺序继续领取——到期时间已过的任务立即被领取，未到期的仍在原时间执行。
- **幂等**：重复暂停/恢复是幂等操作，均返回 `200` 与当前订阅状态（重复暂停保留首次的 `paused_at`）；订阅不存在时返回 `404`。
- **状态可见**：`GET /subscriptions` 与 `GET /subscriptions/{id}` 返回 `paused`（布尔）与 `paused_at`（ISO 时间或 `null`）。
- **持久化**：暂停状态存储在 SQLite 的 `subscriptions.paused_at` 列中，服务（或容器）重启后保持。

### 7. 轮换订阅签名密钥

```bash
curl -s -X POST http://localhost:8000/subscriptions/44a79d7077ce/rotate-secret \
  -H 'Content-Type: application/json' \
  -d '{"new_secret": "my-new-secret", "grace_seconds": 3600}'
```

```json
{"id": "44a79d7077ce", "target_url": "http://localhost:9001/hook", "created_at": "2026-09-17T11:11:24+00:00", "paused": false, "paused_at": null, "rotating": true, "previous_secret_expires_at": "2026-09-18T10:30:00+00:00", "state": "closed", "consecutive_failures": 0, "next_probe_at": null}
```

行为约定：

- **原子切换**：新密钥与旧密钥失效时间在同一个 SQLite 事务中原子落库；自轮换成功起，新投递的 `X-Webhook-Signature` 立即按新密钥生成。
- **宽限期双签名**：`grace_seconds`（0～86400 秒）内，每次出站投递同时携带 `X-Webhook-Signature`（新密钥）与 `X-Webhook-Signature-Previous`（旧密钥）两个签名头；宽限期满后只发送新签名，不再携带 `-Previous` 头。`grace_seconds=0` 表示立即切换，从不双签。
- **宽限期内禁止再次轮换**：上一次轮换的宽限期未结束时再次调用返回 `409 Conflict`（响应中包含当前宽限期截止时间）；期满后可再次轮换，已被淘汰的旧密钥随之丢弃。
- **按发送时状态签名**：密钥状态在每次任务认领（即每次实际发送，含每次重试）时重新读取，宽限期是否结束也在构造请求头时判定——重试中的任务在轮换后自动改用新密钥（宽限期内附带旧签名），宽限期内到期的任务后续尝试只发新签名。
- **持久化**：轮换状态（当前密钥、旧密钥及其失效时间）存储在 SQLite 的 `subscriptions` 表中，服务（或容器）重启后宽限期继续按原截止时间生效。
- **错误码**：订阅不存在返回 `404`；`new_secret` 为空或 `grace_seconds` 超出 0～86400 返回 `422`。
- **密钥保密**：任何查询响应（含本端点响应、`GET /subscriptions*`）都不会包含密钥明文，日志也不记录密钥；响应仅暴露轮换状态 `rotating`（布尔）与 `previous_secret_expires_at`（宽限期截止时间，非轮换期则为 `null`）。

**接收方切换流程**（零中断换密钥）：

1. **接收方先支持双头验签**：验签逻辑改为——`X-Webhook-Signature-Previous` 头存在时，接受任一签名通过（新密钥验 `X-Webhook-Signature`，旧密钥验 `X-Webhook-Signature-Previous`）。
2. **发起轮换**：调用 `POST /subscriptions/{id}/rotate-secret`，`grace_seconds` 取足以完成接收方部署的窗口（如 3600 秒）。
3. **宽限期内更新接收方配置**：将接收方配置的密钥换成新密钥。期间投递始终双签，接收方无论仍用旧密钥还是已切到新密钥都能验签通过，投递不中断。
4. **宽限期满**：中继自动停止发送 `X-Webhook-Signature-Previous`，只发新密钥签名；接收方可移除旧密钥与双头兼容逻辑。可用 `GET /subscriptions/{id}` 观察 `rotating` 变回 `false` 确认窗口已结束。

### 8. 投递熔断器（按订阅）

每个订阅自带一个持久化熔断器，避免对持续故障的目标无休止重试。熔断器状态随订阅查询接口返回：

```bash
curl -s http://localhost:8000/subscriptions/44a79d7077ce
```

```json
{"id": "44a79d7077ce", "target_url": "http://localhost:9001/hook", "created_at": "2026-09-17T11:11:24+00:00", "paused": false, "paused_at": null, "rotating": false, "previous_secret_expires_at": null, "state": "open", "consecutive_failures": 5, "next_probe_at": "2026-09-18T09:31:00+00:00"}
```

三个熔断器字段（`GET /subscriptions`、`GET /subscriptions/{id}` 及各订阅写操作响应均包含）：

| 字段 | 说明 |
|------|------|
| `state` | `closed`（正常投递）/ `open`（熔断中，暂停领取）/ `half_open`（探针投递中） |
| `consecutive_failures` | 当前连续最终失败任务数（成功投递即清零） |
| `next_probe_at` | 仅 `open` 时有值：冷却到期、允许下一次探针的最早时间（ISO 时间） |

状态机行为约定：

- **累计失败（closed）**：某订阅的任务重试耗尽、最终失败（`failed`）时，该订阅的 `consecutive_failures` 加一；任意一次投递成功都会将其清零（只统计**连续**失败）。
- **原子熔断（closed → open）**：连续失败数达到 `BREAKER_FAILURE_THRESHOLD`（默认 5）时，计数更新与状态切换在同一个 SQLite 事务中原子落库，不会出现计数与状态不一致的中间态。
- **熔断期间（open）**：`POST /events` / `POST /ingest` 照常接收该订阅的事件并持久化为 `pending` 任务，但工作器不会领取该订阅的任何任务；`next_probe_at` 显示冷却到期时间。
- **唯一探针（open → half_open）**：冷却到期（`now >= next_probe_at`）后，该订阅**最早的待投递任务**成为唯一 half-open 探针——状态翻转与任务认领在同一原子临界区内完成，多个工作器并发也只会有一个探针；探针的重试按正常退避进行，期间该订阅的其它任务继续等待。
- **探针成功（half_open → closed）**：探针任务投递成功，熔断器关闭、`consecutive_failures` 清零，积压任务按原顺序恢复投递。
- **探针失败（half_open → open）**：探针任务最终失败，熔断器立即重新打开，并以失败时刻为起点刷新冷却时间（`next_probe_at = 失败时刻 + BREAKER_COOLDOWN_SECONDS`）。
- **持久化**：`state`、`consecutive_failures`、`next_probe_at` 及探针归属全部存储在 SQLite 的 `subscriptions` 表中，服务（或容器）重启后熔断状态、冷却截止时间与探针唯一性继续生效。

### 其他端点

| 方法 | 路径 | 说明 |
|------|------|------|
| `POST` | `/ingest` | HMAC 保护的入站事件接口（时间窗 + 签名 + ID 幂等） |
| `POST` | `/events` | 内部提交事件（event_id + payload，无需签名） |
| `GET` | `/events/{id}` | 查询事件投递状态（含 `cancelled` / `cancelled_at` / 各状态 `counts`） |
| `POST` | `/events/{id}/cancel` | 撤销事件未认领的投递任务（原子、幂等，不存在返回 404） |
| `GET` | `/subscriptions` | 列出全部订阅（含 `paused` / `paused_at` 与熔断器 `state` / `consecutive_failures` / `next_probe_at`） |
| `GET` | `/subscriptions/{id}` | 查询单个订阅（含 `paused` / `paused_at` 与熔断器 `state` / `consecutive_failures` / `next_probe_at`） |
| `POST` | `/subscriptions/{id}/pause` | 暂停订阅投递（幂等，不存在返回 404） |
| `POST` | `/subscriptions/{id}/resume` | 恢复订阅投递（幂等，不存在返回 404） |
| `POST` | `/subscriptions/{id}/rotate-secret` | 轮换签名密钥（宽限期双签；宽限期内重复轮换返回 409，不存在返回 404） |
| `DELETE` | `/subscriptions/{id}` | 删除订阅（已有任务保留历史） |
| `GET` | `/healthz` | 健康检查 |

## 按订阅严格有序投递

同一 `subscription_id` 下的投递任务按 `tasks.id` 升序（即任务创建顺序）逐条投递，语义如下：

- 较早任务处于 `pending`（含退避等待重试中）或 `in_progress`（投递中）时，同订阅的后续任务**不会被任何工作器认领**；
- 较早任务到达终态——`delivered`（成功）、`failed`（重试耗尽后最终失败）或 `cancelled`（随事件撤销）——才放行同订阅的下一条任务；
- 不同订阅之间没有顺序约束，`WORKER_COUNT` 个工作器可并行投递不同订阅的任务；
- 约束在认领时基于 SQLite 中的持久化状态判定，因此失败重试、服务重启（含崩溃恢复）后依然有效；
- 被阻塞的任务状态保持 `pending`，通过 `GET /events/{event_id}` 可正常查询到。

## 接收端验签

每个投递请求携带以下头部：

| 头部 | 说明 |
|------|------|
| `X-Webhook-Signature` | `sha256=<HMAC-SHA256(当前 secret, 请求体)>` 十六进制摘要 |
| `X-Webhook-Signature-Previous` | 仅在密钥轮换宽限期内出现：`sha256=<HMAC-SHA256(旧 secret, 请求体)>`，用于接收方平滑切换 |
| `X-Webhook-Event-Id` | 事件 ID |
| `X-Webhook-Task-Id` | 投递任务 ID |
| `X-Webhook-Attempt` | 当前第几次尝试 |
| `X-Webhook-Timestamp` | 发送时间戳（秒） |

Python 验签示例：

```python
import hashlib, hmac

def is_valid(secret: str, body: bytes, signature_header: str) -> bool:
    expected = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature_header)

# 轮换宽限期内：任一签名通过即可（headers 为请求头字典）
def is_valid_during_rotation(new_secret: str, old_secret: str, body: bytes, headers) -> bool:
    if is_valid(new_secret, body, headers.get("X-Webhook-Signature", "")):
        return True
    prev = headers.get("X-Webhook-Signature-Previous")
    return prev is not None and is_valid(old_secret, body, prev)
```

## 设计说明

- **任务模型**：事件存于 `events` 表（payload 只存一份），每个 `(event_id, subscription_id)` 对应 `tasks` 表中一行（唯一约束保证幂等）；每次尝试写入 `attempts` 历史表。
- **入站幂等**：每个验签通过的 `X-Webhook-Id` 在 `ingested_events` 表中记录一行（原始 body 字节 + 首次回执）。查重、冲突判定、建事件、建任务在同一个 SQLite 事务中完成；重放请求直接返回存储的首次回执，即使并发到达也只会有一个请求建任务。
- **原子认领**：工作器在单个连接锁保护的临界区内完成「选中最旧到期任务 → 置为 `in_progress`」，并发工作器不会领到同一任务。
- **按订阅有序**：认领查询附带 `NOT EXISTS` 条件——同订阅存在 id 更小且状态为 `pending`/`in_progress` 的任务时，当前任务不可认领；只有前序任务进入终态（`delivered`/`failed`/`cancelled`）才放行下一条。条件针对持久化状态在认领时求值，故重试退避、崩溃恢复（`in_progress` 重置为 `pending` 后依然阻塞后续任务）与多工作器并发下均成立；不同订阅互不影响，可并行投递。
- **订阅暂停**：暂停只是翻转 `subscriptions.paused_at` 标记（首次暂停的时间戳，重复暂停不变），从不修改任务行。认领查询增加 `s.paused_at IS NULL` 条件，与有序约束在同一原子临界区内求值，因此暂停对已 `in_progress` 的尝试（至多 `WORKER_COUNT` 个并发）无影响、它们照常完成，而新的认领立即停摆；恢复时清除标记即可，所有 `pending` 任务保留原 `next_attempt_at` 与 `task_id` 顺序，无需任何批量更新。
- **密钥轮换**：`subscriptions` 表新增 `previous_secret` 与 `previous_secret_expires_at` 两列（老库由启动迁移自动 `ALTER TABLE` 补齐），二者始终同时设置或同时清空。轮换在连接锁保护的单次提交中原子完成——当前密钥降级为 previous 并写入绝对失效时间，`secret` 列更新为新密钥；`grace_seconds=0` 时直接清空 previous 列。工作器每次认领任务时随连接读出当前密钥状态，构造请求头时按发送时刻判定宽限期是否结束，因此重试、重启后均按当时状态签名；宽限期未结束的再次轮换在判定阶段即被拒绝（409），不产生任何写操作。查询接口只读取失效时间戳推导 `rotating` 标志，密钥明文从不离开持久层进入响应或日志。
- **投递熔断器**：`subscriptions` 表新增 `breaker_state`、`breaker_consecutive_failures`、`breaker_next_probe_at`、`breaker_probe_task_id` 四列（老库由启动迁移自动补齐，存量订阅初始为 `closed`/0）。任务终态（`delivered`/`failed`）与熔断器记账在 `finish_attempt` 的同一个 SQLite 事务中提交：最终失败使连续失败数加一，达到 `BREAKER_FAILURE_THRESHOLD` 即原子切为 `open` 并写入 `next_probe_at = 失败时刻 + BREAKER_COOLDOWN_SECONDS`；任意成功投递清零计数。认领查询按熔断状态过滤——`open` 且冷却未到的订阅不可认领；冷却到期后首个认领把订阅原子翻为 `half_open` 并把该任务（按订阅有序约束保证即最早待投递任务）登记为唯一探针，`half_open` 期间认领查询只允许探针任务本身通过，故多工作器并发下探针唯一；探针成功则关闭并清零，最终失败则重新 `open` 并以该失败时刻刷新冷却。全部状态持久化，重启（含崩溃恢复）后冷却计时与探针唯一性继续生效。
- **崩溃恢复**：启动时将残留的 `in_progress` 任务重置为 `pending` 并立即到期；`pending` 任务按原 `next_attempt_at` 继续调度。崩溃恢复把未完成任务重新视为“尚未认领”，因此若其事件在恢复后被撤销，这些任务会随撤销终止；未撤销则照常重新投递。
- **事件级撤销**：`tasks.status` 增加终态 `cancelled` 并新增 `cancelled_at` 列（新建库直接具备；老库启动迁移时按 SQLite 官方表重建流程——`foreign_keys=OFF`、单个显式事务内建新表、拷贝数据、DROP、RENAME、重建 `tasks` 上的两个索引——自动升级，既有任务数据原样保留）。`POST /events/{id}/cancel` 调用的 `cancel_event` 在连接锁保护的单个事务中执行：事件不存在直接返回 `None`（API 返回 `404`）；否则一条 `UPDATE ... WHERE event_id=? AND status='pending'` 原子地只撤销尚未认领的任务（写入 `cancelled_at`、清空 `next_attempt_at`/`claimed_by`），`in_progress` 与已结束任务不在 WHERE 命中范围内、结果不变；随后按状态 `GROUP BY` 计数返回。由于认领查询只选 `pending` 且有序约束只盯 `pending`/`in_progress`，撤销行既不会被重试、也不会阻塞同订阅后续任务；撤销从不经过 `finish_attempt`，故完全不触碰熔断器记账。唯一需要修复的边角是 half-open 探针在两次尝试之间回到 `pending` 时被撤销：同一事务内把该订阅熔断器从 `half_open` 转回 `open`、探针立即到期并清空探针归属，使下一个待投递任务成为替代探针，避免订阅永久卡在 half-open。重复撤销时 UPDATE 命中 0 行，响应 `cancelled=0` 且计数稳定，天然幂等；撤销与工作器认领共用同一把连接锁，并发下每个任务只可能“被认领（投递并记账）”或“被撤销”，不会重复领取也不会状态错乱。
- **退避策略**：第 n 次失败后延迟 `2^(n-1)` 秒（1、2、4），第 4 次失败后任务终止，结果为 `failed`。
- **成功判定**：仅 HTTP 2xx 视为成功；其余状态码、连接错误、超时均触发重试。

## 配置项（环境变量）

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `INGEST_SECRET` | *无（compose 内为 `dev-ingest-secret-change-me`）* | `POST /ingest` 的 HMAC 共享密钥；未配置时该端点返回 `503` |
| `INGEST_TIMESTAMP_TOLERANCE_SECONDS` | `300` | 入站时间戳允许的最大偏差（秒，正负两个方向） |
| `DB_PATH` | `./webhook_relay.db`（容器内 `/data/webhook_relay.db`） | SQLite 数据库路径 |
| `WORKER_COUNT` | `4` | 并发投递工作器数量 |
| `HTTP_TIMEOUT_SECONDS` | `5` | 单次投递 HTTP 超时 |
| `MAX_ATTEMPTS` | `4` | 单任务最大尝试次数 |
| `BACKOFF_BASE_SECONDS` | `1` | 首次重试延迟（之后每次翻倍） |
| `BREAKER_FAILURE_THRESHOLD` | `5` | 触发订阅熔断的连续最终失败任务数 |
| `BREAKER_COOLDOWN_SECONDS` | `30` | 熔断冷却时间（到期后放行唯一 half-open 探针） |
| `POLL_INTERVAL_SECONDS` | `0.2` | 空闲工作器轮询间隔 |

## 项目结构

```
app/
├── main.py      # FastAPI 应用：API 端点 + 生命周期（启动/停止工作器）
├── worker.py    # 投递工作器：认领任务、HTTP 投递、记录结果
├── db.py        # SQLite 持久层：建表、原子认领、状态更新
├── schemas.py   # Pydantic 请求/响应模型
├── signing.py   # HMAC-SHA256 签名与验签
└── config.py    # 环境变量配置
Dockerfile
docker-compose.yml
requirements.txt
```
