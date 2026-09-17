# 卫星天线控制租约服务（Antenna Control Lease Service）

纯后端服务。卫星过站期间，两套（或多套）上行控制程序可能同时争抢同一副天线。
本服务保证：

- **任一时刻每副天线至多存在一个未到期租约**；
- **数据库时间（PostgreSQL `clock_timestamp()`）是唯一时钟**，应用服务器从不读取本机时间；
- 成功响应即使在链路中丢失，控制端用**同一幂等键 + 相同参数**重试也只会拿回
  **原来的令牌和原来到期时间**，绝不会产生第二份控制权；
- 同一幂等键携带**不同参数**重试返回**稳定冲突**；
- **过站窗口临时延长**：持有方可在租约仍有效时凭令牌**追加 5–120 秒**
  （`POST /leases/{lease_token}/renew`），时长从**当前到期时间**继续累加，
  无需释放控制权后重新参与争抢；同键同参重试只重放第一次续期结果（`replay: true`），
  绝不会二次延长；
- 租约到期的**边界归新请求**所有：`expires_at <= clock_timestamp()` 即视为已交接，
  未到期的争抢不会改写任何现有记录；
- 过站期间控制程序连续下发多条指令，值班人员可凭令牌**上报已确认执行到的指令序号**
  （`POST /leases/{lease_token}/progress`）：序号只能递增，相同序号重放返回原记录时间，
  上报**不会延长租约占用期限**；
- 未知天线、越界租期返回稳定错误，且**不落任何库记录**。
- **控制代次（control_generation）防脑裂**：上行设备在网络分区恢复后可能继续发送
  旧租约指令。每次成功取得天线，租约都会固化该天线**单调递增的控制代次**
  （每副天线独立计数，从 1 开始，逐次 +1）；设备只服从所见最大代次的持有者，
  过期控制者的指令可被直接拒绝。天线忙、未知天线、参数冲突、输入拒绝等
  失败路径**不消耗代次**——失败后的首次成功恰好紧接该天线上一次已提交租约的代次。

技术栈：Python 3.12 · FastAPI · SQLAlchemy 2 · PostgreSQL 16（`pgcrypto`）· Alembic · pytest。

---

## 1. 目录结构

```
.
├── app/                    # FastAPI 应用
│   ├── main.py             # 应用入口、异常处理、/health
│   ├── config.py           # 环境变量配置（DATABASE_URL、租期边界）
│   ├── db.py               # SQLAlchemy 引擎
│   ├── errors.py           # 统一错误信封 {error:{code,message,details}}
│   ├── schemas.py          # Pydantic 请求/响应模型
│   ├── services.py         # 原子获取租约的核心事务逻辑
│   └── routers/            # HTTP 路由（leases、catalog）
├── alembic/                # 迁移脚本（初始迁移含 6 副预置天线种子数据）
├── tests/                  # 连接真实 PostgreSQL 的验收测试
├── Dockerfile              # 多阶段：api 镜像 / verify 镜像
├── docker-compose.yml      # db + api + 一次性 verify
├── entrypoint.sh           # 先迁移后启动 API
├── requirements.txt
└── requirements-dev.txt    # 额外含 pytest、httpx
```

预置天线（迁移写入 `antennas` 表，`GET /antennas` 可查）：

| 编号 | 名称 |
| --- | --- |
| ANT-01 … ANT-06 | Beijing / Sanya / Kashgar / Kunming / Urumqi / Harbin uplink array |

---

## 2. 一键启动（Docker Compose）

```bash
# 默认宿主端口 8000；可用 API_PORT 覆盖
docker compose build
docker compose up -d db api

# 查看 API
curl http://localhost:8000/health
curl http://localhost:8000/antennas
```

换宿主端口：

```bash
API_PORT=18080 docker compose up -d db api
curl http://localhost:18080/health
```

API 容器启动时由 `entrypoint.sh` **自动执行 `alembic upgrade head`**，
等数据库健康检查通过后才开始迁移与启动。

### 一次性验收服务 verify

`verify` 服务会等待 `db` 与 `api` 都健康，然后针对**真实 API + 真实 PostgreSQL**
跑完整 pytest（并发争抢、幂等重放、参数冲突、到期交接、输入拒绝、指令进度上报），结束即退出，
退出码即验收结论，**不会重启**：

```bash
docker compose up --build verify
# 或在已启动栈的基础上单独运行：
docker compose run --build verify
```

验收测试会通过独立的数据库连接截断 `leases` / `idempotency_keys` /
`lease_renewals` / `renewal_idempotency_keys` 表并把各天线的
`last_control_generation` 归零（计数器派自已提交的租约，租约清空后从 0 重启），
以保证用例独立，因此请在测试环境运行（预置天线目录不会被清除）。

---

## 3. 数据库迁移（手动执行）

本地不使用容器入口脚本时：

```bash
python3.12 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
export DATABASE_URL="postgresql+psycopg2://satctl:satctl@localhost:5432/satctl"

alembic upgrade head      # 建表、启用 pgcrypto、写入预置天线
alembic current           # 查看当前版本
alembic downgrade base    # 回滚全部迁移
```

迁移内容：

- `alembic/versions/0001_initial.py`：
  - `antennas(id PK, name, created_at)` + 6 行预置天线；
  - `leases(id BIGINT PK, antenna_id FK, controller, token UNIQUE,
    acquired_at, expires_at)`，校验 `expires_at > acquired_at`，
    索引 `(antenna_id, expires_at)` 服务活跃租约查询；
  - `idempotency_keys(idempotency_key PK, lease_id FK NOT NULL,
    request_params, created_at)`。
- `alembic/versions/0002_lease_progress.py`：`leases` 增加两个可空字段
  `last_command_sequence BIGINT` 与 `last_progress_at TIMESTAMPTZ`，并加两条
  CHECK：序号非负、两字段同生共灭（`(seq IS NULL) = (time IS NULL)`）。
  **只加列不补数据**——迁移前的历史租约与获取接口新建的租约在首次上报前两个字段
  均为 `NULL`。
- `alembic/versions/0002_lease_release.py`：在进度迁移之后增加
  `released_at TIMESTAMPTZ NULL`。该值只由数据库时钟在提前释放时写入；
  `NULL` 表示仍未主动让权。
- `alembic/versions/0003_lease_renew.py`：增加
  `lease_renewals(id, lease_id FK, previous_expires_at, new_expires_at,
  extra_seconds, created_at)`（CHECK 保证
  `new_expires_at = previous_expires_at + make_interval(extra_seconds)`
  且 `5 <= extra_seconds <= 120`）与续期专用幂等表
  `renewal_idempotency_keys(idempotency_key PK, renewal_id FK,
  request_params, created_at)`。续期幂等键与获取幂等键分表存放，
  两类操作各自独立去重；迁移不改动任何历史租约。
- `alembic/versions/0004_control_generation.py`：增加
  `antennas.last_control_generation BIGINT NOT NULL`（该天线已分配的
  最大代次，初值 0）与 `leases.control_generation BIGINT NOT NULL`
  （租约取得时固化的代次，CHECK `>= 1`），并以唯一索引
  `(antenna_id, control_generation)` 保证同一副天线的代次绝不重复发放。
  **历史租约按同一天线的取得时间与记录编号**（`acquired_at, id`）
  顺序补写 1..N 的代次，天线计数器接续到该最大值，因此升级后历史查询
  返回稳定结果，且升级后的首次获取与历史代次无缝衔接。

---

## 4. 调用方法

### 4.1 获取租约 `POST /leases`

请求体：

| 字段 | 类型 | 约束 |
| --- | --- | --- |
| `antenna_id` | string | 必须为预置天线编号 |
| `controller` | string | 控制者标识，1–128 字符 |
| `duration_seconds` | int | 租期，闭区间 **[5, 120]** 秒 |
| `idempotency_key` | string | 调用方生成的幂等键，1–128 字符 |

成功 `200`：

```json
{
  "antenna_id": "ANT-01",
  "controller": "gs-beijing-A",
  "lease_token": "k3J9...（32 随机字节的 base64url 无填充编码，43 个字符，仅含 A–Z a–z 0–9 - _，可直接放进 URL 路径段；PostgreSQL CSPRNG 生成，不可预测）",
  "acquired_at": "2026-09-12T04:00:00.123456+00:00",
  "expires_at": "2026-09-12T04:00:30.123456+00:00",
  "control_generation": 7,
  "replay": false
}
```

`control_generation` 是该天线**单调递增的控制代次**：每次成功取得天线时，
获取事务在天线行锁内把 `antennas.last_control_generation` 加一并固化进租约。
上行设备在网络分区恢复后可能继续发送旧租约的指令——地面站只服从所见最大代次
的持有者，过期控制者的指令可直接拒绝。代次**只在成功取得时分配**：
天线忙、未知天线、幂等参数冲突、输入校验拒绝都不消耗代次，失败后的首次成功
恰好紧接该天线上一次已提交租约的代次（无空洞）。同键同参重放返回**原代次**，
令牌查询返回的是租约取得时固化的值。

时间戳在所有接口（获取、重放、令牌查询）中使用**完全一致的 ISO-8601 带显式偏移量**
表示（`+00:00`，不混用 `Z`），可逐字节比较；同一个 `expires_at` 在获取与查询响应中
字符串相同。

- `lease_token` 不可预测且 URL 安全：由数据库 `gen_random_bytes(32)` 生成后做
  base64url 转换（`+→-`、`/→_`、去掉 `=` 填充，43 字符），可直接用于
  `GET /leases/{lease_token}` 路径；
- `expires_at = clock_timestamp() + duration_seconds`，完全由数据库计算；
- 同键同参重试返回 `200` 且 `replay: true`，令牌与到期时间与首次完全一致。

curl：

```bash
curl -sS -X POST http://localhost:8000/leases \
  -H 'Content-Type: application/json' \
  -d '{
    "antenna_id": "ANT-01",
    "controller": "gs-beijing-A",
    "duration_seconds": 30,
    "idempotency_key": "pass-2026-09-12-ANT01-7f3a"
  }'
```

### 4.2 错误响应

统一信封：`{"error": {"code", "message", "details"}}`

| HTTP | code | 触发条件 | 是否落库 |
| --- | --- | --- | --- |
| 404 | `ANTENNA_NOT_FOUND` | 未知天线编号 | 否 |
| 422 | `VALIDATION_ERROR` | 租期越界（非 5–120 整数）、缺字段、空白、多余字段 | 否 |
| 409 | `ANTENNA_BUSY` | 存在未到期租约（`details.expires_at` 给出交接时间） | 否 |
| 409 | `IDEMPOTENCY_CONFLICT` | 同幂等键但参数与首次不同（获取或续期均可能） | 否（首次成功请求的记录保留） |
| 404 | `LEASE_NOT_FOUND` | `GET /leases/{token}`、进度上报、续期时令牌未知 | 否 |
| 409 | `LEASE_EXPIRED` | 对已到期租约（`expires_at <= clock_timestamp()`）上报进度或续期 | 否 |
| 409 | `PROGRESS_REGRESSION` | 上报序号小于已确认序号（相同序号视为重放，不是错误） | 否 |
| 422 | `RENEW_DURATION_OUT_OF_RANGE` | 续期追加秒数非 5–120 整数（服务层兜底，HTTP 边界通常先返回 `VALIDATION_ERROR`） | 否 |

`ANTENNA_BUSY` 示例：

```json
{
  "error": {
    "code": "ANTENNA_BUSY",
    "message": "天线 ANT-01 已被未到期租约占用。",
    "details": {
      "antenna_id": "ANT-01",
      "held_by_lease": "原令牌…",
      "expires_at": "2026-09-12T04:00:30.123456+00:00"
    }
  }
}
```

客户端策略建议：拿到 `ANTENNA_BUSY` 后若要在到期后争抢，**必须更换幂等键**再重试；
若是“响应可能丢失”的重试，则保持原键原参数直接重发即可安全重放。

### 4.3 上报指令进度 `POST /leases/{lease_token}/progress`

过站过程中控制程序会连续下发多条指令。值班人员调用本入口确认**当前持有方**
已经执行到哪一条；上报进度**不会改变租约的到期时间**（占用期限保持不变）。

请求体：

| 字段 | 类型 | 约束 |
| --- | --- | --- |
| `sequence` | int | 已确认执行到的指令序号，非负整数（JSON 整数，不接受文本 `"3"`、小数、负数） |

成功 `200`：

```json
{
  "lease_token": "k3J9…",
  "last_command_sequence": 3,
  "last_progress_at": "2026-09-12T04:05:06.789012+00:00"
}
```

- `last_progress_at` 由数据库 `clock_timestamp()` 在同一事务内随 `UPDATE` 生成，
  应用层不参与计时；时间格式与获取/查询接口完全一致（显式 `+00:00`）；
- 序号**只能递增**：更大序号推进高水位并刷新记录时间；
- **相同序号视为重放**：返回**原来的序号和原来的 `last_progress_at`**
  （即使此刻数据库时间已经推进，逐字节不变），不产生写入；
- 更小序号返回 `409 PROGRESS_REGRESSION`（`details` 中带上报序号与当前高水位），
  不写库；
- 未知令牌返回 `404 LEASE_NOT_FOUND`；已到期令牌（边界同样归新请求：
  `expires_at <= clock_timestamp()`）返回 `409 LEASE_EXPIRED`，均不写库。

事务在 READ COMMITTED 内先锁定租约所属的天线行（与获取租约同一把锁），
锁后再读租约，因此并发上报被串行化、提交结果以最新高水位为准，
**最终保留的必然是所有被接受序号中的最大值**。

`GET /leases/{lease_token}` 的响应新增两个可空字段：

```json
{
  "antenna_id": "ANT-01",
  "controller": "gs-beijing-A",
  "lease_token": "k3J9…",
  "acquired_at": "…",
  "expires_at": "…",
  "active": true,
  "control_generation": 7,
  "last_command_sequence": 3,
  "last_progress_at": "2026-09-12T04:05:06.789012+00:00"
}
```

尚未上报过的历史租约与新租约两个字段均为 `null`；迁移不会为现有记录补造进度。
租约到期后查询仍可看到最后一次确认的序号和时间（`active` 变为 `false`）。

curl：

```bash
curl -sS -X POST http://localhost:8000/leases/$LEASE_TOKEN/progress \
  -H 'Content-Type: application/json' \
  -d '{"sequence": 3}'
```

### 4.4 提前释放 `POST /leases/{lease_token}/release`

过站提前结束或控制程序主动让权时，持有方可凭租约令牌立即释放天线，
无需请求体。成功响应与状态查询同构，`active` 为 `false`，并返回由数据库
时钟写入的 `released_at`。重复释放会原样重放第一次结果，不改写时间；
未知令牌返回 `LEASE_NOT_FOUND`，从未释放且已经自然到期的租约返回
`LEASE_EXPIRED`。释放和获取使用同一副天线的行锁，因此并发交接不会产生
两个有效持有方。

```bash
curl -sS -X POST http://localhost:8000/leases/$LEASE_TOKEN/release
```

### 4.5 临时延长过站窗口 `POST /leases/{lease_token}/renew`

过站窗口临时延长时，**当前持有方**可在租约仍有效（未到期、未提前释放）期间，
凭令牌把到期时间向后续 5–120 秒，时长从**当前到期时间继续累加**
（多次续期会逐次叠加，每次使用各自的幂等键），无需释放控制权后重新争抢。

请求体：

| 字段 | 类型 | 约束 |
| --- | --- | --- |
| `extra_seconds` | int | 追加秒数，闭区间 **[5, 120]** 的 JSON 整数（不接受文本 `"10"`、小数、负数） |
| `idempotency_key` | string | 调用方生成的幂等键，1–128 字符（与获取租约的幂等键分表，互不影响） |

成功 `200`：

```json
{
  "lease_token": "k3J9…",
  "previous_expires_at": "2026-09-12T04:05:30.123456+00:00",
  "new_expires_at": "2026-09-12T04:05:50.123456+00:00",
  "replay": false
}
```

- `previous_expires_at` 为续期事务内锁定后读到的**续期前到期时间**，
  `new_expires_at = previous_expires_at + extra_seconds`，差值由数据库 CHECK 约束兜底；
  两个时间戳与全服务一致使用显式 `+00:00` 的 ISO-8601；
- 首次成功 `replay: false`；**同一幂等键 + 相同参数**重试返回
  `replay: true`，除该标记外 `lease_token` / `previous_expires_at` /
  `new_expires_at` 与首次结果**逐字节一致**，只延长一次；
- 同一幂等键携带**不同追加秒数**（或用于另一个令牌）返回
  `409 IDEMPOTENCY_CONFLICT`，不改写任何内容；原参数仍可正常重放；
- 未知令牌返回 `404 LEASE_NOT_FOUND`；已到期或已提前释放的令牌返回
  `409 LEASE_EXPIRED`，均不写库，也不会影响后来持有者；
- 续期与获取、上报、释放使用**同一把天线行锁**，并以数据库时间在锁后确认
  租约仍有效；因此在到期边界处续期与争抢获取**只有一方成功**，绝不会出现
  两个控制方。拒绝路径不发任何写语句。

curl：

```bash
curl -sS -X POST http://localhost:8000/leases/$LEASE_TOKEN/renew \
  -H 'Content-Type: application/json' \
  -d '{"extra_seconds": 20, "idempotency_key": "renew-2026-09-12-ANT01-7f3a"}'
```

### 4.6 其他接口

- `GET /leases/{lease_token}` — 查询租约与 `active` 状态（以数据库时间实时计算），
  含取得时固化的 `control_generation`、上述两个可空进度字段和可空的 `released_at`；
- `GET /antennas` — 预置天线目录；
- `GET /health` — 存活探针，返回数据库时钟 `database_time`；
- 交互式文档：`GET /docs`（Swagger UI）。

---

## 5. 并发与正确性如何保证

获取租约在单个 READ COMMITTED 事务内按固定顺序加锁：

1. `pg_advisory_xact_lock(hashtext(:idempotency_key))`
   —— 相同幂等键的事务串行化，杜绝“丢响应后重试”产生第二份租约；
2. 查 `idempotency_keys`：参数一致 → 重放原令牌/原到期时间；不一致 → 稳定冲突；
3. `SELECT ... FROM antennas WHERE id=:id FOR UPDATE`
   —— 串行化同天线的所有争抢者，同时完成“天线必须存在”的校验（未知天线在此之前无任何写入）；
4. 以 `released_at IS NULL AND expires_at > clock_timestamp()` 判定活跃租约：
   未到期且未释放 → `ANTENNA_BUSY`；已释放或 `expires_at` 已到达 → 归新请求；
5. 在天线行锁内把 `antennas.last_control_generation` 加一，将新代次作为
   `control_generation` 写入新租约，并同事务插入幂等记录后提交。
   计数器只在**将要提交**的事务内移动：并发交接只有新持有者得到更大代次，
   任何拒绝路径（忙、未知天线、参数冲突、输入非法）都会随回滚撤销这次自增，
   因此失败从不消耗代次，成功序列无空洞。

进度上报（`POST /leases/{token}/progress`）在同样的单事务模型内：

1. 按令牌找到租约（未知令牌在任何写操作之前返回 `LEASE_NOT_FOUND`）；
2. `SELECT id FROM antennas WHERE id = :antenna_id FOR UPDATE`
   —— 锁定租约所属天线，并发上报（及该天线的获取事务）在此串行化；
3. 锁后重新读取租约的最新高水位，并复核租约仍未到期且未提前释放
   （否则 `LEASE_EXPIRED`，不写库）；
4. 相同序号 → 重放原记录时间；更小序号 → `PROGRESS_REGRESSION`；
   更大序号才执行 `UPDATE ... SET last_command_sequence=:seq,
   last_progress_at=clock_timestamp()`。

所有“当前时间”和到期时间都在 SQL 内由 PostgreSQL 产生，应用层没有任何时间判断。

提前释放同样先锁定租约所属天线，再在锁内重读租约。第一次释放写入
`released_at = clock_timestamp()`；重复释放重放原时间；自然到期且从未释放的
记录保持不变。获取租约的活跃谓词同时要求 `released_at IS NULL`，所以释放提交后
下一位控制者可以立即取得天线。

临时续期（`POST /leases/{token}/renew`）遵循同一模型：

1. `pg_advisory_xact_lock(hashtext(:idempotency_key))`
   —— 相同续期幂等键的事务串行化（续期幂等键与获取幂等键分表）；
2. 先按令牌确认租约存在（未知令牌在任何写操作之前返回 `LEASE_NOT_FOUND`），
   再查重：同键同参 → 重放第一次续期的续期前/续期后到期时间（`replay: true`）；
   同键不同参数 → 稳定 `IDEMPOTENCY_CONFLICT`；
3. `SELECT id FROM antennas WHERE id = :antenna_id FOR UPDATE`
   —— 与获取/上报/释放同一把天线行锁，续期与到期交接在此串行化；
4. **锁后**以数据库时间复核租约仍未到期且未提前释放
   （`expires_at > clock_timestamp()`，边界归争抢方），否则 `LEASE_EXPIRED` 不写库；
5. 在同一条原子语句里把 `expires_at` 从**当前值**顺延 `extra_seconds`、
   写入 `lease_renewals` 历史与续期幂等记录。新到期时间 = 续期事务内读到的
   原到期时间 + 追加秒数（数据库 CHECK 约束保证等式成立）。

因为续期与新获取抢的是同一把天线行锁，且双方都以锁后看到的数据库时间和
`expires_at` 严格比较，在到期边界处两者**恰有一方成功**：要么持有方顺延成功、
所有争抢者收到指向新边界的 `ANTENNA_BUSY`；要么边界已过、续期收到
`LEASE_EXPIRED` 且恰有一位争抢者建约。

---

## 6. 本地运行测试（不使用 verify 容器）

需要一个真实 PostgreSQL（例如 `docker compose up -d db`）：

```bash
pip install -r requirements-dev.txt
alembic upgrade head
uvicorn app.main:app --port 8000 &           # 或直接 docker compose up -d api

export API_BASE_URL="http://localhost:8000"
export DATABASE_URL="postgresql+psycopg2://satctl:satctl@localhost:5432/satctl"
pytest
```

测试内容：

- `tests/test_input_validation.py` — 未知天线、租期越界/非整数（含文本 `"30"`）、缺字段/空白、
  边界值（5 与 120）、拒绝路径零落库；
- `tests/test_token_safety.py` — 令牌为 URL 安全的 base64url（无 `/ + =`）、
  2000 次抽样数据库令牌生成器、刚获取的租约可经路径查询详情、获取/查询/重放三端
  `expires_at`/`acquired_at` 逐字节同格式、文本租期拒绝零落库；
- `tests/test_idempotency.py` — 原令牌/原到期重放、令牌不可预测、三类参数冲突稳定、
  过期后同键仍重放；
- `tests/test_concurrency.py` — 12 路屏障并发争抢空闲天线仅 1 胜、
  在任者不被改写、同键并发自身重试只有一份租约、发散参数 fanout、
  多天线互不干扰、连续争抢波；
- `tests/test_expiry.py` — SQL 造到期租约验证原子交接、到期边界归新请求、
  以及经 API 的 5 秒最短租约端到端到期交接；
- `tests/test_progress.py` — 获取后连续上报并查询最新进度、同序号重放返回
  稳定的原记录时间、更小序号 `PROGRESS_REGRESSION` 不写库、未知/已到期令牌
  分别 `LEASE_NOT_FOUND` / `LEASE_EXPIRED` 且数据不变、非法请求体 422、
  20 路屏障并发上报最终保留最大序号、到期交接后旧令牌被拒新持有方可上报。
- `tests/test_release.py` — 活跃租约提前释放后立即交接、重复释放稳定重放、
  自然到期拒绝写入，以及释放与争抢并发时不产生双重控制权。
- `tests/test_renew.py` — 获取后续期并在新边界完成交接（旧边界仍持有）、
  同键重试（含 8 路屏障并发）只延长一次且业务字段逐字节一致、不同键续期叠加、
  同键改参/换令牌为 `IDEMPOTENCY_CONFLICT`、未知/已到期/已释放令牌拒绝且
  零落库且不影响后来持有者、输入校验（4/121、文本、缺字段、多余字段、边界值）、
  续期与争抢在到期边界三波并发恰有一方成功且无双重控制权。
- `tests/test_control_generation.py` — 控制代次：连续交接（提前释放与自然到期）
  代次严格递增且无空洞、获取/重放/令牌查询三处代次一致、同键并发重试只提交
  一个代次、12 路屏障并发争抢空闲天线只提交一个新代次、忙/未知天线/参数冲突/
  输入拒绝前后天线计数器与租约数据均不变且失败后首次成功紧接上一条已提交租约、
  各天线代次互相独立；并在独立的临时数据库上验证 0004 迁移按
  `(acquired_at, id)` 顺序为历史租约回填代次、计数器接续历史最大值、
  降级再升级回填结果确定一致。

测试不使用任何固定响应或假接口：全部通过 HTTP 打向真实服务，并直连真实
PostgreSQL 制造并发、播种到期数据和断言提交结果。

---

## 7. 配置项（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `API_PORT` | `8000` | compose 映射到宿主的端口（容器内固定 8000） |
| `DATABASE_URL` | compose 内自动指向 `db` | SQLAlchemy/psycopg2 连接串 |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | `satctl` | 数据库初始化凭据 |
| `API_BASE_URL`（仅 verify） | compose 内 `http://api:8000` | 验收测试访问的 API 地址 |
