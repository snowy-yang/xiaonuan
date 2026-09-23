"""PostgreSQL 连接层：asyncpg 连接池 + schema + 全部数据库函数。

主程序只 import 这一层，不直接碰 SQL。
连接串从环境变量 PG_DSN 读取，例如：
    postgresql://postgres:密码@localhost:5432/ai_chat
"""

import time

import asyncpg

_pool: asyncpg.Pool | None = None


async def init(dsn: str) -> None:
    """启动时调用：建连接池 + 建表"""
    global _pool
    _pool = await asyncpg.create_pool(dsn, min_size=1, max_size=10)
    async with _pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS history (
                id      BIGSERIAL PRIMARY KEY,
                session TEXT NOT NULL,
                role    TEXT NOT NULL,
                content TEXT NOT NULL,
                ts      DOUBLE PRECISION NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_history_session ON history(session, id);
            CREATE TABLE IF NOT EXISTS memories (
                id      BIGSERIAL PRIMARY KEY,
                session TEXT NOT NULL,
                text    TEXT NOT NULL,
                ts      DOUBLE PRECISION NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_memories_session ON memories(session, id);
            """
        )
        # 向量列：FLOAT8[] 不依赖 pgvector 扩展，任何 PG 都能用
        await conn.execute(
            "ALTER TABLE memories ADD COLUMN IF NOT EXISTS embedding FLOAT8[]"
        )
        # 定时事件（叫早/提醒），status: pending / done；
        # repeat: once / daily（每日任务今天发过没发过看 last_fired）
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id         BIGSERIAL PRIMARY KEY,
                session    TEXT NOT NULL,
                kind       TEXT NOT NULL,
                fire_at    DOUBLE PRECISION NOT NULL,
                text       TEXT NOT NULL,
                status     TEXT NOT NULL DEFAULT 'pending',
                created_at DOUBLE PRECISION NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_events_status ON events(status, fire_at);
            CREATE INDEX IF NOT EXISTS idx_events_session ON events(session, status);
            """
        )
        await conn.execute(
            "ALTER TABLE events ADD COLUMN IF NOT EXISTS repeat TEXT DEFAULT 'once'")
        await conn.execute(
            "ALTER TABLE events ADD COLUMN IF NOT EXISTS minute_of_day INTEGER")
        await conn.execute(
            "ALTER TABLE events ADD COLUMN IF NOT EXISTS last_fired TEXT")
        # 作息规律（她猜出来的上下学时间等）：
        # minute = 一天中的分钟数，evidence = 依据次数，UNIQUE 保证每个标签一行
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS routines (
                id         BIGSERIAL PRIMARY KEY,
                session    TEXT NOT NULL,
                label      TEXT NOT NULL,
                minute     INTEGER NOT NULL,
                evidence   INTEGER NOT NULL DEFAULT 1,
                kind       TEXT NOT NULL DEFAULT 'inferred',
                last_fired TEXT,
                updated_at DOUBLE PRECISION NOT NULL,
                UNIQUE (session, label)
            );
            """
        )
        # 她惦记的事：emotion（情绪回访）/ promise（承诺追踪）
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS care (
                id         BIGSERIAL PRIMARY KEY,
                session    TEXT NOT NULL,
                kind       TEXT NOT NULL,
                note       TEXT NOT NULL,
                due_at     DOUBLE PRECISION NOT NULL,
                status     TEXT NOT NULL DEFAULT 'pending',
                created_at DOUBLE PRECISION NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_care_due ON care(status, due_at);
            """
        )
        # 慢信：写信人自己，到日子才拆
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS letters (
                id          BIGSERIAL PRIMARY KEY,
                session     TEXT NOT NULL,
                body        TEXT NOT NULL,
                deliver_at  DOUBLE PRECISION NOT NULL,
                status      TEXT NOT NULL DEFAULT 'sealed',
                created_at  DOUBLE PRECISION NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_letters_due ON letters(status, deliver_at);
            """
        )
        # 杂项标记：连续陪伴天数、每日彩蛋是否已发等
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS meta (
                session TEXT NOT NULL,
                key     TEXT NOT NULL,
                value   TEXT NOT NULL,
                PRIMARY KEY (session, key)
            );
            """
        )
        # 她的日记：每晚睡前写一篇
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS diary (
                id         BIGSERIAL PRIMARY KEY,
                session    TEXT NOT NULL,
                day        TEXT NOT NULL,
                body       TEXT NOT NULL,
                created_at DOUBLE PRECISION NOT NULL,
                UNIQUE (session, day)
            );
            """
        )
        # 昨日回顾：小学徒夜里整理，小暖早上检查后转述
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS digests (
                id         BIGSERIAL PRIMARY KEY,
                session    TEXT NOT NULL,
                day        TEXT NOT NULL,
                body       TEXT NOT NULL,
                created_at DOUBLE PRECISION NOT NULL,
                UNIQUE (session, day)
            );
            """
        )
        # 身世档案：用户亲口告诉她的关于她自己的既定事实，全文注入、绝不编造
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS lore (
                id         BIGSERIAL PRIMARY KEY,
                session    TEXT NOT NULL,
                topic      TEXT NOT NULL,
                body       TEXT NOT NULL,
                updated_at DOUBLE PRECISION NOT NULL,
                UNIQUE (session, topic)
            );
            """
        )


async def close() -> None:
    if _pool is not None:
        await _pool.close()


async def get_history(session: str, limit: int = 40) -> list[dict]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT role, content FROM history WHERE session = $1 "
            "ORDER BY id DESC LIMIT $2",
            session, limit,
        )
    return [{"role": r["role"], "content": r["content"]} for r in reversed(rows)]


async def add_message(session: str, role: str, content: str) -> None:
    async with _pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO history (session, role, content, ts) "
            "VALUES ($1, $2, $3, $4)",
            session, role, content, time.time(),
        )


async def get_memories(session: str) -> list[dict]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, text FROM memories WHERE session = $1 ORDER BY id",
            session,
        )
    return [{"id": r["id"], "text": r["text"]} for r in rows]


async def add_memories(
    session: str, facts: list[str], embeddings: list[list[float]] | None = None
) -> None:
    rows = [
        (session, f, time.time(),
         embeddings[i] if embeddings else None)
        for i, f in enumerate(facts)
    ]
    async with _pool.acquire() as conn:
        await conn.executemany(
            "INSERT INTO memories (session, text, ts, embedding) "
            "VALUES ($1, $2, $3, $4)",
            rows,
        )


def cosine(a: list[float], b: list[float]) -> float:
    """余弦相似度（两个向量维度相同、已归一化时即点积）"""
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = sum(x * x for x in a) ** 0.5
    nb = sum(y * y for y in b) ** 0.5
    return dot / (na * nb) if na and nb else 0.0


async def search_memories(
    session: str, query_vec: list[float], k: int = 6
) -> list[dict]:
    """语义检索：取该会话记忆中与 query_vec 余弦相似度最高的 k 条"""
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, text, embedding FROM memories "
            "WHERE session = $1 AND embedding IS NOT NULL",
            session,
        )
    scored = [
        {"id": r["id"], "text": r["text"], "score": cosine(query_vec, r["embedding"])}
        for r in rows
    ]
    scored.sort(key=lambda m: m["score"], reverse=True)
    return scored[:k]


# ---- 定时事件（叫早 / 提醒）----

async def add_event(session: str, kind: str, fire_at: float, text: str,
                    repeat: str = "once", minute_of_day: int | None = None) -> int:
    async with _pool.acquire() as conn:
        return await conn.fetchval(
            "INSERT INTO events (session, kind, fire_at, text, repeat, "
            "minute_of_day, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7) "
            "RETURNING id",
            session, kind, fire_at, text, repeat, minute_of_day, time.time(),
        )


async def due_events(session: str | None = None) -> list[dict]:
    """到点未送达的一次性事件；不给 session 则查所有会话（供调度器用）"""
    sql = ("SELECT id, session, kind, fire_at, text, repeat FROM events "
           "WHERE status = 'pending' AND repeat = 'once' AND fire_at <= $1")
    args: list = [time.time()]
    if session is not None:
        sql += " AND session = $2"
        args.append(session)
    sql += " ORDER BY fire_at"
    async with _pool.acquire() as conn:
        rows = await conn.fetch(sql, *args)
    return [dict(r) for r in rows]


async def due_daily_events(today: str, minute_now: int,
                           window: int = 10) -> list[dict]:
    """今天到点、还没发过的每日事件（window 调大即"今天错过的都算"）"""
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, session, kind, text, repeat, minute_of_day FROM events "
            "WHERE status = 'pending' AND repeat = 'daily' "
            "AND minute_of_day <= $2 AND minute_of_day > $2 - $3 "
            "AND (last_fired IS NULL OR last_fired <> $1)",
            today, minute_now, window,
        )
    return [dict(r) for r in rows]


async def today_events(session: str, day_start: float) -> list[dict]:
    """时间表用：该会话的全部每日任务 + 今天及以后的一次性事件"""
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, kind, fire_at, text, repeat, minute_of_day, status "
            "FROM events WHERE session = $1 AND status = 'pending' "
            "AND (repeat = 'daily' OR fire_at >= $2) ORDER BY fire_at",
            session, day_start,
        )
    return [dict(r) for r in rows]


async def mark_event_done(event_id: int) -> None:
    async with _pool.acquire() as conn:
        await conn.execute(
            "UPDATE events SET status = 'done' WHERE id = $1", event_id)


async def mark_event_fired(event_id: int, today: str) -> None:
    """每日事件：记录今天已发过，明天还会发"""
    async with _pool.acquire() as conn:
        await conn.execute(
            "UPDATE events SET last_fired = $2 WHERE id = $1",
            event_id, today)


# ---- 作息规律（猜出来的上下学时间）----

async def upsert_routine(session: str, label: str,
                         minute: int, kind: str) -> None:
    """新证据合并进作息：explicit 直接定时间；inferred 做累计平均，越猜越准"""
    async with _pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO routines (session, label, minute, kind, evidence, updated_at)
            VALUES ($1, $2, $3, $4, 1, $5)
            ON CONFLICT (session, label) DO UPDATE SET
                minute = CASE WHEN EXCLUDED.kind = 'explicit'
                              THEN EXCLUDED.minute
                              ELSE (routines.minute * routines.evidence
                                    + EXCLUDED.minute)
                                   / (routines.evidence + 1) END,
                evidence = LEAST(routines.evidence + 1, 1000),
                kind = CASE WHEN EXCLUDED.kind = 'explicit'
                            THEN 'explicit' ELSE routines.kind END,
                updated_at = EXCLUDED.updated_at
            """,
            session, label, minute, kind, time.time(),
        )


async def get_routines(session: str) -> list[dict]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT label, minute, evidence, kind FROM routines "
            "WHERE session = $1 ORDER BY minute",
            session,
        )
    return [dict(r) for r in rows]


async def due_routines(today: str, minute_now: int, window: int = 10) -> list[dict]:
    """刚过点 10 分钟内、且今天还没问候过的作息（全会话，供调度器用）"""
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, session, label, minute, evidence, kind FROM routines "
            "WHERE minute <= $2 AND minute > $2 - $3 "
            "AND (last_fired IS NULL OR last_fired <> $1)",
            today, minute_now, window,
        )
    return [dict(r) for r in rows]


async def mark_routine_fired(routine_id: int, today: str) -> None:
    async with _pool.acquire() as conn:
        await conn.execute(
            "UPDATE routines SET last_fired = $2 WHERE id = $1",
            routine_id, today)


# ---- 惦记的事（情绪回访 / 承诺追踪）----

async def add_care(session: str, kind: str, note: str, due_at: float) -> int:
    async with _pool.acquire() as conn:
        return await conn.fetchval(
            "INSERT INTO care (session, kind, note, due_at, created_at) "
            "VALUES ($1, $2, $3, $4, $5) RETURNING id",
            session, kind, note, due_at, time.time(),
        )


async def due_care(now_ts: float) -> list[dict]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, session, kind, note FROM care "
            "WHERE status = 'pending' AND due_at <= $1 ORDER BY due_at",
            now_ts,
        )
    return [dict(r) for r in rows]


async def pending_care(session: str) -> list[dict]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, kind, note, due_at FROM care "
            "WHERE session = $1 AND status = 'pending' ORDER BY due_at",
            session,
        )
    return [dict(r) for r in rows]


async def mark_care_done(care_id: int) -> None:
    async with _pool.acquire() as conn:
        await conn.execute(
            "UPDATE care SET status = 'done' WHERE id = $1", care_id)


# ---- 慢信 ----

async def add_letter(session: str, body: str, deliver_at: float) -> int:
    async with _pool.acquire() as conn:
        return await conn.fetchval(
            "INSERT INTO letters (session, body, deliver_at, created_at) "
            "VALUES ($1, $2, $3, $4) RETURNING id",
            session, body, deliver_at, time.time(),
        )


async def due_letters() -> list[dict]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, session, body, created_at FROM letters "
            "WHERE status = 'sealed' AND deliver_at <= $1 ORDER BY deliver_at",
            time.time(),
        )
    return [dict(r) for r in rows]


async def pending_letters(session: str) -> list[dict]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, body, deliver_at FROM letters "
            "WHERE session = $1 AND status = 'sealed' ORDER BY deliver_at",
            session,
        )
    return [dict(r) for r in rows]


async def mark_letter_delivered(letter_id: int) -> None:
    async with _pool.acquire() as conn:
        await conn.execute(
            "UPDATE letters SET status = 'delivered' WHERE id = $1", letter_id)


# ---- meta 标记（连续天数 / 每日彩蛋）----

async def get_meta(session: str, key: str, default: str = "") -> str:
    async with _pool.acquire() as conn:
        v = await conn.fetchval(
            "SELECT value FROM meta WHERE session = $1 AND key = $2",
            session, key)
    return v if v is not None else default


async def set_meta(session: str, key: str, value: str) -> None:
    async with _pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO meta (session, key, value) VALUES ($1, $2, $3) "
            "ON CONFLICT (session, key) DO UPDATE SET value = EXCLUDED.value",
            session, key, value)


# ---- 回忆杀素材 / 记忆巩固 ----

async def memory_from_days_ago(session: str, days: int,
                               now_ts: float) -> list[dict]:
    """约 days 天前（前后半天内）的记忆和用户说过的话"""
    lo = now_ts - days * 86400
    hi = lo + 86400
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "(SELECT text AS content, ts FROM memories "
            " WHERE session = $1 AND ts BETWEEN $2 AND $3) "
            "UNION ALL "
            "(SELECT content, ts FROM history "
            " WHERE session = $1 AND role = 'user' AND ts BETWEEN $2 AND $3) "
            "ORDER BY ts LIMIT 20",
            session, lo, hi,
        )
    return [dict(r) for r in rows]


async def active_sessions(days: int = 3) -> list[str]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT DISTINCT session FROM history WHERE ts > $1",
            time.time() - days * 86400,
        )
    return [r["session"] for r in rows]


async def replace_memories(session: str, facts: list[str],
                           embeddings: list[list[float]]) -> None:
    """记忆巩固用：整组替换某会话的长期记忆"""
    async with _pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "DELETE FROM memories WHERE session = $1", session)
            await conn.executemany(
                "INSERT INTO memories (session, text, ts, embedding) "
                "VALUES ($1, $2, $3, $4)",
                [(session, f, time.time(), embeddings[i])
                 for i, f in enumerate(facts)],
            )


# ---- 她的日记与身体（容器感知）----

async def add_diary(session: str, day: str, body: str) -> None:
    async with _pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO diary (session, day, body, created_at) "
            "VALUES ($1, $2, $3, $4) ON CONFLICT (session, day) DO NOTHING",
            session, day, body, time.time(),
        )


async def get_diary(session: str, limit: int = 5) -> list[dict]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT day, body FROM diary WHERE session = $1 "
            "ORDER BY day DESC LIMIT $2",
            session, limit,
        )
    return [dict(r) for r in rows]


async def day_counts(session: str, day_start_ts: float) -> dict:
    """今天聊了多少句、新记住了多少件事"""
    async with _pool.acquire() as conn:
        msgs = await conn.fetchval(
            "SELECT count(*) FROM history WHERE session = $1 AND ts >= $2",
            session, day_start_ts)
        mem = await conn.fetchval(
            "SELECT count(*) FROM memories WHERE session = $1 AND ts >= $2",
            session, day_start_ts)
    return {"msgs": msgs, "mem": mem}


async def get_stats(session: str) -> dict:
    """她眼中的自己：容器生命体征"""
    async with _pool.acquire() as conn:
        mem = await conn.fetchval(
            "SELECT count(*) FROM memories WHERE session = $1", session)
        msgs = await conn.fetchval(
            "SELECT count(*) FROM history WHERE session = $1", session)
        first = await conn.fetchval(
            "SELECT min(ts) FROM history WHERE session = $1", session)
        db_bytes = await conn.fetchval(
            "SELECT pg_database_size(current_database())")
    return {"memories": mem, "messages": msgs,
            "first_ts": first, "db_bytes": db_bytes}


# ---- 昨日回顾（小学徒的夜间琐事）----

async def add_digest(session: str, day: str, body: str) -> None:
    async with _pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO digests (session, day, body, created_at) "
            "VALUES ($1, $2, $3, $4) ON CONFLICT (session, day) DO NOTHING",
            session, day, body, time.time(),
        )


async def get_digest(session: str, day: str) -> str | None:
    async with _pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT body FROM digests WHERE session = $1 AND day = $2",
            session, day)


async def count_digests(session: str) -> int:
    async with _pool.acquire() as conn:
        return await conn.fetchval(
            "SELECT count(*) FROM digests WHERE session = $1", session)


# ---- 身世档案 ----

async def upsert_lore(session: str, topic: str, body: str) -> None:
    """同话题=更正（覆盖旧说法），不是删除"""
    async with _pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO lore (session, topic, body, updated_at) "
            "VALUES ($1, $2, $3, $4) "
            "ON CONFLICT (session, topic) "
            "DO UPDATE SET body = EXCLUDED.body, updated_at = EXCLUDED.updated_at",
            session, topic, body, time.time(),
        )


async def get_lore(session: str) -> list[dict]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT topic, body FROM lore WHERE session = $1 ORDER BY id",
            session)
    return [dict(r) for r in rows]
