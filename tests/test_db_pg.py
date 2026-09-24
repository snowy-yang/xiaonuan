"""PostgreSQL 集成测试：真实跑一遍 db.py 的每一层。

未设置 PG_DSN 时整组跳过；设置后需要库已存在（表会自动建）。
"""
import os
import time
import uuid

import pytest

import app.db as db
from app.connector import hash_embed

pytestmark = pytest.mark.skipif(
    not os.getenv("PG_DSN", "").strip(),
    reason="未配置 PG_DSN，跳过 PG 集成测试")


@pytest.fixture
async def conn():
    """每个测试独立的连接池：pytest-asyncio 默认每个测试一个新事件循环，
    连接池必须建在当前循环里，否则 asyncpg 会报 attached to a different loop"""
    await db.init(os.environ["PG_DSN"].strip())
    yield
    await db.close()


def session():
    return "test-" + uuid.uuid4().hex[:8]


async def test_history_roundtrip(conn):
    s = session()
    await db.add_message(s, "user", "第一条")
    await db.add_message(s, "assistant", "第二条")
    await db.add_message(s, "user", "第三条")
    rows = await db.get_history(s)
    assert [(r["role"], r["content"]) for r in rows] == [
        ("user", "第一条"), ("assistant", "第二条"), ("user", "第三条")]
    rows = await db.get_history(s, limit=2)
    assert len(rows) == 2 and rows[0]["content"] == "第二条"


async def test_memories_and_semantic_search(conn):
    s = session()
    facts = ["用户喜欢打篮球", "用户住在杭州", "用户叫小明"]
    vecs = [hash_embed(f) for f in facts]
    await db.add_memories(s, facts, vecs)
    rows = await db.get_memories(s)
    assert [r["text"] for r in rows] == facts
    hits = await db.search_memories(s, hash_embed("打篮球真开心"), k=2)
    assert hits and hits[0]["text"] == "用户喜欢打篮球"
    # 整组替换（夜间巩固用）
    new_facts = ["用户叫小明", "用户在杭州上学"]
    await db.replace_memories(s, new_facts, [hash_embed(f) for f in new_facts])
    rows = await db.get_memories(s)
    assert [r["text"] for r in rows] == new_facts


async def test_routine_upsert_averaging(conn):
    s = session()
    await db.upsert_routine(s, "到校", 7 * 60 + 40, "inferred")
    await db.upsert_routine(s, "到校", 7 * 60 + 50, "inferred")
    rows = await db.get_routines(s)
    assert len(rows) == 1
    assert rows[0]["minute"] == (7 * 60 + 40 + 7 * 60 + 50) // 2
    assert rows[0]["evidence"] == 2
    # explicit 直接定时间
    await db.upsert_routine(s, "到校", 7 * 60 + 30, "explicit")
    rows = await db.get_routines(s)
    assert rows[0]["minute"] == 7 * 60 + 30 and rows[0]["kind"] == "explicit"


async def test_events_one_shot_and_daily(conn):
    s = session()
    now = time.time()
    eid = await db.add_event(s, "reminder", now - 10, "喝水", "once", 0)
    did = await db.add_event(s, "reminder", now + 3600, "吃药", "daily", 22 * 60)

    due = await db.due_events(s)
    assert [e["id"] for e in due] == [eid]
    today = time.strftime("%Y-%m-%d")
    daily = await db.due_daily_events(today, 22 * 60 + 1, window=10)
    assert [e["id"] for e in daily] == [did]
    await db.mark_event_fired(did, today)
    assert await db.due_daily_events(today, 22 * 60 + 1, window=10) == []

    rows = await db.today_events(s, now - 60)
    assert {e["id"] for e in rows} == {eid, did}
    await db.mark_event_done(eid)
    assert await db.due_events(s) == []


async def test_care_and_letters(conn):
    s = session()
    cid = await db.add_care(s, "emotion", "心情不好", time.time() - 5)
    due = await db.due_care(time.time())
    assert [c["id"] for c in due] == [cid]
    await db.mark_care_done(cid)
    assert await db.due_care(time.time()) == []

    lid = await db.add_letter(s, "给未来", time.time() - 5)
    assert [l["id"] for l in await db.due_letters()] == [lid]
    await db.mark_letter_delivered(lid)
    assert await db.pending_letters(s) == []


async def test_meta_lore_diary_digest(conn):
    s = session()
    await db.set_meta(s, "streak", "3")
    assert await db.get_meta(s, "streak") == "3"
    assert await db.get_meta(s, "nope", "d") == "d"

    await db.upsert_lore(s, "诞生", "v1")
    await db.upsert_lore(s, "诞生", "v2")  # 同话题=更正
    lore = await db.get_lore(s)
    assert lore == [{"topic": "诞生", "body": "v2"}]

    day = time.strftime("%Y-%m-%d")
    await db.add_diary(s, day, "今天…")
    assert (await db.get_diary(s))[0]["body"] == "今天…"

    await db.add_digest(s, day, "回顾")
    assert await db.get_digest(s, day) == "回顾"
    assert await db.count_digests(s) == 1


async def test_stats_and_day_counts(conn):
    s = session()
    await db.add_message(s, "user", "计数")
    await db.add_memories(s, ["用户是测试"], None)
    stats = await db.get_stats(s)
    assert stats["messages"] == 1 and stats["memories"] == 1
    assert stats["first_ts"] and stats["db_bytes"] > 0
    counts = await db.day_counts(s, time.time() - 60)
    assert counts["msgs"] == 1 and counts["mem"] == 1


async def test_memory_from_days_ago(conn):
    s = session()
    await db.add_message(s, "user", "半个月前的话")
    rows = await db.memory_from_days_ago(s, 15, time.time())
    assert rows == []  # 刚插入的不在 15 天前
    # days=1 的窗口是 [now-86400, now]，正好罩住刚插入的消息
    rows = await db.memory_from_days_ago(s, 1, time.time())
    assert any(r["content"] == "半个月前的话" for r in rows)
