"""WebSocket 全链路流测试：路由信号 → 回复 → 落库（FakeDB，无需 PG）"""
import asyncio
import time
from datetime import datetime, timedelta

from app import main


def test_echo_reply_and_history(client, route_sig, run_chat, reply_text):
    route_sig({})  # 路由器：什么信号都没有
    received = run_chat([{"type": "chat", "text": "你好呀"}])
    text = reply_text(received)
    assert "回声模式" in text and "你好呀" in text
    assert received[-1]["type"] == "done"


def test_route_signals_land_in_db(client, route_sig, fdb, run_chat):
    future = datetime.now() + timedelta(hours=1)
    route_sig({
        "facts": ["用户叫小明"],
        "events": [{"kind": "alarm", "fire_at": future,
                    "text": "起床", "repeat": "once"}],
        "negative_mood": True,
        "promises": [{"note": "面试",
                      "due_at": datetime.now() + timedelta(days=1)}],
        "lore": [{"topic": "诞生", "body": "小暖是用户亲手造的"}],
    })
    run_chat([{"type": "chat", "text": "我叫小明，明早叫我起床，"
                                        "昨天心情不好，明天面试，你是我造的"}])
    # remember_later 是后台任务，等它跑完
    time.sleep(0.5)
    assert [m["text"] for m in fdb.memories] == ["用户叫小明"]
    assert len(fdb.events) == 1 and fdb.events[0]["text"] == "起床"
    assert len(fdb.care) == 2  # 情绪回访 + 承诺追踪
    kinds = {c["kind"] for c in fdb.care}
    assert kinds == {"emotion", "promise"}
    assert fdb.lore[("s1", "诞生")] == "小暖是用户亲手造的"
    # 对话历史：user + assistant
    assert len(fdb.history) == 2


def test_view_reply_reads_db(client, route_sig, fdb, run_chat, reply_text):
    asyncio.run(fdb.add_memories("s1", ["用户叫小明", "用户喜欢打篮球"], None))
    route_sig({"view": "memories"})
    received = run_chat([{"type": "chat", "text": "你的记忆里有什么"}])
    text = reply_text(received)
    assert "用户叫小明" in text and "用户喜欢打篮球" in text
    # 档案回复也进历史（assistant 角色）
    assert any(h["role"] == "assistant" and "小明" in h["content"]
               for h in fdb.history)


def test_letter_flow(client, route_sig, fdb, run_chat, reply_text):
    route_sig({"letter": (7, "给未来的我：加油")})
    received = run_chat([{"type": "chat", "text": "给7天后的自己写信：加油"}])
    assert "✉️" in reply_text(received)
    assert len(fdb.letters) == 1 and fdb.letters[0]["body"] == "给未来的我：加油"


def test_letter_ask_when_incomplete(client, route_sig, fdb, run_chat,
                                    reply_text):
    route_sig({"letter": ("ask", "")})
    received = run_chat([{"type": "chat", "text": "我想写慢信"}])
    assert "给7天后的自己写信" in reply_text(received)
    assert fdb.letters == []


def test_streak_increments(fdb, offline):
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    asyncio.run(fdb.set_meta("s1", "last_active", yesterday))
    asyncio.run(fdb.set_meta("s1", "streak", "3"))
    sig = {"facts": [], "events": [], "routines": [], "promises": [],
           "lore": [], "letter": None, "view": None, "negative_mood": False}

    class FakeWS:
        async def send_json(self, payload):
            pass

    asyncio.run(main.remember_later(FakeWS(), "s1", "我来了", sig))
    assert asyncio.run(fdb.get_meta("s1", "streak")) == "4"


def test_streak_resets_after_gap(fdb, offline):
    asyncio.run(fdb.set_meta("s1", "last_active", "2020-01-01"))
    sig = {"facts": [], "events": [], "routines": [], "promises": [],
           "lore": [], "letter": None, "view": None, "negative_mood": False}

    class FakeWS:
        async def send_json(self, payload):
            pass

    asyncio.run(main.remember_later(FakeWS(), "s1", "好久不见", sig))
    assert asyncio.run(fdb.get_meta("s1", "streak")) == "1"
