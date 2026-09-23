"""投递与工具执行测试（FakeDB + stub 工具）"""
import json
import time
from datetime import datetime, timedelta

from app import main


class _State:
    name = "CONNECTED"


class FakeWS:
    client_state = _State()

    def __init__(self):
        self.sent = []

    async def send_text(self, payload):
        self.sent.append(payload)

    def last_text(self):
        """最后一条主动消息的纯文本（服务端 JSON 转义了中文，要解一下）"""
        return json.loads(self.sent[-1])["text"]


async def test_one_shot_event_offline_stays_pending(fdb, offline):
    ev = {"id": 1, "session": "s1", "kind": "alarm",
          "fire_at": time.time(), "text": "起床", "repeat": "once"}
    ok = await main.deliver_event(ev)
    assert ok is False
    assert fdb.events == []  # 没投递就不写历史、不标完成


async def test_one_shot_event_online_delivers(fdb, offline):
    ws = FakeWS()
    main.connected.setdefault("s1", set()).add(ws)
    try:
        ev = {"id": 2, "session": "s1", "kind": "reminder",
              "fire_at": time.time(), "text": "喝水", "repeat": "once"}
        ok = await main.deliver_event(ev)
        assert ok is True
        assert "喝水" in ws.last_text()
        # 主动消息写进了历史
        assert [h["role"] for h in fdb.history] == ["assistant"]
    finally:
        main.connected.get("s1", set()).discard(ws)


async def test_daily_event_marks_fired_not_done(fdb, offline):
    ws = FakeWS()
    main.connected.setdefault("s1", set()).add(ws)
    try:
        await fdb.add_event("s1", "reminder", time.time() + 3600, "吃药",
                            "daily", 22 * 60)
        ev = fdb.events[0]
        ok = await main.deliver_daily_event(ev)
        assert ok is True
        assert "吃药" in ws.last_text()
        assert ev["last_fired"] == datetime.now().strftime("%Y-%m-%d")
        assert ev["status"] == "pending"  # 每天还要再响
    finally:
        main.connected.get("s1", set()).discard(ws)


async def test_care_delivery(fdb, offline):
    cid = await fdb.add_care("s1", "emotion", "昨天心情不好",
                             time.time() - 10)
    ok = await main.deliver_care({"id": cid, "session": "s1",
                                  "kind": "emotion", "note": "昨天心情不好"})
    assert ok is False  # 没人在线，保持 pending

    ws = FakeWS()
    main.connected.setdefault("s1", set()).add(ws)
    try:
        ok = await main.deliver_care({"id": cid, "session": "s1",
                                      "kind": "emotion", "note": "昨天心情不好"})
        assert ok is True
        assert "心情" in ws.last_text()
        assert fdb.care[0]["status"] == "done"
    finally:
        main.connected.get("s1", set()).discard(ws)


async def test_routine_echo_greeting(fdb, offline):
    r = {"session": "s1", "label": "到校", "minute": 7 * 60 + 40,
         "kind": "inferred", "evidence": 3}
    text = await main.routine_message(r)
    assert "到学校" in text


async def test_weather_tool_city_chain(fdb, offline, monkeypatch):
    calls = []

    async def fake_weather(city):
        calls.append(city)
        return f"{city} 晴 25°C"

    monkeypatch.setattr(main.tools, "get_weather", fake_weather)

    # 三层都推不出 -> UNKNOWN_CITY，且不假装有天气
    out = await main.execute_tool("s1", "get_weather", {})
    assert out.startswith("UNKNOWN_CITY") and calls == []

    # 路由器的城市提示 -> 查询并记住
    out = await main.execute_tool("s1", "get_weather", {}, city_hint="杭州")
    assert "杭州" in out and calls == ["杭州"]
    assert await fdb.get_meta("s1", "city") == "杭州"

    # 模型显式给了城市 -> 优先用，并覆盖记住的城市
    out = await main.execute_tool("s1", "get_weather", {"city": "北京"})
    assert calls[-1] == "北京"
    assert await fdb.get_meta("s1", "city") == "北京"

    # 之后不带城市 -> 用最近一次记住的
    out = await main.execute_tool("s1", "get_weather", {})
    assert calls[-1] == "北京"


async def test_tool_executor_other_tools(fdb, offline, monkeypatch):
    async def fake_news(q):
        return f"新闻:{q}"

    async def fake_search(q):
        return f"搜索:{q}"

    monkeypatch.setattr(main.tools, "get_news", fake_news)
    monkeypatch.setattr(main.tools, "web_search", fake_search)
    assert await main.execute_tool("s1", "get_news", {}) == "新闻:今日热点"
    assert await main.execute_tool("s1", "web_search",
                                   {"query": "量子"}) == "搜索:量子"
    assert await main.execute_tool("s1", "web_search", {}) == "缺少搜索词。"
    assert "未知工具" in await main.execute_tool("s1", "nope", {})


async def test_system_prompt_includes_lore_and_relevant(fdb, offline):
    await fdb.upsert_lore("s1", "诞生", "小暖是用户亲手造的")
    prompt = await main.system_prompt(
        "s1", [{"text": "用户喜欢打篮球"}])
    assert "既定事实" in prompt
    assert "小暖是用户亲手造的" in prompt
    assert "用户喜欢打篮球" in prompt


async def test_compose_views(fdb, offline):
    # 记忆
    await fdb.add_memories("s1", ["用户叫小明"], None)
    text = await main.compose_view("s1", "memories")
    assert "用户叫小明" in text
    # 空记忆的说法
    text = await main.compose_view("s2", "memories")
    assert "还不太认识你" in text
    # 身世
    text = await main.compose_view("s2", "lore")
    assert "一个字都不会忘" in text
    # 时间表空表
    text = await main.compose_view("s2", "timetable")
    assert "时间表还空着" in text
