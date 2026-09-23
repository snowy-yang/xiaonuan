"""小模型路由的离线测试（mock 模型服务，不发真实请求）"""
import asyncio
import json
from datetime import datetime

import httpx

from app import connector

NOW = datetime(2026, 9, 21, 19, 30)


def mock_model(content: str):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"choices": [{"message": {
            "role": "assistant", "content": content}}]})
    return handler


def enable_router(monkeypatch, content=None, status=200):
    monkeypatch.setattr(connector, "SMALL_KEY", "k")
    monkeypatch.setattr(connector, "SMALL_MODEL", "m")
    if status == 200:
        transport = httpx.MockTransport(mock_model(content or ""))
    else:
        transport = httpx.MockTransport(lambda r: httpx.Response(status))
    monkeypatch.setattr(connector, "_TEST_TRANSPORT", transport)


def test_route_full_parse(monkeypatch):
    raw = json.dumps({
        "view": "memories",
        "letter": {"days": 7, "body": "给未来的我：加油"},
        "negative_mood": True,
        "city_hint": "杭州",
        "facts": ["用户叫小明", "用户叫小明", "用户喜欢打篮球"],
        "events": [{"kind": "alarm", "fire_at": "2026-09-22T07:00:00",
                    "text": "起床", "repeat": "daily"},
                   {"kind": "reminder", "fire_at": "2020-01-01T00:00:00",
                    "text": "过去的应被丢弃"}],
        "routines": [{"label": "到校", "time": "07:40", "kind": "inferred"},
                     {"label": "打游戏", "time": "22:00", "kind": "explicit"}],
        "promises": [{"note": "面试", "due_at": None}],
        "lore": [{"topic": "诞生", "body": "小暖是用户亲手造的"}],
    })
    enable_router(monkeypatch, "好的 " + raw + " (忽略我)")

    sig = asyncio.run(connector.route(
        "我叫小明，我在杭州，明早7点叫我起床，明天面试", NOW, ["用户叫小明"]))

    assert sig["view"] == "memories"
    assert sig["letter"] == (7, "给未来的我：加油")
    assert sig["negative_mood"] is True
    assert sig["city_hint"] == "杭州"
    assert sig["facts"] == ["用户喜欢打篮球"]  # 去重 + 已有的不要
    assert len(sig["events"]) == 1
    assert sig["events"][0]["kind"] == "alarm"
    assert sig["events"][0]["repeat"] == "daily"
    assert sig["events"][0]["fire_at"].day == 22
    assert sig["routines"] == [{"label": "到校", "minute": 460,
                                "kind": "inferred"}]
    assert sig["promises"][0]["note"] == "面试"
    assert (sig["promises"][0]["due_at"].day,
            sig["promises"][0]["due_at"].hour) == (22, 18)  # null -> 次日18点
    assert sig["lore"] == [{"topic": "诞生", "body": "小暖是用户亲手造的"}]


def test_route_garbage_returns_empty(monkeypatch):
    enable_router(monkeypatch, "这不是 JSON")
    sig = asyncio.run(connector.route("随便", NOW, []))
    assert sig["view"] is None and sig["facts"] == [] and sig["letter"] is None


def test_route_server_error_returns_empty(monkeypatch):
    enable_router(monkeypatch, status=500)
    sig = asyncio.run(connector.route("随便", NOW, []))
    assert sig["view"] is None


def test_route_without_model_is_noop(monkeypatch):
    monkeypatch.setattr(connector, "SMALL_KEY", "")
    monkeypatch.setattr(connector, "SMALL_MODEL", "")
    sig = asyncio.run(connector.route("明早7点叫我起床", NOW, []))
    assert sig["view"] is None and sig["events"] == []
