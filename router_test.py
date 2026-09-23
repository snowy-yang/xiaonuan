"""小模型路由的离线测试（mock 模型服务）：uv run python router_test.py"""
import asyncio
import json
from datetime import datetime

import httpx

from app import connector

NOW = datetime(2026, 9, 21, 19, 30)

RAW = json.dumps({
    "view": "memories",
    "letter": {"days": 7, "body": "给未来的我：加油"},
    "negative_mood": True,
    "city_hint": "杭州",
    "facts": ["用户叫小明", "用户叫小明", "用户喜欢打篮球"],
    "events": [{"kind": "alarm", "fire_at": "2026-09-22T07:00:00",
                "text": "起床", "repeat": "daily"},
               {"kind": "reminder", "fire_at": "2020-01-01T00:00:00",
                "text": "过去的应被丢掉"}],
    "routines": [{"label": "到校", "time": "07:40", "kind": "inferred"},
                 {"label": "打游戏", "time": "22:00", "kind": "explicit"}],
    "promises": [{"note": "面试", "due_at": None}],
    "lore": [{"topic": "诞生", "body": "小暖是用户亲手造的"}],
    "extra_field": "无关字段应被忽略"
})


def handler(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, json={"choices": [{"message": {
        "role": "assistant",
        "content": "好的：" + RAW + " （解析我）"}}]})


async def main():
    # 测试环境没有 .env，手动给路由器配上"模型"（走 mock）
    connector.SMALL_KEY = "test-key"
    connector.SMALL_MODEL = "test-model"
    connector._TEST_TRANSPORT = httpx.MockTransport(handler)
    sig = await connector.route(
        "我叫小明，我在杭州，明早7点叫我起床，明天有面试", NOW, ["用户叫小明"])

    assert sig["view"] == "memories", sig
    assert sig["letter"] == (7, "给未来的我：加油"), sig["letter"]
    assert sig["negative_mood"] is True
    assert sig["city_hint"] == "杭州"
    # facts 去重（已有记忆里的不要）
    assert sig["facts"] == ["用户喜欢打篮球"], sig["facts"]
    # events：过去的被丢弃，daily 保留
    assert len(sig["events"]) == 1
    assert sig["events"][0]["kind"] == "alarm"
    assert sig["events"][0]["repeat"] == "daily"
    assert sig["events"][0]["fire_at"].day == 22
    # routines：非法标签被丢掉
    assert sig["routines"] == [{"label": "到校", "minute": 460,
                                "kind": "inferred"}], sig["routines"]
    # promises：due_at=null 回落为次日 18:00
    assert sig["promises"][0]["note"] == "面试"
    assert (sig["promises"][0]["due_at"].day,
            sig["promises"][0]["due_at"].hour) == (22, 18)
    assert sig["lore"] == [{"topic": "诞生", "body": "小暖是用户亲手造的"}]

    # 模型挂掉/输出乱码 → 空判定，系统照常运转
    connector._TEST_TRANSPORT = httpx.MockTransport(
        lambda r: httpx.Response(500))
    sig2 = await connector.route("随便", NOW, [])
    assert sig2["view"] is None and sig2["facts"] == [] and \
        sig2["letter"] is None

    print("路由器测试通过")


asyncio.run(main())
