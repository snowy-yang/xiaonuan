"""OneBot 11 适配器测试（TestClient 模拟 OneBot 实现的反向 WS）"""
import json
import time

import pytest
from starlette.websockets import WebSocketDisconnect

from app import onebot


@pytest.fixture(autouse=True)
def clean_conn():
    onebot.connections.clear()
    yield
    onebot.connections.clear()


def _msg_event(user_id=123, message="你好呀", message_type="private",
               group_id=None, self_id=10000):
    ev = {"time": 0, "self_id": self_id, "post_type": "message",
          "message_type": message_type,
          "sub_type": "friend" if message_type == "private" else "normal",
          "message_id": 1, "user_id": user_id, "message": message,
          "raw_message": str(message),
          "sender": {"user_id": user_id, "nickname": "tester"}}
    if group_id:
        ev["group_id"] = group_id
    return ev


def _drain_action(ws):
    """收一个 action 并替 OneBot 实现回一个成功 echo，返回该 action"""
    action = json.loads(ws.receive_text())
    ws.send_text(json.dumps({"status": "ok", "retcode": 0, "data": {},
                             "echo": action["echo"]}))
    return action


def test_private_reply(client, route_sig, fdb):
    route_sig({})
    with client.websocket_connect("/onebot/v11/ws") as ws:
        ws.send_text(json.dumps(_msg_event(message="你好呀")))
        action = _drain_action(ws)
        assert action["action"] == "send_private_msg"
        assert action["params"]["user_id"] == 123
        assert "回声模式" in action["params"]["message"]
        assert "你好呀" in action["params"]["message"]
        # 会话映射注册成功
        assert "ob-10000-p123" in onebot.connections[10000].targets
        time.sleep(0.3)
        # 对话进了历史（user + assistant）
        assert len(fdb.history) == 2


def test_cq_code_stripped(client, route_sig, fdb):
    route_sig({})
    with client.websocket_connect("/onebot/v11/ws") as ws:
        ws.send_text(json.dumps(
            _msg_event(message="[CQ:face,id=1]你叫什么名字")))
        action = _drain_action(ws)
        assert "你叫什么名字" in action["params"]["message"]


def test_group_requires_at(client, route_sig, fdb):
    route_sig({})
    with client.websocket_connect("/onebot/v11/ws") as ws:
        # 群里没 @ 她 -> 不回应
        ws.send_text(json.dumps(_msg_event(
            message_type="group", group_id=777, message="大家好啊")))
        # 紧跟一条私聊 -> 第一条收到的 action 应该是私聊回复
        ws.send_text(json.dumps(_msg_event(message="私聊一下")))
        action = _drain_action(ws)
        assert action["action"] == "send_private_msg"
        assert fdb.history[0]["session"] == "ob-10000-p123"


def test_group_at_replies_with_at(client, route_sig, fdb):
    route_sig({})
    with client.websocket_connect("/onebot/v11/ws") as ws:
        ws.send_text(json.dumps(_msg_event(
            message_type="group", group_id=777,
            message=[{"type": "at", "data": {"qq": 10000}},
                     {"type": "text", "data": {"text": " 在吗"}}])))
        action = _drain_action(ws)
        assert action["action"] == "send_group_msg"
        assert action["params"]["group_id"] == 777
        assert action["params"]["message"].startswith("[CQ:at,qq=123]")
        assert "回声模式" in action["params"]["message"]
        # 群会话独立于私聊
        sessions = {h["session"] for h in fdb.history}
        assert "ob-10000-g777" in sessions


def test_notify_event_becomes_qq_message(client, route_sig, fdb):
    from datetime import datetime, timedelta
    route_sig({"events": [{"kind": "alarm",
                           "fire_at": datetime.now() + timedelta(hours=14),
                           "text": "起床", "repeat": "daily"}]})
    with client.websocket_connect("/onebot/v11/ws") as ws:
        ws.send_text(json.dumps(_msg_event(message="每天早上7点叫我起床")))
        a1 = _drain_action(ws)                       # 回复
        assert "回声模式" in a1["params"]["message"]
        a2 = _drain_action(ws)                       # 事件确认 -> QQ 消息
        assert a2["action"] == "send_private_msg"
        assert "我记下了" in a2["params"]["message"]
        assert "每天都算" in a2["params"]["message"]
        assert len(fdb.events) == 1


def test_streak_pushed_proactively_via_onebot(client, route_sig, fdb):
    route_sig({})
    from datetime import datetime, timedelta
    import asyncio
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    # 用 FakeDB 直接种下"昨天来过、已连续 4 天" -> 这次该庆祝第 5 天
    asyncio.run(fdb.set_meta("ob-10000-p123", "last_active", yesterday))
    asyncio.run(fdb.set_meta("ob-10000-p123", "streak", "4"))
    with client.websocket_connect("/onebot/v11/ws") as ws:
        ws.send_text(json.dumps(_msg_event(message="我来了")))
        actions = [_drain_action(ws), _drain_action(ws)]
        texts = [a["params"]["message"] for a in actions]
        assert any("连续陪伴的第 5 天" in t for t in texts), texts
        assert any("回声模式" in t for t in texts), texts


def test_access_token_enforced(client, route_sig, monkeypatch):
    monkeypatch.setattr(onebot, "ACCESS_TOKEN", "secret")
    with pytest.raises(WebSocketDisconnect):
        with client.websocket_connect("/onebot/v11/ws") as ws:
            ws.receive_text()
    with client.websocket_connect("/onebot/v11/ws?access_token=secret") as ws:
        ws.send_text(json.dumps(_msg_event(message="在吗")))
        action = _drain_action(ws)
        assert "回声模式" in action["params"]["message"]


def test_disconnected_target_returns_false(client, route_sig):
    import asyncio
    route_sig({})
    assert asyncio.run(onebot.send_to_session("ob-x-p1", "hi")) is False
