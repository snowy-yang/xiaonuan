"""OneBot 11 适配器：反向 WebSocket（NapCat / Lagrange 等实现连进来）。

接入方式（以 NapCat 为例）：在 OneBot 实现里配置反向 WebSocket 地址
    ws://<本服务>:3000/onebot/v11/ws   （配了 OB_ACCESS_TOKEN 时带 ?access_token=）

- 私聊：所有消息都回
- 群聊：只回应 @她 的消息，回复自动 @ 回去
- 会话 id：ob-<self_id>-p<user_id> / ob-<self_id>-g<group_id>，
  与 TUI 会话隔离，各自有独立的记忆、作息和时间表
- 主动消息（叫早 / 提醒 / 回访 / 慢信 / 连续天数…）经适配器发到对应 QQ
"""

import asyncio
import json
import os
import re
import uuid

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from . import db

router = APIRouter()

# 可选访问令牌：OneBot 实现侧配置同样的 token 即可
ACCESS_TOKEN = os.getenv("OB_ACCESS_TOKEN", "")


class OneBotConn:
    def __init__(self, ws: WebSocket, self_id):
        self.ws = ws
        self.self_id = self_id
        self.pending = {}  # echo -> Future
        self.targets = {}  # session -> {"type", "group_id", "user_id"}


connections: dict = {}  # self_id -> OneBotConn


def _extract_text(message, self_id) -> tuple[str, bool]:
    """返回 (纯文本, 是否@了她)。兼容 CQ 码字符串与消息段数组两种格式"""
    if isinstance(message, str):
        at_me = f"[CQ:at,qq={self_id}]" in message
        return re.sub(r"\[CQ:[^\]]*\]", "", message).strip(), at_me
    parts, at_me = [], False
    for seg in message or []:
        t, d = seg.get("type"), seg.get("data") or {}
        if t == "text":
            parts.append(d.get("text", ""))
        elif t == "at" and str(d.get("qq")) == str(self_id):
            at_me = True
    return "".join(parts).strip(), at_me


async def call_action(conn: OneBotConn, action: str, params: dict,
                      timeout: float = 30) -> dict:
    """向 OneBot 实现发一个 action 并等它的 echo 响应"""
    echo = uuid.uuid4().hex
    fut = asyncio.get_running_loop().create_future()
    conn.pending[echo] = fut
    await conn.ws.send_text(json.dumps(
        {"action": action, "params": params, "echo": echo}))
    try:
        return await asyncio.wait_for(fut, timeout)
    finally:
        conn.pending.pop(echo, None)


async def reply_message(conn: OneBotConn, session: str, text: str) -> bool:
    """往会话对应的 QQ 对象发消息；发送成功才记进对话历史"""
    tgt = conn.targets.get(session)
    if not tgt:
        return False
    if tgt["type"] == "group":
        action = "send_group_msg"
        params = {"group_id": tgt["group_id"],
                  "message": f"[CQ:at,qq={tgt['user_id']}] {text}"}
    else:
        action = "send_private_msg"
        params = {"user_id": tgt["user_id"], "message": text}
    try:
        res = await call_action(conn, action, params)
    except Exception:
        return False
    if res.get("retcode") != 0:
        return False
    await db.add_message(session, "assistant", text)
    return True


async def send_to_session(session: str, text: str) -> bool:
    """主动消息入口（push_proactive 调用）：找挂着这个会话的连接发出去"""
    for conn in list(connections.values()):
        if session in conn.targets:
            return await reply_message(conn, session, text)
    return False


def _notify_for(conn: OneBotConn, session: str):
    """把 remember_later 的推送转成 QQ 消息（只挑用户真正关心的）"""
    async def notify(payload: dict) -> None:
        t = payload.get("type")
        try:
            if t == "event":
                kind = "叫早" if payload.get("kind") == "alarm" else "提醒"
                extra = "，每天都算" if payload.get("repeat") == "daily" else ""
                await reply_message(
                    conn, session,
                    f"好呀，我记下了：{payload['fire_at'].replace('T', ' ')} "
                    f"{kind}（{payload['text']}）{extra}")
            elif t == "care":
                await reply_message(
                    conn, session,
                    "🫂 嗯，我记在心里了。明天这个时候我来问问你好不好些。"
                    if payload.get("kind") == "emotion"
                    else f"好，「{payload['note']}」这事儿我惦记着！"
                         "到时候我来问你结果～")
            elif t == "streak":
                await reply_message(
                    conn, session,
                    f"🔥 对了，今天是我们连续陪伴的第 {payload['days']} 天")
            elif t == "lore_saved":
                await reply_message(
                    conn, session,
                    "🧬 这段身世我刻进脑海里了（"
                    + "、".join(payload["topics"]) + "），永远记得，绝不瞎说。")
            # memories / routines 等后台数据推送不发 QQ，静默
        except Exception:
            pass  # 连接断了就算了，数据已入库
    return notify


async def _process_message(conn: OneBotConn, data: dict) -> None:
    mtype = data.get("message_type")
    user_id = data.get("user_id")
    text, at_me = _extract_text(data.get("message"), conn.self_id)
    if mtype == "private":
        session = f"ob-{conn.self_id}-p{user_id}"
        conn.targets[session] = {"type": "private", "user_id": user_id}
    else:
        session = f"ob-{conn.self_id}-g{data.get('group_id')}"
        conn.targets[session] = {"type": "group",
                                 "group_id": data.get("group_id"),
                                 "user_id": user_id}
    if not text or (mtype == "group" and not at_me):
        return  # 空消息 / 群里没 @ 她
    from .main import chat_turn  # 延迟导入避免循环
    reply = await chat_turn(session, text, _notify_for(conn, session))
    if reply:
        await reply_message(conn, session, reply)


@router.websocket("/onebot/v11/ws")
async def onebot_ws(ws: WebSocket):
    if ACCESS_TOKEN:
        token = ws.query_params.get("access_token") or ""
        auth = ws.headers.get("authorization", "")
        if auth.startswith("Bearer "):
            token = auth[7:]
        if token != ACCESS_TOKEN:
            await ws.close(code=4401)
            return
    await ws.accept()
    conn = None
    try:
        while True:
            data = json.loads(await ws.receive_text())
            if "post_type" in data:  # 事件上报
                self_id = data.get("self_id")
                if conn is None or conn.self_id != self_id:
                    conn = OneBotConn(ws, self_id)
                    connections[self_id] = conn
                if data.get("post_type") == "message":
                    # 后台处理，读循环继续收（事件里可能夹着 action 的 echo 响应）
                    asyncio.create_task(_process_message(conn, data))
            elif conn is not None and "echo" in data:  # action 的响应
                fut = conn.pending.pop(data.get("echo"), None)
                if fut and not fut.done():
                    fut.set_result(data)
            # 其余（对端主动调 action 等）忽略
    except WebSocketDisconnect:
        pass
    finally:
        if conn is not None:
            connections.pop(conn.self_id, None)
