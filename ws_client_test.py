"""端到端测试客户端
uv run python ws_client_test.py [消息]            — 发一条消息
uv run python ws_client_test.py --memory         — 会话历史：发消息后重连，核对历史

注意：信号提取（记忆/事件/作息/情绪/身世）由小模型路由完成，
纯回声模式（完全没配模型）下这些功能不会工作。
"""
import asyncio
import json
import sys
import uuid

import websockets

URL = "ws://127.0.0.1:3000/ws"


async def recv_until(ws, types, timeout=15):
    """收消息直到出现 types 中的类型，返回该消息"""
    while True:
        m = json.loads(await asyncio.wait_for(ws.recv(), timeout=timeout))
        if m["type"] in types:
            return m


async def chat_once(text, session):
    async with websockets.connect(URL) as ws:
        await ws.send(json.dumps({"type": "chat", "text": text, "session": session}))
        out = ""
        while True:
            m = json.loads(await asyncio.wait_for(ws.recv(), timeout=15))
            if m["type"] == "delta":
                out += m["text"]
            elif m["type"] == "done":
                return out
            elif m["type"] == "error":
                print("错误:", m["text"])
                sys.exit(1)


async def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--remember":
        # 长期记忆测试：自我介绍 -> 等抽取 -> 查记忆 -> 忘记
        session = f"mem-{uuid.uuid4().hex[:8]}"
        await chat_once("我叫小明，我喜欢打篮球", session)
        async with websockets.connect(URL) as ws:
            await ws.send(json.dumps({"type": "memories", "session": session}))
            m = json.loads(await asyncio.wait_for(ws.recv(), timeout=15))
            facts = [x["text"] for x in m["items"]]
            print("记忆条数:", len(facts), facts)
            assert any("小明" in f for f in facts), "应记住名字"
            assert any("篮球" in f for f in facts), "应记住喜好"
            # 忘记第一条
            await ws.send(json.dumps({"type": "forget", "id": m["items"][0]["id"], "session": session}))
            m = json.loads(await asyncio.wait_for(ws.recv(), timeout=15))
            print("删除后:", [x["text"] for x in m["items"]])
        print("长期记忆测试通过")
        return

    if len(sys.argv) > 1 and sys.argv[1] == "--memory":
        session = f"test-{uuid.uuid4().hex[:8]}"
        r1 = await chat_once("我叫小明，记住我", session)
        print("第 1 轮回复:", r1)

        # 重连（模拟刷新页面），拉回历史
        async with websockets.connect(URL) as ws:
            await ws.send(json.dumps({"type": "history", "session": session}))
            m = await recv_until(ws, {"history"})
            print("重连后历史条数:", len(m["items"]))

        # 同一 session 继续聊，验证模型（回声模式则验证历史长度递增）
        r2 = await chat_once("你还记得我吗？", session)
        print("第 2 轮回复:", r2)

        async with websockets.connect(URL) as ws:
            await ws.send(json.dumps({"type": "history", "session": session}))
            m = await recv_until(ws, {"history"})
            assert len(m["items"]) == 4, f"应累计 4 条，实际 {len(m['items'])}"
            print("记忆验证通过：历史 4 条（2 轮问答）")

        # 会话与记忆不可清空：发 clear/forget 应被忽略，历史原样还在
        async with websockets.connect(URL) as ws:
            await ws.send(json.dumps({"type": "clear", "session": session}))
            await ws.send(json.dumps({"type": "forget", "id": 1,
                                      "session": session}))
            await ws.send(json.dumps({"type": "history", "session": session}))
            m = await recv_until(ws, {"history"})
            assert len(m["items"]) == 4, "clear/forget 应无效，历史仍是 4 条"
            print("永久记忆验证通过：clear/forget 被忽略")
        return

    text = sys.argv[1] if len(sys.argv) > 1 else "你好，测试一下"
    print("回复:", await chat_once(text, "default"))


asyncio.run(main())
