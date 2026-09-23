"""原生工具调用循环的离线测试（mock 模型服务）：uv run python tool_agent_test.py"""
import asyncio
import json

import httpx

from app import connector

calls = {"model": 0}
captured = {}


def handler(request: httpx.Request) -> httpx.Response:
    body = json.loads(request.content)
    calls["model"] += 1
    if calls["model"] == 1:
        # 第一轮：模型决定调用天气工具
        assert body.get("tools"), "第一轮应带上工具声明"
        return httpx.Response(200, json={"choices": [{"message": {
            "role": "assistant", "content": "",
            "tool_calls": [{"id": "call-1", "type": "function",
                            "function": {"name": "get_weather",
                                         "arguments": json.dumps({"city": "杭州"})}}]}}]})
    # 第二轮：应带着工具结果回来
    tool_msg = body["messages"][-1]
    captured["tool_msg"] = tool_msg
    assert tool_msg["role"] == "tool" and tool_msg["tool_call_id"] == "call-1"
    return httpx.Response(200, json={"choices": [{"message": {
        "role": "assistant",
        "content": "杭州今天有小雨，出门记得带伞哦～"}}]})


async def main():
    connector._TEST_TRANSPORT = httpx.MockTransport(handler)

    async def exec_(name, args):
        captured["exec"] = (name, args)
        return "杭州 小雨 15~22°C 降水概率70%"

    out = await connector.run_model_tools(
        [{"role": "user", "content": "今天出门要带伞吗"}], exec_)

    assert out == "杭州今天有小雨，出门记得带伞哦～", out
    assert captured["exec"] == ("get_weather", {"city": "杭州"}), captured
    assert calls["model"] == 2, calls
    print("工具调用循环测试通过")


asyncio.run(main())
