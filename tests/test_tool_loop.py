"""原生工具调用循环的离线测试（mock 模型服务）"""
import asyncio
import json

import httpx

from app import connector


def test_tool_loop_executes_and_feeds_back(monkeypatch):
    calls = {"model": 0}
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        calls["model"] += 1
        if calls["model"] == 1:
            assert body.get("tools"), "第一轮应带上工具声明"
            return httpx.Response(200, json={"choices": [{"message": {
                "role": "assistant", "content": "",
                "tool_calls": [{"id": "call-1", "type": "function",
                                "function": {
                                    "name": "get_weather",
                                    "arguments": json.dumps({"city": "杭州"})}}]}}]})
        tool_msg = body["messages"][-1]
        captured["tool_msg"] = tool_msg
        assert tool_msg["role"] == "tool"
        assert tool_msg["tool_call_id"] == "call-1"
        return httpx.Response(200, json={"choices": [{"message": {
            "role": "assistant",
            "content": "杭州今天有小雨，出门记得带伞哦～"}}]})

    monkeypatch.setattr(connector, "_TEST_TRANSPORT",
                        httpx.MockTransport(handler))

    async def exec_(name, args):
        captured["exec"] = (name, args)
        return "杭州 小雨 15~22°C 降水概率70%"

    out = asyncio.run(connector.run_model_tools(
        [{"role": "user", "content": "今天出门要带伞吗"}], exec_))

    assert out == "杭州今天有小雨，出门记得带伞哦～"
    assert captured["exec"] == ("get_weather", {"city": "杭州"})
    assert calls["model"] == 2


def test_tool_loop_gives_up_after_max_rounds(monkeypatch):
    calls = {"model": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        calls["model"] += 1
        if "tool_calls" not in json.dumps(body["messages"][-1]) and \
                calls["model"] <= 3:
            return httpx.Response(200, json={"choices": [{"message": {
                "role": "assistant", "content": "",
                "tool_calls": [{"id": f"c{calls['model']}", "type": "function",
                                "function": {"name": "web_search",
                                             "arguments": "{}"}}]}}]})
        # 第 4 次（超过 max_rounds 的强制收尾）不带 tools
        assert "tools" not in body
        return httpx.Response(200, json={"choices": [{"message": {
            "role": "assistant", "content": "我直接说吧"}}]})

    monkeypatch.setattr(connector, "_TEST_TRANSPORT",
                        httpx.MockTransport(handler))

    async def exec_(name, args):
        return "结果"

    out = asyncio.run(connector.run_model_tools(
        [{"role": "user", "content": "搜一下"}], exec_, max_rounds=3))
    assert out == "我直接说吧"
    assert calls["model"] == 4


def test_tool_loop_bad_arguments_become_empty_dict(monkeypatch):
    calls = {"model": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["model"] += 1
        if calls["model"] == 1:
            return httpx.Response(200, json={"choices": [{"message": {
                "role": "assistant", "content": "",
                "tool_calls": [{"id": "c1", "type": "function",
                                "function": {"name": "get_weather",
                                             "arguments": "{坏了"}}]}}]})
        return httpx.Response(200, json={"choices": [{"message": {
            "role": "assistant", "content": "好"}}]})

    monkeypatch.setattr(connector, "_TEST_TRANSPORT",
                        httpx.MockTransport(handler))
    seen = {}

    async def exec_(name, args):
        seen["args"] = args
        return "ok"

    out = asyncio.run(connector.run_model_tools(
        [{"role": "user", "content": "天气"}], exec_))
    assert out == "好" and seen["args"] == {}
