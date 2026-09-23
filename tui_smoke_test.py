"""TUI 无头冒烟测试：uv run python tui_smoke_test.py"""
import asyncio

from tui import CompanionTUI


async def main():
    app = CompanionTUI()
    async with app.run_test(size=(100, 30)) as pilot:
        app.add_bubble("你", "你好呀")
        app.add_bubble("小暖", "在呢，怎么了？")
        app.system("这是一条系统消息")
        app.handle_msg({"type": "streak", "days": 4})
        app.handle_msg({"type": "care", "kind": "promise", "note": "面试"})
        app.handle_msg({"type": "lore_saved", "topics": ["诞生"]})
        app.handle_msg({"type": "memories",
                        "items": [{"id": 1, "text": "用户叫小明"}]})  # 应被静默忽略
        await pilot.pause()
        # 输入提交路径（ws 未连接 -> 应有系统提示气泡）
        inp = app.query_one("Input")
        inp.value = "随便说说"
        await pilot.pause()
        from textual.widgets import Input as TInput
        await app.on_input_submitted(TInput.Submitted(inp, inp.value))
        await pilot.pause()
        assert app._bubble is None
    print("TUI 冒烟测试通过")


asyncio.run(main())
