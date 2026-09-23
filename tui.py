"""小暖 TUI 客户端（终端界面）

先启动服务：uv run uvicorn app.main:app --port 3000
再开终端：  uv run python tui.py

没有命令——像人一样说话就好：
「你的记忆里有什么」「给我看看你的时间表」「你的日记呢」「你状态怎么样」
「你的身世是什么」「学徒干了什么」「给7天后的自己写信：<想说的话>」
"""

import asyncio
import json
import uuid
from pathlib import Path

import websockets
from rich.text import Text
from textual.app import App, ComposeResult
from textual.containers import VerticalScroll
from textual.widgets import Footer, Header, Input, Static

URL = "ws://127.0.0.1:3000/ws"
SESSION_FILE = Path(__file__).resolve().parent / "data" / "tui_session.txt"

WHO_STYLE = {"你": "bold cyan", "小暖": "bold green", "系统": "yellow"}


def load_session() -> str:
    """TUI 的会话 id 持久化到 data/，重启后她还记得你"""
    SESSION_FILE.parent.mkdir(exist_ok=True)
    if SESSION_FILE.exists():
        s = SESSION_FILE.read_text("utf-8").strip()
        if s:
            return s
    s = uuid.uuid4().hex[:12]
    SESSION_FILE.write_text(s, "utf-8")
    return s


class CompanionTUI(App):
    TITLE = "小暖 · AI 陪伴"
    SUB_TITLE = "连接中…"

    CSS = """
    #chat { width: 1fr; padding: 0 1; }
    .msg { margin-bottom: 1; }
    Input { border: round #4f6ef7; }
    """

    BINDINGS = [
        ("ctrl+l", "clear_screen", "清屏"),
        ("ctrl+q", "quit", "退出"),
    ]

    def __init__(self) -> None:
        super().__init__()
        self.session = load_session()
        self.ws = None
        self._alive = True
        self._bubble = None      # 正在流式输出的气泡 Static
        self._buf = ""

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield VerticalScroll(id="chat")
        yield Input(placeholder="和小暖说点什么…")
        yield Footer()

    def action_clear_screen(self) -> None:
        self.query_one("#chat", VerticalScroll).remove_children()

    # ---- WebSocket ----

    async def on_mount(self) -> None:
        self.run_worker(self.ws_loop(), exclusive=False)
        self.query_one(Input).focus()

    async def on_unmount(self) -> None:
        self._alive = False

    async def ws_loop(self) -> None:
        """带重连的 WS 客户端循环"""
        while self._alive:
            try:
                async with websockets.connect(URL) as ws:
                    self.ws = ws
                    self.sub_title = "已连接"
                    await ws.send(json.dumps(
                        {"type": "history", "session": self.session}))
                    async for raw in ws:
                        self.handle_msg(json.loads(raw))
            except asyncio.CancelledError:
                return
            except Exception:
                self.ws = None
                self.sub_title = "连接断开，3 秒后重连…（先启动服务？）"
                await asyncio.sleep(3)

    def handle_msg(self, m: dict) -> None:
        t = m.get("type")
        if t == "history":
            for h in m["items"]:
                self.add_bubble("你" if h["role"] == "user" else "小暖",
                                h["content"])
        elif t == "start":
            self._bubble = self.add_bubble("小暖")
            self._buf = ""
        elif t == "delta":
            self._buf += m["text"]
            if self._bubble:
                self._update_bubble(self._bubble, self._buf)
        elif t == "done":
            self._bubble = None
        elif t == "proactive":
            self.add_bubble("小暖", m["text"])
            self.bell()
        elif t == "event":
            when = m["fire_at"].replace("T", " ")
            rep = "，每天都算" if m.get("repeat") == "daily" else ""
            kind = "叫早" if m.get("kind") == "alarm" else "提醒"
            self.add_bubble("小暖",
                            f"好呀，我记下了：{when} {kind}（{m['text']}）{rep}")
        elif t == "care":
            self.add_bubble("小暖", (
                "🫂 嗯，我记在心里了。明天这个时候我来问问你好不好些。"
                if m["kind"] == "emotion"
                else f"好，「{m['note']}」这事儿我惦记着！到时候我来问你结果～"))
        elif t == "streak":
            self.add_bubble("小暖",
                            f"🔥 对了，今天是我们连续陪伴的第 {m['days']} 天")
        elif t == "letter_saved":
            self.add_bubble("小暖", "✉️ 信我替你收好了，到日子亲手交给你。")
        elif t == "lore_saved":
            self.add_bubble(
                "小暖", "🧬 这段身世我刻进脑海里了（"
                + "、".join(m["topics"]) + "），永远记得，绝不瞎说。")
        elif t == "error":
            self.add_bubble("系统", f"出错：{m['text']}")
        # 其余类型（memories/routines/timetable 等）是后台数据推送，界面不展示

    # ---- 聊天区 ----

    def add_bubble(self, who: str, text: str = "") -> Static:
        w = Static(classes="msg")
        self.query_one("#chat", VerticalScroll).mount(w)
        self._update_bubble(w, text, who)
        return w

    def _update_bubble(self, w: Static, text: str,
                       who: str | None = None) -> None:
        if who is None:  # 流式气泡，沿用之前的说话人
            who = getattr(w, "_who", "小暖")
        w._who = who
        prefix = {"你": "你", "小暖": "小暖", "系统": "系统"}[who]
        w.update(Text.assemble((f"{prefix} · ", WHO_STYLE[who]), (text, None)))
        self.query_one("#chat", VerticalScroll).scroll_end(animate=False)

    def system(self, text: str) -> None:
        self.add_bubble("系统", text)

    # ---- 输入：纯对话，没有命令 ----

    async def on_input_submitted(self, event: Input.Submitted) -> None:
        text = event.value.strip()
        event.input.value = ""
        if not text:
            return
        self.add_bubble("你", text)
        if not self.ws:
            self.system("还没连上服务——先在另一个终端启动 "
                         "uv run uvicorn app.main:app --port 3000")
            return
        await self.ws.send(json.dumps(
            {"type": "chat", "text": text, "session": self.session}))


if __name__ == "__main__":
    CompanionTUI().run()
