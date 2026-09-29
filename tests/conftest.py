"""共享测试设施：内存版 FakeDB + 不依赖 PG 的测试应用。

FakeDB 完整复刻 app/db.py 的函数签名，让服务端全链路逻辑
（WebSocket 聊天流、调度投递、路由信号落库）可以脱离 PG 测试。
"""

import time

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app import connector, db, main
from app.db import cosine


class FakeDB:
    """和 app/db.py 一一对应的内存实现（字段同名，行为简化）"""

    def __init__(self):
        self.history = []   # {id, session, role, content, ts}
        self.memories = []  # {id, session, text, ts, embedding}
        self.events = []    # {id, session, kind, fire_at, text, repeat,
        #                     minute_of_day, status, last_fired, created_at}
        self.routines = {}  # (session, label) -> {id, session, label, minute,
        #                     evidence, kind, last_fired, updated_at}
        self.care = []      # {id, session, kind, note, due_at, status, created_at}
        self.letters = []   # {id, session, body, deliver_at, status, created_at}
        self.meta = {}      # (session, key) -> str
        self.lore = {}      # (session, topic) -> body
        self.diary = {}     # (session, day) -> body
        self.digests = {}   # (session, day) -> body
        self._id = 0

    def _next(self):
        self._id += 1
        return self._id

    # ---- history ----
    async def add_message(self, session, role, content):
        self.history.append({"id": self._next(), "session": session,
                             "role": role, "content": content,
                             "ts": time.time()})

    async def get_history(self, session, limit=40):
        rows = [r for r in self.history if r["session"] == session]
        return [{"role": r["role"], "content": r["content"]}
                for r in rows[-limit:]]

    # ---- memories ----
    async def add_memories(self, session, facts, embeddings=None):
        for i, f in enumerate(facts):
            self.memories.append({"id": self._next(), "session": session,
                                  "text": f, "ts": time.time(),
                                  "embedding": embeddings[i] if embeddings else None})

    async def get_memories(self, session):
        return [{"id": m["id"], "text": m["text"]}
                for m in self.memories if m["session"] == session]

    async def search_memories(self, session, query_vec, k=6):
        rows = [m for m in self.memories if m["session"] == session
                and m["embedding"]]
        scored = [{"id": m["id"], "text": m["text"],
                   "score": cosine(query_vec, m["embedding"])} for m in rows]
        scored.sort(key=lambda x: x["score"], reverse=True)
        return scored[:k]

    async def replace_memories(self, session, facts, embeddings):
        self.memories = [m for m in self.memories if m["session"] != session]
        await self.add_memories(session, facts, embeddings)

    async def memory_from_days_ago(self, session, days, now_ts):
        lo, hi = now_ts - days * 86400, now_ts - (days - 1) * 86400
        out = [{"content": m["text"], "ts": m["ts"]}
               for m in self.memories if m["session"] == session
               and lo <= m["ts"] <= hi]
        out += [{"content": h["content"], "ts": h["ts"]}
                for h in self.history if h["session"] == session
                and h["role"] == "user" and lo <= h["ts"] <= hi]
        return sorted(out, key=lambda x: x["ts"])[:20]

    # ---- events ----
    async def add_event(self, session, kind, fire_at, text,
                        repeat="once", minute_of_day=None):
        eid = self._next()
        self.events.append({"id": eid, "session": session, "kind": kind,
                            "fire_at": fire_at, "text": text,
                            "repeat": repeat, "minute_of_day": minute_of_day,
                            "status": "pending", "last_fired": None,
                            "created_at": time.time()})
        return eid

    async def due_events(self, session=None):
        now = time.time()
        rows = [e for e in self.events
                if e["status"] == "pending" and e["repeat"] == "once"
                and e["fire_at"] <= now
                and (session is None or e["session"] == session)]
        return [dict(e) for e in sorted(rows, key=lambda x: x["fire_at"])]

    async def due_daily_events(self, today, minute_now, window=10):
        rows = [e for e in self.events
                if e["status"] == "pending" and e["repeat"] == "daily"
                and e["minute_of_day"] is not None
                and e["minute_of_day"] <= minute_now
                and e["minute_of_day"] > minute_now - window
                and e["last_fired"] != today]
        return [dict(e) for e in rows]

    async def today_events(self, session, day_start):
        rows = [e for e in self.events if e["session"] == session
                and e["status"] == "pending"
                and (e["repeat"] == "daily" or e["fire_at"] >= day_start)]
        return [dict(e) for e in sorted(rows, key=lambda x: x["fire_at"])]

    async def mark_event_done(self, event_id):
        for e in self.events:
            if e["id"] == event_id:
                e["status"] = "done"

    async def mark_event_fired(self, event_id, today):
        for e in self.events:
            if e["id"] == event_id:
                e["last_fired"] = today

    # ---- routines ----
    async def upsert_routine(self, session, label, minute, kind):
        key = (session, label)
        row = self.routines.get(key)
        if row is None:
            self.routines[key] = {"id": self._next(), "session": session,
                                  "label": label, "minute": minute,
                                  "evidence": 1, "kind": kind,
                                  "last_fired": None, "updated_at": time.time()}
        else:
            if kind == "explicit":
                row["minute"] = minute
                row["kind"] = "explicit"
            else:
                row["minute"] = (row["minute"] * row["evidence"] + minute) \
                    // (row["evidence"] + 1)
            row["evidence"] = min(row["evidence"] + 1, 1000)
            row["updated_at"] = time.time()

    async def get_routines(self, session):
        rows = [r for r in self.routines.values() if r["session"] == session]
        return [{k: r[k] for k in ("label", "minute", "evidence", "kind")}
                for r in sorted(rows, key=lambda x: x["minute"])]

    async def due_routines(self, today, minute_now, window=10):
        return [dict(r) for r in self.routines.values()
                if r["minute"] <= minute_now and r["minute"] > minute_now - window
                and r["last_fired"] != today]

    async def mark_routine_fired(self, routine_id, today):
        for r in self.routines.values():
            if r["id"] == routine_id:
                r["last_fired"] = today

    # ---- care ----
    async def add_care(self, session, kind, note, due_at):
        cid = self._next()
        self.care.append({"id": cid, "session": session, "kind": kind,
                          "note": note, "due_at": due_at,
                          "status": "pending", "created_at": time.time()})
        return cid

    async def due_care(self, now_ts):
        return [dict(c) for c in self.care
                if c["status"] == "pending" and c["due_at"] <= now_ts]

    async def pending_care(self, session):
        return [{"id": c["id"], "kind": c["kind"], "note": c["note"],
                 "due_at": c["due_at"]}
                for c in self.care
                if c["session"] == session and c["status"] == "pending"]

    async def mark_care_done(self, care_id):
        for c in self.care:
            if c["id"] == care_id:
                c["status"] = "done"

    # ---- letters ----
    async def add_letter(self, session, body, deliver_at):
        lid = self._next()
        self.letters.append({"id": lid, "session": session, "body": body,
                             "deliver_at": deliver_at, "status": "sealed",
                             "created_at": time.time()})
        return lid

    async def due_letters(self):
        now = time.time()
        return [dict(l) for l in self.letters
                if l["status"] == "sealed" and l["deliver_at"] <= now]

    async def pending_letters(self, session):
        return [{"id": l["id"], "body": l["body"],
                 "deliver_at": l["deliver_at"]}
                for l in self.letters
                if l["session"] == session and l["status"] == "sealed"]

    async def mark_letter_delivered(self, letter_id):
        for l in self.letters:
            if l["id"] == letter_id:
                l["status"] = "delivered"

    # ---- meta ----
    async def get_meta(self, session, key, default=""):
        return self.meta.get((session, key), default)

    async def set_meta(self, session, key, value):
        self.meta[(session, key)] = value

    # ---- lore ----
    async def upsert_lore(self, session, topic, body):
        self.lore[(session, topic)] = body

    async def get_lore(self, session):
        return [{"topic": t[1], "body": b} for t, b in self.lore.items()
                if t[0] == session]

    # ---- diary / digest ----
    async def add_diary(self, session, day, body):
        self.diary.setdefault((session, day), body)

    async def get_diary(self, session, limit=5):
        rows = [{"day": d[1], "body": b} for d, b in self.diary.items()
                if d[0] == session]
        return sorted(rows, key=lambda x: x["day"], reverse=True)[:limit]

    async def add_digest(self, session, day, body):
        self.digests.setdefault((session, day), body)

    async def get_digest(self, session, day):
        return self.digests.get((session, day))

    async def count_digests(self, session):
        return sum(1 for d in self.digests if d[0] == session)

    # ---- stats ----
    async def day_counts(self, session, day_start_ts):
        return {"msgs": sum(1 for h in self.history
                            if h["session"] == session and h["ts"] >= day_start_ts),
                "mem": sum(1 for m in self.memories
                           if m["session"] == session and m["ts"] >= day_start_ts)}

    async def get_stats(self, session):
        rows = [h for h in self.history if h["session"] == session]
        return {"memories": sum(1 for m in self.memories
                                if m["session"] == session),
                "messages": len(rows),
                "first_ts": rows[0]["ts"] if rows else None,
                "db_bytes": 1048576}

    async def active_sessions(self, days=3):
        cut = time.time() - days * 86400
        return list({h["session"] for h in self.history if h["ts"] > cut})


# 需要替换到 app.db 模块上的函数名（与 db.py 对应）
FAKE_FUNCS = [
    "add_message", "get_history",
    "add_memories", "get_memories", "search_memories", "replace_memories",
    "memory_from_days_ago",
    "add_event", "due_events", "due_daily_events", "today_events",
    "mark_event_done", "mark_event_fired",
    "upsert_routine", "get_routines", "due_routines", "mark_routine_fired",
    "add_care", "due_care", "pending_care", "mark_care_done",
    "add_letter", "due_letters", "pending_letters", "mark_letter_delivered",
    "get_meta", "set_meta",
    "upsert_lore", "get_lore",
    "add_diary", "get_diary",
    "add_digest", "get_digest", "count_digests",
    "day_counts", "get_stats", "active_sessions",
]


@pytest.fixture
def fdb(monkeypatch):
    """把 FakeDB 的每个函数替换到 app.db 模块上"""
    fake = FakeDB()
    for name in FAKE_FUNCS:
        monkeypatch.setattr(db, name, getattr(fake, name))
    return fake


@pytest.fixture
def offline(monkeypatch):
    """强制回声模式 + 路由器离线，保证测试确定性"""
    for mod in (main, connector):
        monkeypatch.setattr(mod, "API_KEY", "", raising=False)
    monkeypatch.setattr(connector, "SMALL_KEY", "", raising=False)
    monkeypatch.setattr(connector, "SMALL_MODEL", "", raising=False)


@pytest.fixture
def client(offline, fdb):
    """不带 lifespan 的测试应用（绕过 PG 连接）"""
    from app import onebot

    app = FastAPI()
    app.websocket("/ws")(main.chat)
    app.include_router(onebot.router)
    return TestClient(app)


@pytest.fixture
def route_sig(monkeypatch):
    """指定小模型路由的返回结果"""
    def use(sig):
        async def fake_route(text, now, existing):
            base = {"view": None, "letter": None, "negative_mood": False,
                    "city_hint": "", "facts": [], "events": [], "routines": [],
                    "promises": [], "lore": []}
            base.update(sig)
            return base
        monkeypatch.setattr(main, "route", fake_route)
    return use


@pytest.fixture
def run_chat(client):
    """开一条 WS，依次发送消息，收集到每条 chat 的 done 为止"""
    def _run(messages, session="s1"):
        with client.websocket_connect("/ws") as ws:
            out = []
            for m in messages:
                ws.send_json({"session": session, **m})
            expected = sum(1 for m in messages if m.get("type") == "chat")
            done = 0
            while done < expected:
                m = ws.receive_json()
                out.append(m)
                if m["type"] == "done":
                    done += 1
            return out
    return _run


@pytest.fixture
def reply_text():
    return lambda received: "".join(m["text"] for m in received
                                    if m["type"] == "delta")
