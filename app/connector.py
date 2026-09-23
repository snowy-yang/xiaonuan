"""AI 连接器：大模型能力的统一入口（OpenAI 兼容接口）。

- 主模型：流式对话已由「原生工具调用循环」取代（run_model_tools）
- 小模型（AI_SMALL_*，缺省复用主模型）：每条消息一次快速路由判定
- 夜间通道（AI_BATCH_*，缺省复用主模型）：小学徒夜间琐事用
- 回声模式：什么模型都没配时，诚实回声，不做任何假装
"""

import asyncio
import json
import os
import re
from datetime import datetime, timedelta

import httpx
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("AI_API_KEY", "")
BASE_URL = os.getenv("AI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
MODEL = os.getenv("AI_MODEL", "gpt-4o-mini")
EMBED_MODEL = os.getenv("AI_EMBED_MODEL", "text-embedding-3-small")
EMBED_DIM = 256  # 回声模式哈希 embedding 的维度

# 夜间/批量通道：给小学徒干琐事用的便宜通道，不配则回落主模型
BATCH_KEY = os.getenv("AI_BATCH_KEY", "") or API_KEY
BATCH_BASE = (os.getenv("AI_BATCH_BASE_URL", "") or BASE_URL).rstrip("/")
BATCH_MODEL = os.getenv("AI_BATCH_MODEL", "") or MODEL
BATCH_CONFIGURED = bool(os.getenv("AI_BATCH_MODEL")
                        or os.getenv("AI_BATCH_BASE_URL"))

# 极小模型快速路由：不配则复用主模型
SMALL_KEY = os.getenv("AI_SMALL_KEY", "") or API_KEY
SMALL_BASE = (os.getenv("AI_SMALL_BASE_URL", "") or BASE_URL).rstrip("/")
SMALL_MODEL = os.getenv("AI_SMALL_MODEL", "") or MODEL

# 小暖的小学徒：夜间琐事都是她干的
ASSISTANT_NAME = os.getenv("AI_ASSISTANT_NAME", "小满")
ASSISTANT_PERSONA = os.getenv(
    "AI_ASSISTANT_PERSONA",
    "你是一位小学徒，勤快、话少、偶尔偷懒。你负责夜间的琐事：整理回忆、"
    "写回顾草稿。你写的东西都会经小暖检查后才交给用户。",
)


# ---- 回声模式 ----

async def _echo_stream(text: str):
    """回声模式：把用户的话切片吐回去"""
    reply = f"（回声模式，未配置 AI_API_KEY）你说的是：{text}"
    for ch in reply:
        yield ch
        await asyncio.sleep(0.015)


# ---- 向量 ----

def hash_embed(text: str) -> list[float]:
    """无 embedding 接口时的本地替身：字符二元组哈希到 256 维并归一化"""
    import hashlib
    vec = [0.0] * EMBED_DIM
    t = text.strip().lower()
    grams = [t[i:i + 2] for i in range(len(t) - 1)] or [t]
    for g in grams:
        h = int(hashlib.md5(g.encode("utf-8")).hexdigest(), 16)
        vec[h % EMBED_DIM] += 1.0
    norm = sum(v * v for v in vec) ** 0.5 or 1.0
    return [v / norm for v in vec]


async def embed(texts: list[str]) -> list[list[float]]:
    """批量把文本变成向量。没配 embedding 接口时用本地哈希"""
    if not API_KEY:
        return [hash_embed(t) for t in texts]
    async with httpx.AsyncClient(timeout=60) as client:
        res = await client.post(
            f"{BASE_URL}/embeddings",
            headers={"Authorization": f"Bearer {API_KEY}"},
            json={"model": EMBED_MODEL, "input": texts},
        )
        res.raise_for_status()
        return [d["embedding"] for d in res.json()["data"]]


# ---- 基础调用 ----

_TEST_TRANSPORT = None  # 测试注入点


def _make_client() -> httpx.AsyncClient:
    if _TEST_TRANSPORT is not None:
        return httpx.AsyncClient(timeout=120, transport=_TEST_TRANSPORT)
    return httpx.AsyncClient(timeout=120)


async def _complete(base: str, key: str, model: str,
                    messages: list[dict], temperature: float = 0) -> str:
    async with _make_client() as client:
        res = await client.post(
            f"{base}/chat/completions",
            headers={"Authorization": f"Bearer {key}"},
            json={"model": model, "messages": messages,
                  "temperature": temperature},
        )
        res.raise_for_status()
        return res.json()["choices"][0]["message"]["content"]


async def complete(messages: list[dict], temperature: float = 0) -> str:
    """主模型非流式调用：问候/日记/关怀这类一次性生成用"""
    return await _complete(BASE_URL, API_KEY, MODEL, messages, temperature)


async def complete_night(messages: list[dict], temperature: float = 0.3) -> str:
    """夜间通道：配置了 AI_BATCH_* 走便宜通道，否则回落主模型"""
    return await _complete(BATCH_BASE, BATCH_KEY, BATCH_MODEL,
                           messages, temperature)


# ---- 原生工具调用（function calling）----

TOOLS_SCHEMA = [
    {"type": "function", "function": {
        "name": "get_weather",
        "description": "查询某地现在的天气和今日温度、降水概率。"
                       "用户问到天气、要不要带伞、冷不冷，或你在写和出门"
                       "有关的问候需要天气时使用。",
        "parameters": {"type": "object", "properties": {
            "city": {"type": "string",
                     "description": "城市名。用户没提到就省略，"
                                    "系统会尝试从记忆里推断。"}},
            "required": []}}},
    {"type": "function", "function": {
        "name": "get_news",
        "description": "看新闻。用户问新闻、热点、最近发生什么事时使用。",
        "parameters": {"type": "object", "properties": {
            "query": {"type": "string",
                      "description": "新闻主题关键词，如 科技；省略则给头条"}},
            "required": []}}},
    {"type": "function", "function": {
        "name": "web_search",
        "description": "联网搜索你不知道的、或时效性强的事实。",
        "parameters": {"type": "object", "properties": {
            "query": {"type": "string", "description": "搜索词"}},
            "required": ["query"]}}},
]


async def run_model_tools(messages: list[dict], executor,
                          max_rounds: int = 3) -> str:
    """带原生工具调用的对话循环：模型自己决定调什么工具，
    executor(name, args) 负责执行并返回文本结果。返回最终回复文本。"""
    msgs = list(messages)
    async with _make_client() as client:
        for _ in range(max_rounds):
            res = await client.post(
                f"{BASE_URL}/chat/completions",
                headers={"Authorization": f"Bearer {API_KEY}"},
                json={"model": MODEL, "messages": msgs,
                      "tools": TOOLS_SCHEMA},
            )
            res.raise_for_status()
            m = res.json()["choices"][0]["message"]
            calls = m.get("tool_calls")
            if not calls:
                return m.get("content") or ""
            msgs.append({"role": "assistant",
                         "content": m.get("content") or "",
                         "tool_calls": calls})
            for tc in calls:
                fn = tc.get("function") or {}
                try:
                    args = json.loads(fn.get("arguments") or "{}")
                except json.JSONDecodeError:
                    args = {}
                result = await executor(fn.get("name"), args)
                msgs.append({"role": "tool",
                             "tool_call_id": tc.get("id") or "",
                             "content": str(result)})
        # 超出轮次上限：不带工具强制收尾，宁可平淡也不空转
        res = await client.post(
            f"{BASE_URL}/chat/completions",
            headers={"Authorization": f"Bearer {API_KEY}"},
            json={"model": MODEL, "messages": msgs},
        )
        res.raise_for_status()
        return res.json()["choices"][0]["message"].get("content") or ""


# ---- 极小模型快速路由：每条消息一次调用，判完所有快事 ----

_ROUTER_VIEWS = {"memories", "timetable", "diary", "digest",
                 "assistant", "status", "lore"}
_ROUTER_LABELS = {"出门上学", "到校", "放学", "到家", "起床", "睡觉"}


async def route(text: str, now: datetime, existing: list[str]) -> dict:
    """小模型一次判定：查看档案 / 慢信 / 情绪 / 城市 / 各类提取。
    所有字段已清洗；模型不可用或解析失败时返回空判定。"""
    empty = {"view": None, "letter": None, "negative_mood": False,
             "city_hint": "", "facts": [], "events": [], "routines": [],
             "promises": [], "lore": []}
    if not (SMALL_KEY and SMALL_MODEL):
        return empty

    user = (
        f"现在是 {now:%Y-%m-%d %H:%M}（本地时间）。对下面的用户发言做判定，"
        '只输出一个 JSON 对象：\n'
        '{"view": null或"memories|timetable|diary|digest|assistant|status|lore",'
        ' "letter": null或{"days":7,"body":"信的内容"}或{"ask":true},'
        ' "negative_mood": false, "city_hint": "",'
        ' "facts": ["用户叫小明"],'
        ' "events": [{"kind":"alarm或reminder","fire_at":"ISO时间",'
        '"text":"起床","repeat":"once或daily"}],'
        ' "routines": [{"label":"出门上学|到校|放学|到家|起床|睡觉 之一",'
        '"time":"HH:MM","kind":"explicit或inferred"}],'
        ' "promises": [{"note":"面试","due_at":"ISO时间或null"}],'
        ' "lore": [{"topic":"诞生","body":"小暖是用户亲手造的"}]}\n'
        "字段含义：\n"
        "- view：用户想看她的某份档案——记忆/时间表/日记/昨天回顾/学徒/状态/身世；"
        "普通聊天就是 null。\n"
        "- letter：写给未来自己的慢信。days=天数、body=内容；"
        '想写但没说完整给 {"ask":true}；不是写信就是 null。\n'
        "- negative_mood：是否在倾诉负面情绪。\n"
        '- city_hint：这句话里提到的用户所在城市（"我在杭州，明天冷不冷"→"杭州"），'
        "没有为空串。\n"
        "- facts：值得长期记住的关于用户的信息，以「用户」开头，"
        "已有的不要重复，最多5条。\n"
        "- events：定时任务（叫我起床/提醒我/每天XX点…）；"
        "说「每天」repeat=daily 否则 once；fire_at 推算成最近一个将来的时间。\n"
        "- routines：作息规律(explicit，如「每天7点半出门上学」)或"
        "当下日常活动(inferred，如「到学校啦」，time 用当前时间)。\n"
        "- promises：用户提到将来要做/会发生、值得之后回访的事；"
        "due_at 说不清给 null。\n"
        "- lore：用户在定义小暖自己的身世、身份、来历"
        "（「你是XX造的」「你的生日是X」「你叫小暖因为X」）。\n"
        "哪类都没有就 null / 空数组 / false。\n"
        f"已有记忆：{json.dumps(existing, ensure_ascii=False)}\n"
        f"用户发言：{text}"
    )
    try:
        raw = await _complete(
            SMALL_BASE, SMALL_KEY, SMALL_MODEL,
            [{"role": "system",
              "content": "你是意图路由器，只输出 JSON。"},
             {"role": "user", "content": user}],
            temperature=0)
        m = re.search(r"\{.*\}", raw, re.S)
        if not m:
            return empty
        data = json.loads(m.group())
    except Exception:
        return empty

    out = dict(empty)
    if data.get("view") in _ROUTER_VIEWS:
        out["view"] = data["view"]

    letter = data.get("letter")
    if isinstance(letter, dict):
        if letter.get("ask"):
            out["letter"] = ("ask", "")
        else:
            try:
                days = int(letter.get("days"))
                body = str(letter.get("body") or "").strip()
                if 1 <= days <= 365 and body:
                    out["letter"] = (days, body[:2000])
            except (TypeError, ValueError):
                pass

    out["negative_mood"] = bool(data.get("negative_mood"))
    out["city_hint"] = str(data.get("city_hint") or "").strip()[:20]

    if isinstance(data.get("facts"), list):
        for f in data["facts"][:5]:
            s = str(f).strip()
            if s and s not in existing and s not in out["facts"]:
                out["facts"].append(s)

    fallback = (now + timedelta(days=1)).replace(hour=18, minute=0,
                                                 second=0, microsecond=0)
    for item in (data.get("events") or [])[:3]:
        try:
            fire = datetime.fromisoformat(str(item["fire_at"]))
        except (KeyError, ValueError, TypeError):
            continue
        if fire <= now:
            continue
        out["events"].append({
            "kind": "alarm" if item.get("kind") == "alarm" else "reminder",
            "fire_at": fire,
            "text": str(item.get("text") or "提醒").strip(),
            "repeat": "daily" if item.get("repeat") == "daily" else "once"})

    for item in (data.get("routines") or [])[:2]:
        label = str(item.get("label") or "")
        mm = re.match(r"^(\d{1,2}):(\d{2})$", str(item.get("time") or ""))
        if label in _ROUTER_LABELS and mm:
            out["routines"].append({
                "label": label,
                "minute": int(mm.group(1)) * 60 + int(mm.group(2)),
                "kind": "explicit" if item.get("kind") == "explicit"
                else "inferred"})

    for item in (data.get("promises") or [])[:3]:
        note = str(item.get("note") or "").strip()
        if not note:
            continue
        try:
            due = datetime.fromisoformat(str(item.get("due_at")))
            if due <= now:
                due = fallback
        except (ValueError, TypeError):
            due = fallback
        out["promises"].append({"note": note, "due_at": due})

    for item in (data.get("lore") or [])[:3]:
        topic = str(item.get("topic") or "").strip()[:12]
        body = str(item.get("body") or "").strip()[:200]
        if topic and body:
            out["lore"].append({"topic": topic, "body": body})
    return out
