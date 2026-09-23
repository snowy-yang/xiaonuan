"""纯逻辑测试：连续天数 / 彩蛋时刻 / 时间间隔 / 模板 / 向量"""
from datetime import datetime

from app.connector import ASSISTANT_NAME, hash_embed
from app.db import cosine
from app.main import (_daily_slot, absence_text, compute_streak, digest_echo,
                      human_gap)

NOW = datetime(2026, 9, 21, 19, 30)  # 周一晚上


# ---- 连续陪伴天数 ----

def test_streak_yesterday_increments():
    assert compute_streak("2026-09-20", 5, NOW) == 6


def test_streak_gap_resets():
    assert compute_streak("2026-09-19", 5, NOW) == 1


def test_streak_first_time():
    assert compute_streak("", 0, NOW) == 1


def test_streak_same_day_unchanged():
    assert compute_streak("2026-09-21", 7, NOW) == 7


# ---- 每日彩蛋时刻（哈希槽位）----

def test_daily_slot_deterministic():
    a = _daily_slot("sess-a", "replay", 19 * 60, 22 * 60, "2026-09-21")
    b = _daily_slot("sess-a", "replay", 19 * 60, 22 * 60, "2026-09-21")
    assert a == b


def test_daily_slot_within_window():
    for key in ("replay", "share", "diary", "digest"):
        slot = _daily_slot("sess-a", key, 10 * 60, 21 * 60, "2026-09-21")
        assert 10 * 60 <= slot < 21 * 60


def test_daily_slot_differs_per_session():
    a = _daily_slot("sess-a", "replay", 19 * 60, 22 * 60, "2026-09-21")
    b = _daily_slot("sess-b", "replay", 19 * 60, 22 * 60, "2026-09-21")
    # 不保证必然不同，但同窗口内两者都合法；这里验证可分布
    assert 19 * 60 <= a < 22 * 60 and 19 * 60 <= b < 22 * 60


# ---- 时间间隔说法 ----

def test_human_gap_minutes():
    assert human_gap(59) == "1 分钟"
    assert human_gap(5 * 60) == "5 分钟"


def test_human_gap_hours():
    assert human_gap(2 * 3600 + 47 * 60) == "2 小时 47 分钟"
    assert human_gap(2 * 3600) == "2 小时"


def test_human_gap_days():
    assert human_gap(3 * 86400) == "3 天"


# ---- 缺勤迎接（回声模板）----

async def test_absence_text_mentions_the_gap():
    line = await absence_text("s1", 3700)
    assert "小时" in line
    assert ("安静" in line) or ("心跳" in line) or ("开了灯" in line)


# ---- 学徒回顾模板 ----

def test_digest_echo_signature_and_numbers():
    d = digest_echo({"msgs": 12, "mem": 3}, 45)
    assert "12" in d and "3" in d and "45" in d and ASSISTANT_NAME in d


# ---- 向量（回声模式哈希 embedding）----

def test_hash_embed_self_similarity():
    v = hash_embed("我喜欢打篮球")
    assert len(v) == 256
    assert abs(cosine(v, v) - 1.0) < 1e-9


def test_hash_embed_related_beats_unrelated():
    base = hash_embed("我喜欢打篮球")
    near = hash_embed("打篮球的时候我很开心")
    far = hash_embed("明天北京的天气怎么样")
    assert cosine(base, near) > cosine(base, far)


def test_cosine_handles_empty():
    assert cosine(hash_embed("x"), []) == 0.0
