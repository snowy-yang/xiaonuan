"""杂项离线测试（连续天数/彩蛋时刻/容器感知/学徒模板）：
uv run python extra_test.py
"""
import asyncio
from datetime import datetime

from app.connector import ASSISTANT_NAME
from app.main import _daily_slot, absence_text, compute_streak, digest_echo, \
    human_gap

NOW = datetime(2026, 9, 21, 19, 30)  # 周一晚上

# 情绪识别已交给小模型路由（见 router_test.py）；这里只测不依赖模型的纯逻辑。

# 连续陪伴天数
assert compute_streak("2026-09-20", 5, NOW) == 6   # 昨天来过 -> +1
assert compute_streak("2026-09-19", 5, NOW) == 1   # 断了 -> 重新数
assert compute_streak("", 0, NOW) == 1             # 第一次
assert compute_streak("2026-09-21", 7, NOW) == 7   # 今天已记过，不变

# 每日彩蛋时刻：确定性 + 落在窗口内
s1 = _daily_slot("sess-a", "replay", 19 * 60, 22 * 60, "2026-09-21")
s2 = _daily_slot("sess-a", "replay", 19 * 60, 22 * 60, "2026-09-21")
s3 = _daily_slot("sess-b", "replay", 19 * 60, 22 * 60, "2026-09-21")
assert s1 == s2, "同一会话同一天应恒定"
assert 19 * 60 <= s1 < 22 * 60, "应落在窗口内"
assert 19 * 60 <= s3 < 22 * 60

slot_share = _daily_slot("sess-a", "share", 10 * 60, 21 * 60, "2026-09-21")
assert 10 * 60 <= slot_share < 21 * 60

# 容器感知：时间间隔的说法
assert human_gap(59) == "1 分钟"
assert human_gap(5 * 60) == "5 分钟"
assert human_gap(2 * 3600 + 47 * 60) == "2 小时 47 分钟"
assert human_gap(3 * 86400) == "3 天"


# 缺勤迎接（回声模板；真实模式由模型写，接入后由 ws 测试覆盖）
async def absence_check():
    line = await absence_text("s1", 3700)
    assert "小时" in line, line
    assert ("安静" in line) or ("心跳" in line) or ("开灯" in line), line


asyncio.run(absence_check())

# 回顾草稿模板（小学徒署名）
d = digest_echo({"msgs": 12, "mem": 3}, 45)
assert "12" in d and "3" in d and "45" in d and ASSISTANT_NAME in d, d

print("扩展功能测试通过")
