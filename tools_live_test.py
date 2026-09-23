"""工具联网实测：uv run python tools_live_test.py"""
import asyncio

from app.tools import get_news, get_weather, web_search


async def main():
    w = await get_weather("北京")
    print("天气:", w)
    assert "°C" in w and "不太顺畅" not in w, w

    n = await get_news("科技")
    print("新闻:\n", n)
    assert "·" in n and "不太顺畅" not in n, n

    s = await web_search("open-meteo api")
    print("搜索:\n", s)
    assert "·" in s and "不太顺畅" not in s, s

    print("工具实测通过")


asyncio.run(main())
