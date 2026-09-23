"""她的小工具：天气 / 新闻 / 搜索（全部免 API Key）

天气  open-meteo（地理编码 + 实时天气）
新闻  Bing News RSS（支持中文关键词）
搜索  DuckDuckGo lite
"""

import html
import re
import xml.etree.ElementTree as ET

import httpx

_UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# WMO 天气代码 -> 中文
WMO = {
    0: "晴", 1: "大致晴", 2: "多云", 3: "阴",
    45: "有雾", 48: "雾凇",
    51: "毛毛雨", 53: "毛毛雨", 55: "浓毛毛雨",
    61: "小雨", 63: "中雨", 65: "大雨", 66: "冻雨", 67: "强冻雨",
    71: "小雪", 73: "中雪", 75: "大雪", 77: "雪粒",
    80: "阵雨", 81: "阵雨", 82: "强阵雨", 85: "阵雪", 86: "阵雪",
    95: "雷雨", 96: "雷雨伴冰雹", 99: "雷雨伴冰雹",
}


def _clean(s: str | None) -> str:
    """去 HTML 标签和实体"""
    if not s:
        return ""
    s = re.sub(r"<[^>]+>", "", s)
    return html.unescape(s).strip()


async def get_weather(city: str) -> str:
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as c:
            g = await c.get("https://geocoding-api.open-meteo.com/v1/search",
                            params={"name": city, "language": "zh", "count": 1})
            g.raise_for_status()
            results = g.json().get("results") or []
            if not results:
                return f"没找到「{city}」这个地方，换个说法试试？"
            loc = results[0]
            f = await c.get("https://api.open-meteo.com/v1/forecast", params={
                "latitude": loc["latitude"], "longitude": loc["longitude"],
                "current": "temperature_2m,weather_code,wind_speed_10m,"
                           "relative_humidity_2m",
                "daily": "temperature_2m_max,temperature_2m_min,"
                         "precipitation_probability_max",
                "timezone": "auto", "forecast_days": 1,
            })
            f.raise_for_status()
            data = f.json()
            cur, daily = data["current"], data["daily"]
            sky = WMO.get(int(cur["weather_code"]), "天气未知")
            return (f"{loc['name']}现在{sky}，{cur['temperature_2m']}°C，"
                    f"湿度{cur['relative_humidity_2m']}%，"
                    f"风{cur['wind_speed_10m']}km/h；今天 "
                    f"{daily['temperature_2m_min'][0]}~"
                    f"{daily['temperature_2m_max'][0]}°C，"
                    f"降水概率{daily['precipitation_probability_max'][0]}%")
    except Exception as e:
        return f"天气服务这会儿不太顺畅（{e.__class__.__name__}）"


async def get_news(query: str = "今日热点", limit: int = 5) -> str:
    """Google News RSS（中文）为主，新浪滚动新闻兜底"""
    headers = {"User-Agent": _UA["User-Agent"]}
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True,
                                     headers=headers) as c:
            try:
                r = await c.get("https://news.google.com/rss/search", params={
                    "q": query, "hl": "zh-CN", "gl": "CN",
                    "ceid": "CN:zh-Hans"})
                r.raise_for_status()
                items = ET.fromstring(r.text).findall(".//item")[:limit]
                lines = []
                for i in items:
                    # Google News 标题带「 - 媒体名」尾巴，去掉
                    title = _clean(i.findtext("title")).rsplit(" - ", 1)[0]
                    lines.append(f"· {title}")
                if lines:
                    return "\n".join(lines)
            except Exception:
                pass
            # 兜底：新浪滚动新闻
            r = await c.get("https://rss.sina.com.cn/news/marquee/ddt.xml")
            r.raise_for_status()
            items = ET.fromstring(r.content).findall(".//item")[:limit]
            if not items:
                return "新闻源这会儿没给出内容"
            return "\n".join(f"· {_clean(i.findtext('title'))}" for i in items)
    except Exception as e:
        return f"新闻服务这会儿不太顺畅（{e.__class__.__name__}）"


async def web_search(query: str, limit: int = 5) -> str:
    try:
        async with httpx.AsyncClient(timeout=10, follow_redirects=True,
                                     headers=_UA) as c:
            r = await c.get("https://lite.duckduckgo.com/lite/",
                            params={"q": query})
            r.raise_for_status()
            titles = re.findall(r"class=['\"]result-link['\"][^>]*>(.*?)</a>",
                                r.text)
            snippets = re.findall(
                r"class=['\"]result-snippet['\"]>(.*?)</td>", r.text)
            out = []
            for i, t in enumerate(titles[:limit]):
                snip = _clean(snippets[i]) if i < len(snippets) else ""
                out.append(f"· {_clean(t)}" + (f" —— {snip[:60]}" if snip else ""))
            return "\n".join(out) if out else "没搜到什么，换个词试试？"
    except Exception as e:
        return f"搜索这会儿不太顺畅（{e.__class__.__name__}）"
