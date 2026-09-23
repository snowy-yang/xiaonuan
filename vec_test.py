"""向量逻辑离线测试：uv run python vec_test.py"""
from app.connector import hash_embed
from app.db import cosine

base = hash_embed("我喜欢打篮球")
near = hash_embed("打篮球的时候我很开心")
far = hash_embed("明天北京的天气怎么样")

print("self:", round(cosine(base, base), 3))
print("near:", round(cosine(base, near), 3))
print("far: ", round(cosine(base, far), 3))

assert abs(cosine(base, base) - 1.0) < 1e-9, "自身相似度应为 1"
assert cosine(base, near) > cosine(base, far), "相近文本应比无关文本相似"
assert cosine(base, []) == 0.0 and cosine([], base) == 0.0, "空向量应返回 0"
assert len(base) == 256, "维度应为 256"
print("向量逻辑测试通过")
