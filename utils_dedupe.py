# utils_dedupe.py
import json, re, hashlib, time, os
from datetime import datetime, timedelta, timezone
try:
    from rapidfuzz import fuzz
except Exception:
    class _F: 
        @staticmethod
        def ratio(a,b): return 0
    fuzz = _F()

SEEN_PATH = "data/seen.json"
STATE_PATH = "data/state.json"  # 记录上次成功推送时间

def _normalize_title(t: str) -> str:
    if not t: return ""
    t = re.sub(r"^【.*?】", "", t)            # 去掉前缀标签
    t = t.lower()
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"[【】\[\]（）()，、。.!?！？:：;；\-—_“”\"'’·•|]+", " ", t)
    t = re.sub(r"\b\d{4}[-/]\d{1,2}[-/]\d{1,2}\b", " ", t)  # 去日期
    return t.strip()

def canonical_url(u: str) -> str:
    if not u: return ""
    u = re.sub(r"([?&])(utm_[^=&]+|spm|from|ref|source|src|share_token|zx|qd)=([^&#]*)", r"\1", u, flags=re.I)
    u = re.sub(r"[?&]+$", "", u)
    return u

def title_id(title: str) -> str:
    # 仅用标题生成指纹，防止同文不同链
    base = _normalize_title(title)
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

def load_json(path, default):
    if not os.path.exists(path): return default
    with open(path, "r", encoding="utf-8") as f:
        try: return json.load(f)
        except Exception: return default

def save_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def load_seen():  return load_json(SEEN_PATH, {"items": []})
def save_seen(s): save_json(SEEN_PATH, s)

def load_state(): return load_json(STATE_PATH, {"last_push_ts": 0})
def save_state(s): save_json(STATE_PATH, s)

def is_near_duplicate(title, seen_titles, threshold=90):
    norm = _normalize_title(title)
    return any(fuzz.ratio(norm, _normalize_title(t)) >= threshold for t in seen_titles)

def dedupe_in_memory(items):
    """同一轮去重（合并多来源重复）"""
    seen_local = set()
    out = []
    for it in items:
        tid = title_id(it.get("title",""))
        if tid in seen_local: 
            continue
        seen_local.add(tid)
        it["url"] = canonical_url(it.get("url",""))
        out.append(it)
    return out

def filter_new(items, window_hours=72, sim_threshold=90, use_last_push=True):
    """
    先同轮去重 → 过滤时间窗口 → 历史库精确/近似去重
    返回 (new_items, seen_after, state_after)
    """
    items = dedupe_in_memory(items)

    # 时间窗口：只保留最近 N 小时，或上次成功推送之后的新内容
    state = load_state()
    last_push_ts = state.get("last_push_ts", 0)
    cutoff_ts = max(
        (datetime.now(timezone.utc) - timedelta(hours=window_hours)).timestamp(),
        last_push_ts if use_last_push else 0
    )
    def _ts(it):
        dt = it.get("published_at")
        return dt.timestamp() if hasattr(dt, "timestamp") else 0
    items = [it for it in items if _ts(it) == 0 or _ts(it) >= cutoff_ts]  # 没时间的保留，但靠标题去重兜底

    seen = load_seen()
    old = seen.get("items", [])

    # 仅保留近 30 天指纹
    cutoff_keep = datetime.now(timezone.utc) - timedelta(days=30)
    keep = [x for x in old if x.get("ts", 0) >= cutoff_keep.timestamp()]

    seen_titles = [x.get("title","") for x in keep]
    seen_ids = {x["id"] for x in keep}

    new_items = []
    for it in items:
        tid = title_id(it.get("title",""))
        if tid in seen_ids: 
            continue
        if is_near_duplicate(it.get("title",""), seen_titles, sim_threshold):
            continue
        new_items.append(it)
        keep.append({
            "id": tid,
            "title": it.get("title",""),
            "url": canonical_url(it.get("url","")),
            "ts": time.time()
        })

    seen["items"] = keep[-8000:]  # 上限
    return new_items, seen, state
