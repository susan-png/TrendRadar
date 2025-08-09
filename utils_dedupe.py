import json, re, hashlib, time, os
from datetime import datetime, timedelta, timezone
try:
    from rapidfuzz import fuzz
except Exception:
    # 允许在本地未装库时导入失败（CI 会安装）
    class _F:
        @staticmethod
        def ratio(a, b): return 0
    fuzz = _F()

SEEN_PATH = "data/seen.json"

def _normalize_title(t: str) -> str:
    if not t: return ""
    t = t.lower()
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"[【】\[\]（）()，、。.!?！？:：;；\-—_“”\"'’·•]", " ", t)
    t = t.strip()
    return t

def canonical_url(u: str) -> str:
    if not u: return ""
    u = re.sub(r"([?&])(utm_[^=&]+|spm|from|ref|source|src)=[^&#]*", r"\1", u, flags=re.I)
    u = re.sub(r"[?&]+$", "", u)
    return u

def make_id(title: str, url: str) -> str:
    base = _normalize_title(title) + "||" + canonical_url(url or "")
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

def load_seen():
    if not os.path.exists(SEEN_PATH):
        return {"items": []}
    with open(SEEN_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def save_seen(seen):
    os.makedirs(os.path.dirname(SEEN_PATH), exist_ok=True)
    with open(SEEN_PATH, "w", encoding="utf-8") as f:
        json.dump(seen, f, ensure_ascii=False, indent=2)

def is_near_duplicate(title, seen_titles, threshold=90):
    norm = _normalize_title(title)
    return any(fuzz.ratio(norm, _normalize_title(t)) >= threshold for t in seen_titles)

def filter_new(items, window_days=14, sim_threshold=90):
    seen = load_seen()
    old = seen.get("items", [])

    cutoff = datetime.now(timezone.utc) - timedelta(days=window_days)
    keep = [x for x in old if x.get("ts", 0) >= cutoff.timestamp()]

    seen_titles = [x.get("title", "") for x in keep]
    seen_ids = {x["id"] for x in keep}

    new_items = []
    for it in items:
        _id = make_id(it.get("title", ""), it.get("url", ""))
        if _id in seen_ids:
            continue
        if is_near_duplicate(it.get("title", ""), seen_titles, sim_threshold):
            continue
        new_items.append(it)
        keep.append({
            "id": _id,
            "title": it.get("title", ""),
            "url": canonical_url(it.get("url", "")),
            "ts": time.time()
        })

    seen["items"] = keep[-5000:]
    return new_items, seen
