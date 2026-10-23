"""
ส่วน Transform: แปลงข้อมูลดิบของแต่ละเว็บให้อยู่ในสคีมาเดียวกัน (ตาราง raw_anime)
และฟังก์ชันตรวจคุณภาพข้อมูล (Data Quality) กับการรวมข้อมูลข้ามเว็บ (merge/dedup)

สคีมากลาง 1 แถว:
  source, source_id, mal_id, title, title_norm, score_10, rank, popularity,
  anime_type, episodes, genres, year, status
"""
import re
import unicodedata

SOURCES = ["mal", "anilist", "kitsu"]  # ลำดับความสำคัญเวลาเลือกชื่อเรื่อง/ประเภท

# ตารางแปลงชื่อประเภทให้เป็นมาตรฐานเดียวกัน
TYPE_MAP = {
    "tv": "TV",
    "tv_short": "TV",
    "movie": "Movie",
    "ova": "OVA",
    "ona": "ONA",
    "special": "Special",
    "tv special": "Special",
    "tv_special": "Special",
    "music": "Music",
}

# ตารางแปลงสถานะให้เป็นมาตรฐานเดียวกัน
STATUS_MAP = {
    "finished": "Finished",
    "current": "Airing",
    "releasing": "Airing",
    "upcoming": "Upcoming",
    "not_yet_released": "Upcoming",
    "unreleased": "Upcoming",
    "tba": "Upcoming",
    "cancelled": "Cancelled",
    "hiatus": "Hiatus",
}


# ------------------------------------------------------------------
# ฟังก์ชันช่วยแปลงค่า
# ------------------------------------------------------------------
def normalize_title(title):
    """ตัวพิมพ์เล็ก ตัดเครื่องหมาย/สัญลักษณ์ และช่องว่างซ้ำ เช่น 'Gintama°' -> 'gintama'"""
    if not title:
        return None
    text = unicodedata.normalize("NFKC", title).lower()
    text = re.sub(r"[^\w\s]|_", " ", text)
    return re.sub(r"\s+", " ", text).strip() or None


def normalize_type(value):
    if not value:
        return None
    key = value.strip().lower()
    return TYPE_MAP.get(key, value.strip())


def normalize_status(value):
    if not value:
        return None
    return STATUS_MAP.get(value.strip().lower(), value.strip().title())


def to_int(value):
    """แปลงเป็น int ถ้าแปลงไม่ได้ (เช่น '?', None) คืน None"""
    try:
        return int(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def to_score_10(value, divide_by=1):
    """แปลงคะแนนให้เป็นสเกล 0-10 ทศนิยม 2 ตำแหน่ง (AniList/Kitsu ต้องหาร 10)"""
    try:
        return round(float(value) / divide_by, 2)
    except (TypeError, ValueError):
        return None


def first_year(text):
    """หาเลขปี 4 หลักตัวแรกในข้อความ เช่น 'Sep 2023 - Mar 2024' -> 2023"""
    match = re.search(r"(19|20)\d{2}", text or "")
    return int(match.group(0)) if match else None


def make_row(source, source_id, mal_id, title, score_10, rank, popularity,
             anime_type, episodes, genres, year, status):
    return {
        "source": source,
        "source_id": str(source_id) if source_id is not None else None,
        "mal_id": to_int(mal_id),
        "title": title,
        "title_norm": normalize_title(title),
        "score_10": score_10,
        "rank": to_int(rank),
        "popularity": to_int(popularity),
        "anime_type": normalize_type(anime_type),
        "episodes": to_int(episodes),
        "genres": genres or None,
        "year": year,
        "status": status,
    }


# ------------------------------------------------------------------
# แปลงข้อมูลของแต่ละเว็บ
# ------------------------------------------------------------------
def transform_mal(items):
    rows = []
    for it in items:
        # "TV (28 eps)" -> type="TV", episodes=28 / "TV Special (2 eps)" -> "TV Special"
        m = re.match(r"(.+?)\s*\((\?|\d+) eps?\)", it.get("type_eps") or "")
        anime_type = m.group(1) if m else it.get("type_eps")
        episodes = m.group(2) if m else None
        aired = it.get("aired") or ""
        # MAL หน้า Top ไม่มีสถานะโดยตรง: ถ้าช่วงออกอากาศลงท้ายด้วย " - " แปลว่ายังฉายอยู่
        status = "Airing" if aired.strip().endswith("-") else "Finished"
        rows.append(make_row(
            source="mal",
            source_id=it.get("mal_id"),
            mal_id=it.get("mal_id"),
            title=it.get("title"),
            score_10=to_score_10(it.get("score")),          # MAL เป็นสเกล 10 อยู่แล้ว
            rank=it.get("rank"),
            popularity=re.sub(r"[^\d]", "", it.get("members") or ""),
            anime_type=anime_type,
            episodes=episodes,
            genres=None,                                    # หน้า Top ไม่มีแนวเรื่อง
            year=first_year(aired),
            status=status,
        ))
    return rows


def transform_anilist(items):
    rows = []
    for it in items:
        title = it.get("title") or {}
        rows.append(make_row(
            source="anilist",
            source_id=it.get("id"),
            mal_id=it.get("idMal"),
            title=title.get("romaji") or title.get("english"),
            score_10=to_score_10(it.get("averageScore"), divide_by=10),  # 0-100 -> 0-10
            rank=it.get("rank"),
            popularity=it.get("popularity"),
            anime_type=it.get("format"),
            episodes=it.get("episodes"),
            genres=it.get("genres"),
            year=it.get("seasonYear") or (it.get("startDate") or {}).get("year"),  # บางเรื่องไม่มี seasonYear
            status=normalize_status(it.get("status")),
        ))
    return rows


def transform_kitsu(items):
    rows = []
    for it in items:
        rows.append(make_row(
            source="kitsu",
            source_id=it.get("id"),
            mal_id=it.get("mal_id"),
            title=it.get("canonicalTitle"),
            score_10=to_score_10(it.get("averageRating"), divide_by=10),  # "88.89" -> 8.89
            rank=it.get("rank"),
            popularity=it.get("userCount"),
            anime_type=it.get("subtype"),
            episodes=it.get("episodeCount"),
            genres=None,                                    # Kitsu แยกแนวเรื่องไว้อีก endpoint
            year=first_year(it.get("startDate")),
            status=normalize_status(it.get("status")),
        ))
    return rows


# ------------------------------------------------------------------
# Data Quality Check
# ------------------------------------------------------------------
def check_row(row, next_year):
    """คืน list ของเหตุผลที่แถวนี้ไม่ผ่าน (list ว่าง = ผ่าน)"""
    errors = []
    if not row.get("source_id"):
        errors.append("source_id เป็นค่าว่าง")
    if not row.get("title"):
        errors.append("title เป็นค่าว่าง")
    score = row.get("score_10")
    if score is not None and not (0 <= score <= 10):
        errors.append(f"score_10={score} ไม่อยู่ในช่วง 0-10")
    episodes = row.get("episodes")
    if episodes is not None and episodes < 0:
        errors.append(f"episodes={episodes} ติดลบ")
    year = row.get("year")
    if year is None or not (1950 <= year <= next_year):
        errors.append(f"year={year} ไม่อยู่ในช่วง 1950-{next_year}")
    return errors


def split_good_bad(rows, next_year):
    """แยกแถวที่ผ่าน/ไม่ผ่าน และตัดแถวซ้ำ (source, source_id) ภายในรอบเดียวกัน"""
    good, bad, seen = [], [], set()
    for row in rows:
        errors = check_row(row, next_year)
        if errors:
            bad.append({"source": row.get("source"), "payload": row, "reason": "; ".join(errors)})
            continue
        key = (row["source"], row["source_id"])
        if key in seen:  # เช่น MAL อันดับขยับระหว่างดึงหน้า ทำให้เรื่องเดียวอยู่ 2 หน้า
            continue
        seen.add(key)
        good.append(row)
    return good, bad


# ------------------------------------------------------------------
# Merge / Deduplication ข้ามเว็บ
# ------------------------------------------------------------------
def _first(rows, field):
    """คืนค่าแรกที่ไม่ว่าง ตามลำดับความสำคัญของเว็บ"""
    for r in rows:
        if r.get(field) not in (None, [], ""):
            return r[field]
    return None


def merge_rows(rows):
    """
    รวมแถวจากหลายเว็บให้เหลือ 1 แถวต่อ 1 เรื่อง
      1) จับคู่ด้วย mal_id ก่อน
      2) ถ้าไม่มี mal_id ใช้ title_norm + year
    คืน list ของ dict พร้อมบันทึกลง anime_merged
    """
    # เรียงตามลำดับเว็บ แล้วตามอันดับ เพื่อให้แถวที่ดีที่สุดมาก่อน
    rows = sorted(rows, key=lambda r: (SOURCES.index(r["source"]), r.get("rank") or 10**9))
    groups = {}       # merge_key -> {source: row}
    title_index = {}  # (title_norm, year) -> merge_key

    def add(key, row):
        groups.setdefault(key, {}).setdefault(row["source"], row)  # เว็บละ 1 แถวต่อเรื่อง

    # รอบที่ 1: แถวที่มี mal_id
    for row in rows:
        if row.get("mal_id"):
            key = f"mal:{row['mal_id']}"
            add(key, row)
            title_index.setdefault((row["title_norm"], row["year"]), key)

    # รอบที่ 2: แถวที่ไม่มี mal_id -> ใช้ชื่อ + ปี
    for row in rows:
        if not row.get("mal_id"):
            title_key = (row["title_norm"], row["year"])
            key = title_index.get(title_key) or f"title:{row['title_norm']}|{row['year']}"
            title_index.setdefault(title_key, key)
            add(key, row)

    merged = []
    for key, by_source in groups.items():
        ordered = [by_source[s] for s in SOURCES if s in by_source]
        scores = {s: by_source[s]["score_10"] if s in by_source else None for s in SOURCES}
        values = [v for v in scores.values() if v is not None]
        merged.append({
            "merge_key": key,
            "mal_id": int(key[4:]) if key.startswith("mal:") else None,
            "title": _first(ordered, "title"),
            "anime_type": _first(ordered, "anime_type"),
            "episodes": _first(ordered, "episodes"),
            "year": _first(ordered, "year"),
            "genres": _first(ordered, "genres"),
            "mal_score": scores["mal"],
            "anilist_score": scores["anilist"],
            "kitsu_score": scores["kitsu"],
            "avg_score": round(sum(values) / len(values), 2) if values else None,
            "score_diff": round(max(values) - min(values), 2) if len(values) >= 2 else None,
            "sources": [r["source"] for r in ordered],
            "source_count": len(ordered),
            "source_keys": [f"{r['source']}:{r['source_id']}" for r in ordered],
        })
    return merged
