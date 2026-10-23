"""
ส่วน Extract: ดึงข้อมูลอนิเมะยอดนิยมจาก 3 เว็บ
- MyAnimeList : Web scraping ด้วย requests + BeautifulSoup (เฉพาะหน้า Top Anime)
- AniList     : GraphQL API
- Kitsu       : REST API (JSON:API)

ฟังก์ชันในไฟล์นี้คืนค่าเป็น "ข้อมูลดิบ" (list ของ dict) ยังไม่แปลงหน่วย
การแปลงเป็นสคีมาเดียวกันทำใน transform.py
"""
import logging
import os
import re
import time

import requests
from bs4 import BeautifulSoup

log = logging.getLogger(__name__)

# ระบุตัวตนให้ชัดเจนว่าเป็นโปรเจกต์นักศึกษา (ไม่ปลอมเป็นเบราว์เซอร์)
USER_AGENT = "StudentProject-AnimePipeline/1.0 (educational)"
TIMEOUT = 30  # วินาที

MAL_URL = "https://myanimelist.net/topanime.php"
ANILIST_URL = "https://graphql.anilist.co"
KITSU_URL = "https://kitsu.app/api/edge/anime"

MAL_SLEEP = 3      # หน่วงระหว่างหน้า MAL (วินาที)
API_SLEEP = 2      # หน่วงระหว่างคำขอ AniList / Kitsu (วินาที)


def check_break(source):
    """ถ้าตั้ง BREAK_SOURCE=<source> ไว้ ให้จงใจ error เพื่อสาธิตกรณีเว็บล่ม"""
    if os.getenv("BREAK_SOURCE", "").strip().lower() == source:
        raise RuntimeError(f"จำลองเว็บ {source} ล่ม (BREAK_SOURCE={source})")


# ------------------------------------------------------------------
# 1) MyAnimeList (Web scraping)
# ------------------------------------------------------------------
def fetch_mal_page(offset):
    """ดึงหน้า Top Anime ของ MAL 1 หน้า (50 เรื่อง) แล้ว parse เป็น list ของ dict"""
    check_break("mal")
    if offset > 0:
        time.sleep(MAL_SLEEP)  # rate limit: หน่วง 3 วินาทีก่อนดึงหน้าถัดไป

    resp = requests.get(
        MAL_URL,
        params={"limit": offset},
        headers={"User-Agent": USER_AGENT},
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    items = parse_mal_html(resp.text)
    if not items:
        # ถ้า parse ไม่ได้เลย แปลว่าโครงสร้าง HTML อาจเปลี่ยน ให้ถือว่า error
        raise ValueError(f"MAL offset={offset}: ไม่พบแถว tr.ranking-list ในหน้าเว็บ")
    log.info("MAL offset=%s ได้ %s เรื่อง", offset, len(items))
    return items


def parse_mal_html(html):
    """
    แยกข้อมูลจาก HTML ของหน้า topanime.php (ตรวจโครงสร้างจริงแล้ว):
      <tr class="ranking-list">
        <td class="rank"><span class="top-anime-rank-text">1</span></td>
        <td class="title"> ... <h3 class="anime_ranking_h3"><a href=".../anime/52991/...">ชื่อ</a></h3>
            <div class="information">TV (28 eps)<br>Sep 2023 - Mar 2024<br>1,534,699 members</div>
        <td class="score"> ... <span class="score-label">9.24</span>
    """
    soup = BeautifulSoup(html, "lxml")
    items = []
    for row in soup.select("tr.ranking-list"):
        link = row.select_one("h3.anime_ranking_h3 a")
        if link is None:
            continue
        match = re.search(r"/anime/(\d+)", link.get("href", ""))
        rank_el = row.select_one("td.rank span")
        score_el = row.select_one("td.score span.score-label")
        info_el = row.select_one("div.information")

        # div.information มี 3 บรรทัด: ประเภท(ตอน) / ช่วงออกอากาศ / จำนวนสมาชิก
        lines = []
        if info_el is not None:
            lines = [t.strip() for t in info_el.get_text("\n").split("\n") if t.strip()]
        lines += [""] * (3 - len(lines))

        items.append({
            "mal_id": match.group(1) if match else None,
            "title": link.get_text(strip=True),
            "rank": rank_el.get_text(strip=True) if rank_el else None,
            "score": score_el.get_text(strip=True) if score_el else None,
            "type_eps": lines[0],   # เช่น "TV (28 eps)"
            "aired": lines[1],      # เช่น "Sep 2023 - Mar 2024"
            "members": lines[2],    # เช่น "1,534,699 members"
        })
    return items


# ------------------------------------------------------------------
# 2) AniList (GraphQL API)
# ------------------------------------------------------------------
ANILIST_QUERY = """
query ($page: Int) {
  Page(page: $page, perPage: 50) {
    media(type: ANIME, sort: SCORE_DESC) {
      id idMal title { romaji english } averageScore popularity
      format episodes genres seasonYear startDate { year } status
    }
  }
}
"""


def fetch_anilist_page(page):
    """ดึง AniList 1 หน้า (50 เรื่อง) รองรับ HTTP 429 ด้วย Retry-After"""
    check_break("anilist")
    if page > 1:
        time.sleep(API_SLEEP)

    for attempt in range(3):
        resp = requests.post(
            ANILIST_URL,
            json={"query": ANILIST_QUERY, "variables": {"page": page}},
            headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
            timeout=TIMEOUT,
        )
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 60))
            log.warning("AniList ตอบ 429 (ถูกจำกัดความถี่) รอ %s วินาที", wait)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        media = resp.json()["data"]["Page"]["media"]
        # เก็บลำดับในรายการไว้เป็น rank
        for i, m in enumerate(media):
            m["rank"] = (page - 1) * 50 + i + 1
        log.info("AniList page=%s ได้ %s เรื่อง", page, len(media))
        return media
    raise RuntimeError(f"AniList page={page}: โดน 429 ซ้ำหลายครั้ง")


# ------------------------------------------------------------------
# 3) Kitsu (REST API)
# ------------------------------------------------------------------
def fetch_kitsu(total=200, per_page=20):
    """ดึง Kitsu เรียงตามคะแนน ทีละ 20 เรื่อง (สูงสุดที่ API อนุญาต) จนครบ ~200 เรื่อง"""
    check_break("kitsu")
    items = []
    for offset in range(0, total, per_page):
        if offset > 0:
            time.sleep(API_SLEEP)
        resp = requests.get(
            KITSU_URL,
            params={
                "sort": "-averageRating",
                "page[limit]": per_page,
                "page[offset]": offset,
                "include": "mappings",
            },
            headers={"User-Agent": USER_AGENT, "Accept": "application/vnd.api+json"},
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        body = resp.json()

        # หา MAL ID จาก included mappings ที่ externalSite == "myanimelist/anime"
        mal_by_mapping = {
            inc["id"]: inc["attributes"]["externalId"]
            for inc in body.get("included", [])
            if inc.get("type") == "mappings"
            and inc["attributes"].get("externalSite") == "myanimelist/anime"
        }

        for i, anime in enumerate(body.get("data", [])):
            attrs = anime["attributes"]
            mapping_refs = anime.get("relationships", {}).get("mappings", {}).get("data") or []
            mal_id = next((mal_by_mapping[m["id"]] for m in mapping_refs if m["id"] in mal_by_mapping), None)
            items.append({
                "id": anime["id"],
                "canonicalTitle": attrs.get("canonicalTitle"),
                "averageRating": attrs.get("averageRating"),
                "userCount": attrs.get("userCount"),
                "subtype": attrs.get("subtype"),
                "episodeCount": attrs.get("episodeCount"),
                "startDate": attrs.get("startDate"),
                "status": attrs.get("status"),
                "rank": offset + i + 1,
                "mal_id": mal_id,
            })
        log.info("Kitsu offset=%s รวมแล้ว %s เรื่อง", offset, len(items))
    return items
