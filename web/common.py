"""ฟังก์ชันใช้ร่วมกันของแดชบอร์ด: ต่อฐานข้อมูล, สีประจำเว็บ, footer"""
import os

import pandas as pd
import psycopg2
import streamlit as st

# ชื่อแสดงผล + สีประจำแต่ละเว็บ (ใช้สีเดิมทุกกราฟ เพื่อให้จำได้ว่าสีไหนคือเว็บไหน)
SOURCE_LABELS = {"mal": "MyAnimeList", "anilist": "AniList", "kitsu": "Kitsu"}
SCORE_COLUMNS = {"mal": "mal_score", "anilist": "anilist_score", "kitsu": "kitsu_score"}
SOURCE_COLORS = {"MyAnimeList": "#2a78d6", "AniList": "#eb6834", "Kitsu": "#1baf7a"}
SINGLE_COLOR = "#2a78d6"


@st.cache_data(ttl=60, show_spinner=False)
def query(sql, params=None):
    """รัน SQL กับฐานข้อมูล animedb แล้วคืนเป็น DataFrame (cache 60 วินาที)"""
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5433")),
        user=os.getenv("DB_USER", "anime"),
        password=os.getenv("DB_PASSWORD", ""),
        dbname=os.getenv("DB_NAME", "animedb"),
    )
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            columns = [c.name for c in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=columns)
    finally:
        conn.close()


def footer():
    st.divider()
    st.caption(
        "แหล่งข้อมูล: [MyAnimeList](https://myanimelist.net) · "
        "[AniList](https://anilist.co) · [Kitsu](https://kitsu.app) — "
        "ลิขสิทธิ์ข้อมูลเป็นของเว็บไซต์ต้นทาง · **จัดทำเพื่อการศึกษา**"
    )
