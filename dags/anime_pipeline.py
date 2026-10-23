"""
DAG: anime_pipeline
เก็บอนิเมะยอดนิยม ~200 เรื่องจาก MyAnimeList, AniList, Kitsu
-> แปลงเป็นสคีมาเดียวกัน -> ตรวจคุณภาพ -> โหลดลง Postgres (animedb) -> รวมข้อมูลข้ามเว็บ

โครงสร้าง (ดูใน Graph view):
  [mal: extract(x4 หน้า) -> transform] ┐
  [anilist: extract(x4 หน้า) -> transform] ├-> validate -> check_quality (branch) ┬-> load_raw ──────┬-> merge
  [kitsu: extract -> transform]        ┘                                       └-> quarantine_rows ┘
"""
import logging
import os
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import get_current_context
from airflow.utils.task_group import TaskGroup

from anime_lib import db, transform
from anime_lib.extract import fetch_anilist_page, fetch_kitsu, fetch_mal_page

log = logging.getLogger(__name__)

TZ = "Asia/Bangkok"
MAL_OFFSETS = [0, 50, 100, 150]   # 4 หน้า x 50 = 200 เรื่อง
ANILIST_PAGES = [1, 2, 3, 4]      # 4 หน้า x 50 = 200 เรื่อง

# ตั้งค่าสำหรับ task extract: retry 2 ครั้ง ถ้ายังพังให้ skip (ไม่ทำให้ทั้ง DAG ล้ม)
EXTRACT_ARGS = {"retries": 2, "retry_delay": timedelta(seconds=20)}


def run_extract(source, fetch_fn):
    """
    เรียก fetch_fn() ถ้า error:
      - ยังเหลือรอบ retry -> raise ต่อให้ Airflow retry
      - เป็นรอบสุดท้ายแล้ว -> บันทึก failed ลง pipeline_runs แล้ว raise AirflowSkipException
    """
    ctx = get_current_context()
    ti = ctx["ti"]
    try:
        return fetch_fn()
    except Exception as exc:
        if ti.try_number <= ti.max_tries:
            log.warning("%s ดึงข้อมูลไม่สำเร็จ (ครั้งที่ %s) จะ retry: %s", source, ti.try_number, exc)
            raise
        log.error("%s ล้มเหลวครบทุกรอบ retry แล้ว -> skip source นี้: %s", source, exc)
        db.log_run(ctx["run_id"], source, "failed", error_message=str(exc)[:1000],
                   started_at=ctx["dag_run"].start_date)
        raise AirflowSkipException(f"{source} ล้มเหลว: {exc}")


def finish_transform(source, rows):
    """บันทึกว่า source นี้ดึง+แปลงสำเร็จ แล้วคืนแถวให้ task ถัดไป"""
    ctx = get_current_context()
    db.log_run(ctx["run_id"], source, "success", rows_fetched=len(rows),
               started_at=ctx["dag_run"].start_date)
    log.info("%s แปลงข้อมูลได้ %s แถว", source, len(rows))
    return rows


def flatten(pages):
    """รวมผลจาก mapped tasks (list ของ list) เป็น list เดียว ข้ามหน้าที่ถูก skip"""
    return [item for page in pages if page for item in page]


@dag(
    dag_id="anime_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 10, 1, tz=TZ),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "student"},
    tags=["anime", "student-project"],
    doc_md=__doc__,
)
def anime_pipeline():

    # ---------------- TaskGroup: MyAnimeList (Web scraping) ----------------
    with TaskGroup("mal", tooltip="MyAnimeList - web scraping"):

        # max_active_tis_per_dag=1 -> ดึงทีละหน้า ไม่ยิงพร้อมกัน (เคารพ rate limit)
        @task(**EXTRACT_ARGS, max_active_tis_per_dag=1)
        def extract(offset: int):
            return run_extract("mal", lambda: fetch_mal_page(offset))

        @task(task_id="transform", trigger_rule="none_failed_min_one_success")
        def transform_rows(pages):
            return finish_transform("mal", transform.transform_mal(flatten(pages)))

        mal_rows = transform_rows(extract.expand(offset=MAL_OFFSETS))

    # ---------------- TaskGroup: AniList (GraphQL API) ----------------
    with TaskGroup("anilist", tooltip="AniList - GraphQL API"):

        @task(**EXTRACT_ARGS, max_active_tis_per_dag=1)
        def extract(page: int):
            return run_extract("anilist", lambda: fetch_anilist_page(page))

        @task(task_id="transform", trigger_rule="none_failed_min_one_success")
        def transform_rows(pages):
            return finish_transform("anilist", transform.transform_anilist(flatten(pages)))

        anilist_rows = transform_rows(extract.expand(page=ANILIST_PAGES))

    # ---------------- TaskGroup: Kitsu (REST API) ----------------
    with TaskGroup("kitsu", tooltip="Kitsu - REST API"):

        @task(**EXTRACT_ARGS)
        def extract():
            return run_extract("kitsu", lambda: fetch_kitsu(total=200))

        @task(task_id="transform")
        def transform_rows(items):
            return finish_transform("kitsu", transform.transform_kitsu(items))

        kitsu_rows = transform_rows(extract())

    # ---------------- Data Quality ----------------
    @task(trigger_rule="none_failed_min_one_success")
    def validate(mal, anilist, kitsu):
        """รวมแถวจากทุก source ที่สำเร็จ แล้วแยกเป็น good / bad"""
        rows = (mal or []) + (anilist or []) + (kitsu or [])

        # โหมดสาธิต: ใส่แถวเสียเข้าไปเพื่อให้เห็นการทำงานของ quarantine
        if os.getenv("INJECT_BAD_ROWS", "").lower() in ("1", "true", "yes"):
            rows.append({"source": "demo", "source_id": "bad-1", "title": None, "score_10": 7.5,
                         "episodes": 12, "year": 2020})
            rows.append({"source": "demo", "source_id": "bad-2", "title": "Broken Anime",
                         "score_10": 15.0, "episodes": -3, "year": 1900})

        next_year = pendulum.now(TZ).year + 1
        good, bad = transform.split_good_bad(rows, next_year)
        log.info("DQ: ผ่าน %s แถว / ไม่ผ่าน %s แถว", len(good), len(bad))
        return {"good": good, "bad": bad}

    @task.branch
    def check_quality(result):
        """เลือกเส้นทาง: มีแถวดี -> load_raw, มีแถวเสีย -> quarantine_rows (เป็นไปได้ทั้งสองทาง)"""
        branches = []
        if result["good"]:
            branches.append("load_raw")
        if result["bad"]:
            branches.append("quarantine_rows")
        return branches

    @task
    def load_raw(result):
        """UPSERT ลง raw_anime และ score_history แล้วคืนรายชื่อ source ที่โหลดสำเร็จ"""
        ctx = get_current_context()
        good = result["good"]
        now = pendulum.now(TZ)
        db.upsert_raw_anime(good, fetched_at=now)
        db.upsert_score_history(good, snapshot_date=now.date())

        sources = sorted({r["source"] for r in good})
        for source in sources:
            count = sum(1 for r in good if r["source"] == source)
            db.set_rows_loaded(ctx["run_id"], source, count)
        log.info("โหลด %s แถว จาก %s", len(good), sources)
        return sources

    @task
    def quarantine_rows(result):
        n = db.insert_quarantine(result["bad"], run_date=pendulum.now(TZ).date())
        log.warning("ย้าย %s แถวไป quarantine", n)

    @task(trigger_rule="none_failed_min_one_success")
    def merge():
        """รวมข้อมูลข้ามเว็บลง anime_merged (เฉพาะ source ที่โหลดสำเร็จในรอบนี้)"""
        ti = get_current_context()["ti"]
        sources = ti.xcom_pull(task_ids="load_raw")
        if not sources:
            raise AirflowSkipException("รอบนี้ไม่มี source ใดโหลดสำเร็จ จึงไม่ merge")
        rows = db.fetch_latest_raw(sources)
        merged = transform.merge_rows(rows)
        n = db.replace_merged(merged)
        log.info("merge จาก %s -> anime_merged %s เรื่อง", sources, n)

    # ---------------- เชื่อม task ----------------
    result = validate(mal_rows, anilist_rows, kitsu_rows)
    branch = check_quality(result)
    loaded = load_raw(result)
    quarantined = quarantine_rows(result)
    branch >> [loaded, quarantined]
    [loaded, quarantined] >> merge()


anime_pipeline()
