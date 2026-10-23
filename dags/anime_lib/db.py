"""
ส่วนติดต่อฐานข้อมูล animedb ผ่าน Airflow Connection (conn_id = "animedb")
Connection นี้ตั้งจาก environment variable AIRFLOW_CONN_ANIMEDB ใน docker-compose
"""
from contextlib import contextmanager

from psycopg2.extras import Json, execute_values

CONN_ID = "animedb"

RAW_COLUMNS = ["source", "source_id", "mal_id", "title", "title_norm", "score_10", "rank",
               "popularity", "anime_type", "episodes", "genres", "year", "status"]

MERGED_COLUMNS = ["merge_key", "mal_id", "title", "anime_type", "episodes", "year", "genres",
                  "mal_score", "anilist_score", "kitsu_score", "avg_score", "score_diff",
                  "sources", "source_count", "source_keys"]


def get_conn():
    # import ในฟังก์ชัน เพื่อให้ Airflow parse DAG ได้เร็ว
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    return PostgresHook(postgres_conn_id=CONN_ID).get_conn()


@contextmanager
def db_cursor():
    """เปิด connection + cursor แล้ว commit อัตโนมัติ (rollback ถ้า error) และปิด connection เสมอ"""
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            yield cur
    finally:
        conn.close()


def upsert_raw_anime(rows, fetched_at):
    """UPSERT ลง raw_anime: ถ้า (source, source_id) มีอยู่แล้วให้อัปเดตแทนการเพิ่มแถวใหม่"""
    if not rows:
        return 0
    values = [[r[c] for c in RAW_COLUMNS] + [fetched_at] for r in rows]
    update_cols = ", ".join(f"{c} = EXCLUDED.{c}" for c in RAW_COLUMNS[2:] + ["fetched_at"])
    sql = f"""
        INSERT INTO raw_anime ({", ".join(RAW_COLUMNS)}, fetched_at) VALUES %s
        ON CONFLICT (source, source_id) DO UPDATE SET {update_cols}
    """
    with db_cursor() as cur:
        execute_values(cur, sql, values)
    return len(rows)


def upsert_score_history(rows, snapshot_date):
    """UPSERT ลง score_history: วันเดียวกันรันซ้ำกี่ครั้งก็มี 1 แถวต่อเรื่องต่อเว็บ"""
    if not rows:
        return 0
    values = [(r["source"], r["source_id"], snapshot_date, r["score_10"], r["rank"], r["popularity"])
              for r in rows]
    sql = """
        INSERT INTO score_history (source, source_id, snapshot_date, score_10, rank, popularity)
        VALUES %s
        ON CONFLICT (source, source_id, snapshot_date) DO UPDATE SET
            score_10 = EXCLUDED.score_10, rank = EXCLUDED.rank, popularity = EXCLUDED.popularity
    """
    with db_cursor() as cur:
        execute_values(cur, sql, values)
    return len(rows)


def insert_quarantine(bad_rows, run_date):
    """เก็บแถวที่ไม่ผ่าน DQ (แถวเดิมในวันเดียวกันจะไม่ถูกเพิ่มซ้ำ)"""
    if not bad_rows:
        return 0
    values = [(b["source"], Json(b["payload"]), b["reason"], run_date) for b in bad_rows]
    sql = """
        INSERT INTO quarantine (source, payload, reason, run_date) VALUES %s
        ON CONFLICT DO NOTHING
    """
    with db_cursor() as cur:
        execute_values(cur, sql, values)
    return len(bad_rows)


def log_run(dag_run_id, source, status, rows_fetched=0, error_message=None, started_at=None):
    """บันทึกผลการรันของแต่ละ source ลง pipeline_runs (รันซ้ำ dag_run เดิม = อัปเดตแถวเดิม)"""
    sql = """
        INSERT INTO pipeline_runs (dag_run_id, source, status, rows_fetched, error_message,
                                   started_at, finished_at)
        VALUES (%s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (dag_run_id, source) DO UPDATE SET
            status = EXCLUDED.status, rows_fetched = EXCLUDED.rows_fetched,
            error_message = EXCLUDED.error_message, started_at = EXCLUDED.started_at,
            finished_at = now()
    """
    with db_cursor() as cur:
        cur.execute(sql, (dag_run_id, source, status, rows_fetched, error_message, started_at))


def set_rows_loaded(dag_run_id, source, rows_loaded):
    with db_cursor() as cur:
        cur.execute(
            "UPDATE pipeline_runs SET rows_loaded = %s, finished_at = now() "
            "WHERE dag_run_id = %s AND source = %s",
            (rows_loaded, dag_run_id, source),
        )


def fetch_latest_raw(sources):
    """ดึงแถวล่าสุดของแต่ละ source (ชุดที่โหลดในรอบล่าสุด) เพื่อนำไป merge"""
    sql = f"""
        SELECT {", ".join(RAW_COLUMNS)}
        FROM raw_anime r
        WHERE r.source = ANY(%s)
          AND r.fetched_at = (SELECT max(fetched_at) FROM raw_anime r2 WHERE r2.source = r.source)
    """
    with db_cursor() as cur:
        cur.execute(sql, (list(sources),))
        rows = [dict(zip(RAW_COLUMNS, rec)) for rec in cur.fetchall()]
    for r in rows:  # NUMERIC -> Decimal ให้เป็น float เพื่อคำนวณง่าย
        if r["score_10"] is not None:
            r["score_10"] = float(r["score_10"])
    return rows


def replace_merged(merged_rows):
    """
    เขียนผล merge แบบ idempotent:
      - UPSERT ตาม merge_key (เรื่องเดิมอัปเดต เรื่องใหม่เพิ่ม)
      - ลบเรื่องที่ไม่อยู่ในผลรอบนี้แล้ว
    ทำใน transaction เดียว รันซ้ำกี่ครั้งผลก็เหมือนเดิม
    """
    values = [[m[c] for c in MERGED_COLUMNS] for m in merged_rows]
    update_cols = ", ".join(f"{c} = EXCLUDED.{c}" for c in MERGED_COLUMNS[1:])
    keys = [m["merge_key"] for m in merged_rows]
    with db_cursor() as cur:
        # ลบก่อน เพื่อไม่ให้ชน UNIQUE(mal_id) กับแถวเก่าที่ merge_key ต่างกัน
        cur.execute("DELETE FROM anime_merged WHERE NOT (merge_key = ANY(%s))", (keys,))
        if values:
            execute_values(cur, f"""
                INSERT INTO anime_merged ({", ".join(MERGED_COLUMNS)}) VALUES %s
                ON CONFLICT (merge_key) DO UPDATE SET {update_cols}, updated_at = now()
            """, values)
    return len(values)

