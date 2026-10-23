-- =============================================================
-- สคีมาฐานข้อมูลโปรเจกต์ (animedb)
-- ไฟล์นี้จะถูกรันอัตโนมัติครั้งแรกที่สร้างคอนเทนเนอร์ Postgres
-- (mount ไว้ที่ /docker-entrypoint-initdb.d)
-- =============================================================

-- ข้อมูลดิบจากแต่ละเว็บ (หลังแปลงเป็นสคีมาเดียวกันแล้ว)
CREATE TABLE IF NOT EXISTS raw_anime (
    source      TEXT        NOT NULL,          -- mal / anilist / kitsu
    source_id   TEXT        NOT NULL,          -- id ของเว็บต้นทาง
    mal_id      INTEGER,                       -- MAL ID ใช้จับคู่ข้ามเว็บ
    title       TEXT        NOT NULL,
    title_norm  TEXT,                          -- ชื่อที่ทำให้เป็นมาตรฐาน (ใช้ dedup)
    score_10    NUMERIC(4,2),                  -- คะแนนสเกล 0-10
    rank        INTEGER,
    popularity  INTEGER,                       -- members / popularity / userCount
    anime_type  TEXT,
    episodes    INTEGER,
    genres      TEXT[],
    year        INTEGER,
    status      TEXT,
    fetched_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source, source_id)
);
CREATE INDEX IF NOT EXISTS idx_raw_anime_mal_id ON raw_anime (mal_id);

-- ข้อมูลที่รวมแล้ว 1 แถวต่อ 1 เรื่อง
CREATE TABLE IF NOT EXISTS anime_merged (
    merged_id     SERIAL PRIMARY KEY,
    merge_key     TEXT UNIQUE NOT NULL,        -- "mal:<id>" หรือ "title:<title_norm>|<year>"
    mal_id        INTEGER UNIQUE NULL,
    title         TEXT NOT NULL,
    anime_type    TEXT,
    episodes      INTEGER,
    year          INTEGER,
    genres        TEXT[],
    mal_score     NUMERIC(4,2),
    anilist_score NUMERIC(4,2),
    kitsu_score   NUMERIC(4,2),
    avg_score     NUMERIC(4,2),
    score_diff    NUMERIC(4,2),                -- max - min ของคะแนนจากทุกเว็บ
    sources       TEXT[],
    source_count  INTEGER,
    source_keys   TEXT[],                      -- เช่น {"mal:52991","kitsu:48323"} ใช้ดูประวัติคะแนน
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ประวัติคะแนนรายวัน (1 แถวต่อเรื่องต่อเว็บต่อวัน)
CREATE TABLE IF NOT EXISTS score_history (
    source        TEXT NOT NULL,
    source_id     TEXT NOT NULL,
    snapshot_date DATE NOT NULL,
    score_10      NUMERIC(4,2),
    rank          INTEGER,
    popularity    INTEGER,
    PRIMARY KEY (source, source_id, snapshot_date)
);

-- แถวที่ไม่ผ่าน Data Quality Check
CREATE TABLE IF NOT EXISTS quarantine (
    id          SERIAL PRIMARY KEY,
    source      TEXT,
    payload     JSONB NOT NULL,
    reason      TEXT NOT NULL,
    run_date    DATE NOT NULL DEFAULT CURRENT_DATE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- กันไม่ให้แถวเสียแถวเดิมถูกบันทึกซ้ำเมื่อรัน DAG ซ้ำในวันเดียวกัน
CREATE UNIQUE INDEX IF NOT EXISTS uq_quarantine_daily
    ON quarantine (source, run_date, md5(payload::text));

-- บันทึกการรันของแต่ละ source
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id        SERIAL PRIMARY KEY,
    dag_run_id    TEXT NOT NULL,
    source        TEXT NOT NULL,
    status        TEXT NOT NULL,               -- success / failed
    rows_fetched  INTEGER DEFAULT 0,
    rows_loaded   INTEGER DEFAULT 0,
    error_message TEXT,
    started_at    TIMESTAMPTZ,
    finished_at   TIMESTAMPTZ,
    UNIQUE (dag_run_id, source)
);
