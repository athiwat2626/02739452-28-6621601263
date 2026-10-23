# 📊 Anime Score Comparison Pipeline

## หัวข้อโปรเจกต์

**ระบบเปรียบเทียบคะแนนอนิเมะจากหลายเว็บไซต์** — เว็บรีวิวอนิเมะแต่ละเว็บให้คะแนนเรื่องเดียวกันไม่เท่ากัน
โปรเจกต์ Data Engineering นี้เก็บอนิเมะยอดนิยม Top ~200 เรื่องจาก **3 เว็บไซต์** มารวมเป็นชุดข้อมูลเดียว
เพื่อดูว่าเรื่องไหนแต่ละเว็บให้คะแนนต่างกันมากที่สุด และคะแนนเปลี่ยนแปลงอย่างไรตามเวลา

ขั้นตอน: ดึงข้อมูล → รวมเป็นชุดข้อมูลเดียวด้วย **Apache Airflow** → เก็บใน **PostgreSQL**
→ แสดงผลบน **เว็บแดชบอร์ด Streamlit**

## แหล่งข้อมูล

| แหล่งข้อมูล | วิธีเก็บ | ข้อมูลที่ใช้ | จำนวน |
|---|---|---|---|
| [MyAnimeList](https://myanimelist.net/topanime.php) | Web scraping (requests + BeautifulSoup) หน้า `topanime.php` | MAL ID, ชื่อ, อันดับ, คะแนน, ประเภท, จำนวนตอน, ปีที่ฉาย, จำนวนสมาชิก | 4 หน้า × 50 |
| [AniList](https://anilist.co) | GraphQL API `https://graphql.anilist.co` | ID, MAL ID, ชื่อ, คะแนนเฉลี่ย (0–100), ความนิยม, ประเภท, จำนวนตอน, แนวเรื่อง, ปี, สถานะ | 4 หน้า × 50 |
| [Kitsu](https://kitsu.app) | REST API `https://kitsu.app/api/edge/anime` | ID, MAL ID (จาก mappings), ชื่อ, คะแนนเฉลี่ย (0–100), จำนวนผู้ใช้, ประเภท, จำนวนตอน, วันเริ่มฉาย, สถานะ | 10 หน้า × 20 |

ไม่มี API key ใด ๆ เพราะทั้ง 3 แหล่งเปิดให้อ่านข้อมูลสาธารณะได้โดยไม่ต้องลงทะเบียน

---

## 1. วิธีติดตั้งและรัน

ต้องมี **Docker Desktop** (มี Docker Compose v2) เท่านั้น ไม่ต้องลง Python ในเครื่อง
(ถ้าต้องการรันโค้ดนอก Docker ติดตั้งไลบรารีได้ด้วย `pip install -r requirements.txt`)

```bash
# 1) สร้างไฟล์ .env จากตัวอย่าง แล้วเปิดแก้ค่า change_me_* ทุกตัวเป็นรหัสผ่านของตัวเอง
#    (.env อยู่ใน .gitignore จะไม่ถูก commit ขึ้น git)
cp .env.example .env

# 2) build และเปิดทุกบริการ (ครั้งแรกใช้เวลาหลายนาที)
docker compose up -d --build

# 3) ตรวจว่าทุกบริการทำงาน
docker compose ps
```

| บริการ | URL / พอร์ต | หมายเหตุ |
|---|---|---|
| Airflow UI | http://localhost:8080 | ผู้ใช้ `AIRFLOW_ADMIN_USER` / รหัสผ่าน `AIRFLOW_ADMIN_PASSWORD` ที่ตั้งใน `.env` |
| เว็บแดชบอร์ด | http://localhost:8501 | Streamlit (โค้ดในโฟลเดอร์ `web/`) |
| animedb (Postgres 16) | `localhost:5433` | ต่อด้วย DBeaver / psql ได้ |

> ถ้าพอร์ตชนกับโปรแกรมอื่น ให้แก้ `AIRFLOW_HOST_PORT`, `DASHBOARD_HOST_PORT`, `ANIMEDB_HOST_PORT` ใน `.env`

**สั่งรัน DAG** (DAG จะถูก pause ไว้ตอนสร้าง):

```bash
docker compose exec airflow-scheduler airflow dags unpause anime_pipeline
docker compose exec airflow-scheduler airflow dags trigger anime_pipeline
```

หรือเข้า Airflow UI → เปิดสวิตช์ `anime_pipeline` → กดปุ่ม ▶ Trigger DAG
(หนึ่งรอบใช้เวลาประมาณ 1–2 นาที เพราะมีการหน่วงเวลาระหว่างคำขอเพื่อเคารพ rate limit)

ปิดระบบ: `docker compose down` (ลบข้อมูลทั้งหมดด้วย: `docker compose down -v`)

---

## 2. สถาปัตยกรรม

```mermaid
flowchart LR
    subgraph SRC[แหล่งข้อมูล]
        MAL[MyAnimeList<br/>HTML]
        AL[AniList<br/>GraphQL]
        KT[Kitsu<br/>REST]
    end

    subgraph AF[Apache Airflow - DAG anime_pipeline]
        direction LR
        subgraph G1[TaskGroup mal]
            M1["extract ×4<br/>(.expand offset)"] --> M2[transform]
        end
        subgraph G2[TaskGroup anilist]
            A1["extract ×4<br/>(.expand page)"] --> A2[transform]
        end
        subgraph G3[TaskGroup kitsu]
            K1[extract] --> K2[transform]
        end
        V[validate] --> B{check_quality<br/>branch}
        B -->|ผ่าน| L[load_raw<br/>UPSERT]
        B -->|ไม่ผ่าน| Q[quarantine_rows]
        L --> MG[merge<br/>dedup ข้ามเว็บ]
        Q --> MG
    end

    MAL --> M1
    AL --> A1
    KT --> K1
    M2 --> V
    A2 --> V
    K2 --> V

    subgraph DB[(PostgreSQL animedb)]
        RAW[raw_anime]
        HIS[score_history]
        QT[quarantine]
        MRG[anime_merged]
        RUN[pipeline_runs]
    end

    L --> RAW & HIS
    Q --> QT
    MG --> MRG
    AF -. บันทึกผลรันราย source .-> RUN
    DB --> DASH[Streamlit Dashboard]
```

### โครงสร้างไฟล์

```
anime-pipeline/
├── docker-compose.yml        # Airflow + Postgres x2 + Streamlit
├── requirements.txt          # รายการไลบรารี Python ทั้งหมดของโปรเจกต์
├── .env.example              # ตัวอย่างค่าลับ/ค่าตั้ง (คัดลอกเป็น .env)
├── airflow/
│   ├── Dockerfile            # apache/airflow:2.10.5 + ไลบรารีเพิ่ม
│   └── requirements.txt
├── sql/init.sql              # สร้างตารางทั้งหมด (รันอัตโนมัติ)
├── dags/
│   ├── anime_pipeline.py     # DAG หลัก (TaskFlow API)
│   └── anime_lib/
│       ├── extract.py        # ดึงข้อมูล 3 เว็บ
│       ├── transform.py      # แปลงสคีมา, DQ check, merge/dedup
│       └── db.py             # UPSERT / log / merge ลง Postgres
└── web/                      # เว็บแดชบอร์ด Streamlit
    ├── app.py                # จุดเริ่มต้น + เมนูหน้า
    ├── overview.py           # หน้า 1: ภาพรวมคะแนน
    ├── pipeline_status.py    # หน้า 2: สถานะ Pipeline
    ├── common.py             # ต่อฐานข้อมูล, สี, footer
    ├── Dockerfile
    └── requirements.txt
```

### ตารางในฐานข้อมูล

| ตาราง | หน้าที่ | Primary / Unique key |
|---|---|---|
| `raw_anime` | ข้อมูลจากแต่ละเว็บในสคีมาเดียวกัน | `(source, source_id)` |
| `anime_merged` | 1 แถวต่อ 1 เรื่อง พร้อมคะแนนทุกเว็บ | `merge_key`, `mal_id` (UNIQUE NULL) |
| `score_history` | snapshot คะแนนรายวัน | `(source, source_id, snapshot_date)` |
| `quarantine` | แถวที่ไม่ผ่าน DQ (เก็บ payload เป็น JSONB) | กันซ้ำรายวันด้วย `md5(payload)` |
| `pipeline_runs` | ผลการรันของแต่ละ source | `(dag_run_id, source)` |

### กฎการแปลงข้อมูล (Transform)

- คะแนนทุกเว็บเป็น **สเกล 0–10 ทศนิยม 2 ตำแหน่ง** (AniList / Kitsu หาร 10)
- ประเภทเป็นมาตรฐานเดียว: `TV`, `Movie`, `OVA`, `ONA`, `Special`, `Music` (เช่น `MOVIE`/`movie` → `Movie`, `TV Special` → `Special`)
- `title_norm` = ตัวพิมพ์เล็ก ตัดเครื่องหมาย/สัญลักษณ์ และช่องว่างซ้ำ (เช่น `Gintama°` → `gintama`)
- `popularity` = members (MAL) / popularity (AniList) / userCount (Kitsu)

---

## 3. ตารางเทียบเกณฑ์การให้คะแนน

| ระดับ | เกณฑ์ | ทำที่ไฟล์ / task |
|---|---|---|
| **Level 1** | Extract → Transform → Load ลง Postgres | `dags/anime_pipeline.py`: `*.extract` → `*.transform` → `load_raw`; ฟังก์ชันใน `anime_lib/extract.py`, `transform.py`, `db.py` |
| **Level 2** | TaskGroup ต่อ source, 3 กลุ่มรันขนานกัน | `TaskGroup("mal")`, `TaskGroup("anilist")`, `TaskGroup("kitsu")` ใน `anime_pipeline.py` (ไม่มี dependency ระหว่างกลุ่ม จึงรันขนานด้วย LocalExecutor) |
| | ทุก source แปลงเป็นสคีมาเดียวกัน | `transform.make_row()` + `transform_mal/anilist/kitsu()` → ตาราง `raw_anime` |
| | UPSERT (`INSERT ... ON CONFLICT DO UPDATE`) รันซ้ำวันเดิมจำนวนแถวไม่เพิ่ม | `db.upsert_raw_anime()`, `db.upsert_score_history()` (task `load_raw`) |
| **Level 3** | source หนึ่งพัง DAG ยัง merge ที่เหลือได้ | extract `retries=2` → `run_extract()` raise `AirflowSkipException` เมื่อครบ retry; `validate` และ `merge` ใช้ `trigger_rule="none_failed_min_one_success"` |
| | Data Quality ด้วย `@task.branch` เห็นใน Graph view | `validate` → `check_quality` (branch) → `load_raw` / `quarantine_rows`; กฎอยู่ใน `transform.check_row()` |
| | Dedup ข้ามเว็บ (mal_id ก่อน, ไม่มีใช้ title_norm + year), idempotent | `transform.merge_rows()` + `db.replace_merged()` (task `merge`) |
| **Bonus** | Dynamic Task Mapping `.expand()` | `mal.extract.expand(offset=[0,50,100,150])`, `anilist.extract.expand(page=[1,2,3,4])` |
| | บันทึกการรันราย source | `db.log_run()` / `db.set_rows_loaded()` → ตาราง `pipeline_runs` (แสดงในหน้า "สถานะ Pipeline") |
| | DB connection ผ่าน Airflow Connection + secrets ใน `.env` | `AIRFLOW_CONN_ANIMEDB` ใน `docker-compose.yml`, `PostgresHook("animedb")` ใน `db.py`, `.env.example` |
| | จำลองเว็บล่ม | `BREAK_SOURCE` → `extract.check_break()` |
| | Dashboard | `web/overview.py`, `web/pipeline_status.py` |

### กฎ Data Quality (`transform.check_row`)

| ฟิลด์ | เงื่อนไขที่ต้องผ่าน |
|---|---|
| `source_id`, `title` | ต้องไม่ว่าง |
| `score_10` | อยู่ในช่วง 0–10 หรือเป็น NULL |
| `episodes` | ≥ 0 หรือเป็น NULL |
| `year` | ระหว่าง 1950 ถึง ปีหน้า |

แถวที่ไม่ผ่านจะถูกเก็บใน `quarantine` พร้อมเหตุผล ส่วนแถวที่ผ่านไป `load_raw`
ถ้าอยากเห็นทั้งสองเส้นทางของ branch ทำงาน ให้ตั้ง `INJECT_BAD_ROWS=1` (ใส่แถวเสียปลอม 2 แถว)

### วิธี Merge ข้ามเว็บ (`transform.merge_rows`)

1. แถวที่มี `mal_id` → จับกลุ่มด้วย `merge_key = "mal:<mal_id>"` (AniList มี `idMal`, Kitsu ได้จาก mappings)
2. แถวที่ไม่มี `mal_id` → หากลุ่มที่มี `title_norm + year` ตรงกัน ถ้าไม่เจอสร้างกลุ่ม `"title:<title_norm>|<year>"`
3. ต่อกลุ่มคำนวณ `mal_score`, `anilist_score`, `kitsu_score`, `avg_score`, `score_diff` (max − min), `sources`, `source_count`
4. เขียนลง `anime_merged` ใน transaction เดียว: UPSERT ตาม `merge_key` แล้วลบเรื่องที่ไม่อยู่ในรอบนี้ → **รันซ้ำกี่ครั้งผลเท่าเดิม**

> merge ใช้เฉพาะ source ที่โหลดสำเร็จ **ในรอบนี้** ดังนั้นถ้า Kitsu ล่ม คอลัมน์ `kitsu_score` จะเป็นว่าง
> และ `sources` จะไม่มี `kitsu` (ข้อมูลดิบเก่าของ Kitsu ยังอยู่ใน `raw_anime` / `score_history`)

---

## 4. ทดสอบ Idempotency (รันซ้ำแล้วจำนวนแถวต้องไม่เพิ่ม)

```bash
# นับจำนวนแถวก่อน
docker compose exec animedb psql -U anime -d animedb -c "
SELECT 'raw_anime' t, count(*) FROM raw_anime
UNION ALL SELECT 'score_history', count(*) FROM score_history
UNION ALL SELECT 'anime_merged', count(*) FROM anime_merged;"

# รัน DAG ซ้ำในวันเดียวกัน แล้วรอจนเสร็จ (ดูใน Airflow UI)
docker compose exec airflow-scheduler airflow dags trigger anime_pipeline

# นับอีกครั้ง → ตัวเลขต้องเท่าเดิม
docker compose exec animedb psql -U anime -d animedb -c "
SELECT 'raw_anime' t, count(*) FROM raw_anime
UNION ALL SELECT 'score_history', count(*) FROM score_history
UNION ALL SELECT 'anime_merged', count(*) FROM anime_merged;"
```

เหตุผลที่ไม่เพิ่ม: `raw_anime` และ `score_history` ใช้ `ON CONFLICT DO UPDATE` บน primary key,
`anime_merged` UPSERT ตาม `merge_key`, `pipeline_runs` 1 แถวต่อ `(dag_run_id, source)`

> หมายเหตุ: `raw_anime` อาจเพิ่มขึ้นได้ในวันถัด ๆ ไป ถ้ามีเรื่องใหม่หลุดเข้ามาใน Top 200 (เป็นพฤติกรรมที่ถูกต้อง)
> ส่วน `score_history` จะเพิ่มวันละ ~600 แถวเพราะเป็น snapshot รายวัน

---

## 5. จำลองเว็บล่ม (BREAK_SOURCE)

```bash
# 1) แก้ใน .env ให้เป็น
BREAK_SOURCE=kitsu

# 2) สร้างคอนเทนเนอร์ Airflow ใหม่ให้อ่านค่า env ใหม่
docker compose up -d airflow-scheduler airflow-webserver

# 3) trigger DAG
docker compose exec airflow-scheduler airflow dags trigger anime_pipeline
```

สิ่งที่จะเห็น:

- `kitsu.extract` จะ **retry 2 ครั้ง** (สีเหลือง up_for_retry) แล้วกลายเป็น **skipped** (สีชมพู) ไม่ใช่ failed
- `kitsu.transform` ถูก skip ตาม แต่ `validate` → `load_raw` → `merge` ยังทำงานด้วยข้อมูล MAL + AniList
- DAG run จบด้วยสถานะ **success**
- ทดสอบแล้วทั้ง `BREAK_SOURCE=kitsu` และ `BREAK_SOURCE=mal` — กรณี MAL ซึ่งใช้ Dynamic Task Mapping
  ทั้ง 4 หน้า (`mal.extract [0..3]`) จะ retry ครบ 3 ครั้งแล้ว skip ทุกหน้า และ merge จาก AniList + Kitsu ต่อได้
- ตาราง `pipeline_runs` มีแถว `kitsu | failed | จำลองเว็บ kitsu ล่ม ...` และหน้า **สถานะ Pipeline** แสดงการ์ดสีแดง

```bash
docker compose exec animedb psql -U anime -d animedb -c \
  "SELECT dag_run_id, source, status, rows_fetched, rows_loaded, error_message FROM pipeline_runs ORDER BY run_id DESC LIMIT 6;"
```

เสร็จแล้วอย่าลืมตั้ง `BREAK_SOURCE=` (ว่าง) แล้วรัน `docker compose up -d airflow-scheduler airflow-webserver` อีกครั้ง

---

## 6. แดชบอร์ด

**หน้า "ภาพรวมคะแนน"**
- ตัวกรองด้านข้าง: เว็บไซต์ (เลือกได้หลายเว็บ), แนวเรื่อง, ประเภท, ช่วงปี, ช่วงคะแนนเฉลี่ย
- KPI: จำนวนอนิเมะที่รวมแล้ว, คะแนนเฉลี่ยของแต่ละเว็บ, จำนวนเรื่องที่พบครบ 3 เว็บ, เรื่องที่คะแนนต่างกันมากที่สุด
- ตารางอนิเมะที่รวมแล้วตามตัวกรอง
- กราฟ: เทียบคะแนน 3 เว็บ (Top 20), คะแนนเฉลี่ยตามแนวเรื่อง, คะแนนเฉลี่ยตามปี, ประวัติคะแนนของเรื่องที่เลือก

**หน้า "สถานะ Pipeline"**
- สถานะล่าสุดของแต่ละเว็บ (เขียว = success, แดง = failed พร้อมไอคอนและข้อความ), จำนวนแถว, ข้อผิดพลาด
- ประวัติการรัน และตาราง quarantine

> แนวเรื่อง (genres) มาจาก AniList เท่านั้น เพราะหน้า Top ของ MAL และ API หลักของ Kitsu ไม่มีข้อมูลนี้
> กราฟประวัติคะแนนจะเป็นเส้นเมื่อ DAG รันครบหลายวัน (วันแรกมีแค่จุดเดียว)

---

## 7. จริยธรรมข้อมูล (Data Ethics)

- **ตรวจ robots.txt แล้ว**: `https://myanimelist.net/robots.txt` ไม่ได้ห้าม `/topanime.php` สำหรับ User-agent ทั่วไป
  (ห้ามเฉพาะ `/admin/`, `/comments.php`, `/clubs.php/*` ฯลฯ) โปรเจกต์นี้ดึงเฉพาะหน้า Top Anime 4 หน้าต่อวัน
  และไม่เข้าหน้าที่ถูกห้าม robots.txt ของ MAL บล็อก User-agent `Scrapy` และบอทเก็บข้อมูลฝึก AI
  ซึ่งโปรเจกต์นี้ไม่ได้ใช้และไม่ได้มีวัตถุประสงค์นั้น
  ตรวจซ้ำด้วย `urllib.robotparser` ของ Python: User-agent ของโปรเจกต์ได้ผล `allowed=True`
  ส่วน User-agent `Scrapy` ได้ `allowed=False`
- **AniList**: `https://anilist.co/robots.txt` เป็น `User-agent: * / Disallow:` (อนุญาตทั้งหมด)
  และเราใช้ GraphQL API ทางการ ไม่ได้ scrape หน้าเว็บ
- **Kitsu**: ไฟล์ `https://kitsu.app/robots.txt` อยู่หลัง Cloudflare (ตอบ HTTP 403 ให้โปรแกรมอัตโนมัติ)
  จึงอ่านกฎไม่ได้ แต่เราไม่ได้ scrape หน้าเว็บของ Kitsu เลย ใช้เฉพาะ JSON:API สาธารณะที่ Kitsu
  เปิดให้ใช้และมีเอกสารทางการ (`/api/edge`) ไม่ต้องใช้ API key และดึงเพียง 10 คำขอต่อวัน
- **ไม่ใช้ Scrapy / Selenium / Playwright / headless browser**: ใช้แค่ `requests` + `BeautifulSoup`
  และ API ทางการของ AniList / Kitsu
- **ระบุตัวตนตรงไปตรงมา**: User-Agent `StudentProject-AnimePipeline/1.0 (educational)` ไม่ปลอมเป็นเบราว์เซอร์
- **Rate limiting**: MAL หน่วง 3 วินาที/หน้า และดึงทีละหน้า (`max_active_tis_per_dag=1`), AniList/Kitsu หน่วง 2 วินาที/คำขอ,
  AniList รองรับ HTTP 429 ด้วย `Retry-After`, timeout 30 วินาที, DAG รันวันละครั้ง
- **PDPA / ข้อมูลส่วนบุคคล**: เก็บเฉพาะข้อมูลระดับ "อนิเมะ" (ชื่อ คะแนน ประเภท ปี จำนวนสมาชิกรวม)
  **ไม่เก็บ** รีวิว ความคิดเห็น โปรไฟล์ผู้ใช้ ชื่อผู้ใช้ หรือรูปภาพ จึงไม่มีข้อมูลส่วนบุคคล
- **ให้เครดิตแหล่งข้อมูล**: แดชบอร์ดทุกหน้ามี footer อ้างอิง MyAnimeList, AniList, Kitsu
  และระบุว่า "จัดทำเพื่อการศึกษา" ไม่นำข้อมูลไปใช้เชิงพาณิชย์
- **รหัสผ่านไม่อยู่ในโค้ด**: เก็บใน `.env` (อยู่ใน `.gitignore`) มีแค่ `.env.example` ใน repo

---

## 8. ปัญหาที่พบระหว่างพัฒนาและวิธีแก้

ปัญหาทั้งหมดด้านล่างเกิดขึ้นจริงระหว่างพัฒนาและทดสอบโปรเจกต์นี้

### ปัญหาที่ 1: AniList บางเรื่องไม่มีปีที่ฉาย ทำให้ข้อมูลดีถูกส่งเข้า quarantine

**อาการ**
- รัน DAG ครั้งแรกสำเร็จทุก task แต่ task `quarantine_rows` ทำงานด้วย (ปกติควรถูก skip เพราะข้อมูลจริงไม่ควรเสีย)
- AniList ดึงได้ 200 แถว แต่ `pipeline_runs.rows_loaded` เหลือ 187 แถว

**วิธีหาสาเหตุ**
1. เปิดดูตาราง quarantine ว่าแถวไหนไม่ผ่านและเพราะอะไร
   ```sql
   SELECT source, reason, payload->>'title' AS title FROM quarantine;
   ```
2. พบว่าทั้ง 13 แถวเป็นของ AniList และมีเหตุผลเดียวกันคือ `year=None ไม่อยู่ในช่วง 1950-2027`
   ชื่อเรื่องเป็นอนิเมะจีน (donghua) เช่น *Modao Zushi 3*, *Tian Guan Ci Fu 2*
3. ดู `payload` พบว่าฟิลด์ `seasonYear` เป็น `null` เพราะ AniList กำหนด season (ฤดูฉาย) เฉพาะอนิเมะญี่ปุ่นที่ฉายตามฤดูกาล
   แต่ข้อมูลวันที่เริ่มฉาย (`startDate.year`) ยังมีอยู่

**วิธีแก้**
- เพิ่ม `startDate { year }` ใน GraphQL query ([extract.py](dags/anime_lib/extract.py))
- ใน [transform.py](dags/anime_lib/transform.py) ใช้ `seasonYear` ก่อน ถ้าว่างให้ใช้ `startDate.year` แทน
- ผลหลังแก้: AniList โหลดได้ครบ 200 แถว, จำนวนเรื่องที่พบครบ 3 เว็บเพิ่มจาก 101 เป็น 106 เรื่อง

**บทเรียน:** กฎ Data Quality ทำงานถูกต้อง (จับแถวที่ไม่มีปีได้) แต่ต้องตรวจ quarantine ทุกครั้งว่าเป็น "ข้อมูลเสียจริง"
หรือ "เราดึงข้อมูลมาไม่ครบเอง"

---

### ปัญหาที่ 2: ชื่อคอลัมน์ภาษาไทยทำให้กราฟแดชบอร์ด error

**อาการ**
- หน้าแดชบอร์ดแสดงตารางและ KPI ได้ แต่กราฟ "คะแนนเฉลี่ยตามแนวเรื่อง" ขึ้น error:
  ```
  ValueError: Value of 'custom_data_0' is not the name of a column in 'data_frame'.
  Expected one of ['genres', 'คะแนนเฉลี่ย', 'จํานวน'] but received: จำนวน
  ```

**วิธีหาสาเหตุ**
1. ข้อความ error บอกว่าหาคอลัมน์ `จำนวน` ไม่เจอ ทั้งที่ในรายการก็มี `จํานวน` อยู่ ดูด้วยตาเหมือนกันทุกตัวอักษร
2. แสดงว่าเป็นตัวอักษรคนละรหัส Unicode: `ำ` (สระอำ U+0E33) ถูกแยกเป็น `ํ` + `า` (U+0E4D + U+0E32)
3. ต้นเหตุคือโค้ด `df.agg(จำนวน=("title", "count"))` ใช้ภาษาไทยเป็น **ชื่อ keyword argument**
   ซึ่ง Python จะแปลงชื่อตัวแปรทุกตัวเป็นรูป NFKC ตาม PEP 3131 ทำให้ `จำนวน` กลายเป็น `จํานวน`
   แต่ตอนเรียก `custom_data=["จำนวน"]` เป็น **string ธรรมดา** จึงไม่ถูกแปลง ชื่อเลยไม่ตรงกัน

**วิธีแก้**
- ใช้ชื่อคอลัมน์ภาษาอังกฤษภายในโค้ด (`mean_score`, `count`) แล้วแปลงเป็นภาษาไทยเฉพาะตอนแสดงผลด้วย
  `labels={"mean_score": "คะแนนเฉลี่ย"}` ([overview.py](web/overview.py))
- ทดสอบซ้ำด้วย Streamlit `AppTest` ทั้ง 2 หน้า ไม่มี exception

**บทเรียน:** ภาษาไทยใช้ได้ดีใน string และหน้าจอ แต่ไม่ควรใช้เป็นชื่อตัวแปรหรือชื่อ argument ในโค้ด Python

---

### ปัญหาที่ 3: เปิด Docker แล้วพอร์ตชนกับโปรแกรมอื่นในเครื่อง

**อาการ**
- `docker compose up -d` สร้างคอนเทนเนอร์ได้ แต่บางตัวไม่ start:
  ```
  Bind for 0.0.0.0:5433 failed: port is already allocated
  Bind for 0.0.0.0:8080 failed: port is already allocated
  ```

**วิธีหาสาเหตุ**
1. อ่าน error ทราบว่าพอร์ต 5433 (animedb) และ 8080 (Airflow) ถูกโปรแกรมอื่นใช้อยู่แล้ว
2. ตรวจด้วย `netstat -ano | findstr :8080` (Windows) หรือ `lsof -i :8080` (Linux/macOS) เพื่อดูว่าโปรแกรมไหนจองไว้
3. พอร์ตเหล่านี้เป็นพอร์ตยอดนิยม ทุกเครื่องอาจเจอไม่เหมือนกัน การเขียนพอร์ตตายตัวใน `docker-compose.yml` จึงไม่ดี

**วิธีแก้**
- เปลี่ยนให้พอร์ตฝั่งเครื่องอ่านจาก `.env`: `"${AIRFLOW_HOST_PORT:-8080}:8080"`,
  `"${ANIMEDB_HOST_PORT:-5433}:5432"`, `"${DASHBOARD_HOST_PORT:-8501}:8501"`
- เครื่องที่พอร์ตชนแก้แค่ `.env` (เช่น `AIRFLOW_HOST_PORT=8081`, `ANIMEDB_HOST_PORT=5434`) โดยไม่ต้องแก้โค้ด
- พอร์ต **ภายใน** Docker network ไม่ต้องเปลี่ยน (Airflow ยังต่อ `animedb:5432` เหมือนเดิม)

---

### ปัญหาที่ 4: Kitsu ได้ข้อมูลไม่ครบ 200 และเสี่ยงให้ UPSERT error

**อาการ**
- Kitsu ดึง 10 หน้า × 20 = 200 แถว แต่หลังโหลดเหลือ 198 แถว ทั้งที่ไม่มีแถวใดเข้า quarantine

**วิธีหาสาเหตุ**
1. ตรวจว่ามี `source_id` ซ้ำในข้อมูลรอบเดียวกันหรือไม่ → พบว่ามีเรื่องเดียวกันปรากฏ 2 หน้า
2. สาเหตุคือการแบ่งหน้าด้วย `page[offset]` บนรายการที่ **เรียงตามคะแนนและเปลี่ยนแปลงได้ตลอดเวลา**
   ถ้าระหว่างที่เราดึงหน้า 1 กับหน้า 2 อันดับมีการขยับ (หรือหลายเรื่องคะแนนเท่ากัน) เรื่องหนึ่งอาจโผล่ซ้ำ 2 หน้า
3. ถ้าส่งแถวซ้ำแบบนี้เข้า `INSERT ... ON CONFLICT DO UPDATE` ในคำสั่งเดียว PostgreSQL จะ error:
   `ON CONFLICT DO UPDATE command cannot affect row a second time` ทำให้ `load_raw` ล้มทั้ง task

**วิธีแก้**
- ใน `split_good_bad()` ([transform.py](dags/anime_lib/transform.py)) ตัดแถวซ้ำด้วยคีย์ `(source, source_id)`
  เก็บแถวแรก (อันดับดีกว่า) ไว้ก่อนโหลด
- ผล: `load_raw` ไม่ error และแต่ละเรื่องมี 1 แถวต่อเว็บ (MAL ก็ใช้หลักเดียวกัน เพราะแบ่งหน้าแบบ offset เหมือนกัน)

---

### ปัญหาที่ 5: เชื่อม pgAdmin เข้าฐานข้อมูลไม่ได้ (password authentication failed)

**อาการ**
- pgAdmin แจ้ง `FATAL: password authentication failed for user "anime"` ทั้งที่ `localhost:5434`

**วิธีหาสาเหตุ**
1. ทดสอบด้วย `psql` จากเครื่องเดียวกันโดยใช้ค่าจาก `.env` → **เชื่อมต่อสำเร็จ** (PostgreSQL 16.15)
   แปลว่าฐานข้อมูลและรหัสผ่านถูกต้อง ปัญหาอยู่ที่ค่าที่กรอกใน pgAdmin
   ```bash
   psql -h 127.0.0.1 -p 5434 -U anime -d animedb -c "select version();"
   ```
2. `netstat -ano | findstr :5434` พบว่า `[::1]:5434` (IPv6 ของ localhost) มีโปรแกรม `wslrelay` จองอยู่ด้วย
   นอกเหนือจาก Docker ทำให้ `localhost` อาจต่อไปผิดที่
3. เครื่องนี้มี PostgreSQL 18 ติดตั้งในเครื่องด้วย จึงอาจสับสนกับรหัสผ่านของ PostgreSQL ในเครื่อง (port 5432)

**วิธีแก้**
- ใน pgAdmin ใช้ Host = `127.0.0.1` แทน `localhost`, Port = `5434`, และพิมพ์รหัสผ่านจาก `.env` ใหม่เอง (ไม่ใช้ autofill)

**บทเรียน:** ทดสอบการเชื่อมต่อด้วยเครื่องมือที่สองเสมอ (เช่น `psql`) เพื่อแยกให้ออกว่าปัญหาอยู่ที่เซิร์ฟเวอร์หรือที่ client

---

## 9. แก้ปัญหาเบื้องต้น

| อาการ | วิธีแก้ |
|---|---|
| `port is already allocated` | เปลี่ยนพอร์ตใน `.env` แล้ว `docker compose up -d` |
| DAG ไม่ขึ้นใน UI | `docker compose exec airflow-scheduler airflow dags list-import-errors` |
| แดชบอร์ดขึ้นว่า "ยังไม่มีข้อมูล" | ยังไม่ได้รัน DAG หรือรันยังไม่เสร็จ |
| MAL ดึงไม่ได้ (403/ไม่พบแถว) | MAL อาจเปลี่ยนโครงสร้าง HTML หรือจำกัดการเข้าถึงชั่วคราว — DAG จะ skip MAL และ merge จาก 2 เว็บที่เหลือ |
| แก้ init.sql แล้วไม่มีผล | init.sql รันเฉพาะตอนสร้าง volume ครั้งแรก: `docker compose down -v` แล้ว `up` ใหม่ |
