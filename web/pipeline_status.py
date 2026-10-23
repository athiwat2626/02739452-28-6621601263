"""หน้า 2: สถานะ Pipeline — ผลการรันล่าสุดของแต่ละ source และแถวที่ถูก quarantine"""
import streamlit as st

from common import SOURCE_LABELS, footer, query

st.title("🛠️ สถานะ Pipeline")
if st.button("🔄 โหลดข้อมูลใหม่"):
    query.clear()

# ---------------- สถานะล่าสุดของแต่ละ source ----------------
st.subheader("การรันล่าสุดของแต่ละเว็บ")
latest = query("""
    SELECT DISTINCT ON (source) source, status, rows_fetched, rows_loaded, error_message,
           dag_run_id, started_at, finished_at
    FROM pipeline_runs
    ORDER BY source, finished_at DESC NULLS LAST
""")

cols = st.columns(len(SOURCE_LABELS))
for col, (source, label) in zip(cols, SOURCE_LABELS.items()):
    with col:
        row = latest[latest["source"] == source]
        if row.empty:
            st.info(f"**{label}**\n\nยังไม่เคยรัน", icon="⏳")
            continue
        r = row.iloc[0]
        finished = r["finished_at"].strftime("%d/%m/%Y %H:%M") if r["finished_at"] is not None else "—"
        text = (f"**{label}** — {r['status'].upper()}\n\n"
                f"ดึงได้ {r['rows_fetched']} แถว · โหลด {r['rows_loaded']} แถว\n\n"
                f"เสร็จเมื่อ {finished}")
        # สีเขียว/แดง + ไอคอน + ข้อความ (ไม่สื่อสถานะด้วยสีอย่างเดียว)
        if r["status"] == "success":
            st.success(text, icon="✅")
        else:
            st.error(text + f"\n\nสาเหตุ: {r['error_message']}", icon="❌")

# ---------------- ประวัติการรัน ----------------
st.subheader("ประวัติการรัน (50 รายการล่าสุด)")
runs = query("""
    SELECT dag_run_id, source, status, rows_fetched, rows_loaded, error_message, started_at, finished_at
    FROM pipeline_runs ORDER BY finished_at DESC NULLS LAST LIMIT 50
""")
if runs.empty:
    st.info("ยังไม่มีประวัติการรัน")
else:
    runs["status"] = runs["status"].map({"success": "✅ success", "failed": "❌ failed"}).fillna(runs["status"])
    runs["source"] = runs["source"].map(SOURCE_LABELS).fillna(runs["source"])

    def color_status(value):
        if "success" in str(value):
            return "background-color: rgba(27, 175, 122, 0.18)"
        if "failed" in str(value):
            return "background-color: rgba(227, 73, 72, 0.18)"
        return ""

    st.dataframe(
        runs.style.map(color_status, subset=["status"]),
        hide_index=True,
        use_container_width=True,
        column_config={
            "dag_run_id": "DAG run", "source": "เว็บ", "status": "สถานะ",
            "rows_fetched": "ดึงได้", "rows_loaded": "โหลดแล้ว", "error_message": "ข้อผิดพลาด",
            "started_at": st.column_config.DatetimeColumn("เริ่ม", format="DD/MM/YYYY HH:mm"),
            "finished_at": st.column_config.DatetimeColumn("เสร็จ", format="DD/MM/YYYY HH:mm"),
        },
    )

# ---------------- Quarantine ----------------
st.subheader("แถวที่ไม่ผ่านการตรวจคุณภาพ (Quarantine)")
quarantine = query("""
    SELECT id, source, reason, payload::text AS payload, run_date, created_at
    FROM quarantine ORDER BY created_at DESC LIMIT 200
""")
if quarantine.empty:
    st.success("ไม่มีแถวที่ถูก quarantine 🎉", icon="✅")
else:
    st.caption(f"ทั้งหมด {len(quarantine)} แถว (แสดงล่าสุดไม่เกิน 200 แถว)")
    st.dataframe(
        quarantine, hide_index=True, use_container_width=True,
        column_config={
            "id": "ID", "source": "เว็บ", "reason": "เหตุผล", "payload": "ข้อมูลดิบ (JSON)",
            "run_date": "วันที่รัน",
            "created_at": st.column_config.DatetimeColumn("บันทึกเมื่อ", format="DD/MM/YYYY HH:mm"),
        },
    )

footer()
