# อิมเมจ Airflow ทางการ + ไลบรารีที่โปรเจกต์ใช้
FROM apache/airflow:2.10.5-python3.11

COPY requirements.txt /requirements.txt
# ใส่ apache-airflow เวอร์ชันเดิมไว้ด้วย เพื่อกัน pip อัปเกรด Airflow โดยไม่ตั้งใจ
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
