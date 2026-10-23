"""
แดชบอร์ดเปรียบเทียบคะแนนอนิเมะ (Streamlit)
รัน: streamlit run app.py
หน้า 1 = ภาพรวมคะแนน, หน้า 2 = สถานะ Pipeline
"""
import streamlit as st

st.set_page_config(page_title="เปรียบเทียบคะแนนอนิเมะ", page_icon="📊", layout="wide")

pages = st.navigation([
    st.Page("overview.py", title="ภาพรวมคะแนน", icon="📊", default=True),
    st.Page("pipeline_status.py", title="สถานะ Pipeline", icon="🛠️"),
])
pages.run()
