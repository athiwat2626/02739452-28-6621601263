"""หน้า 1: ภาพรวมการเปรียบเทียบคะแนนอนิเมะจาก 3 เว็บ"""
import pandas as pd
import plotly.express as px
import streamlit as st

from common import SCORE_COLUMNS, SINGLE_COLOR, SOURCE_COLORS, SOURCE_LABELS, footer, query

st.title("📊 เปรียบเทียบคะแนนอนิเมะยอดนิยม")
st.caption("รวมข้อมูล Top ~200 เรื่องจาก MyAnimeList, AniList และ Kitsu (คะแนนปรับเป็นสเกล 0–10)")

# ---------------- โหลดข้อมูล ----------------
df = query("SELECT * FROM anime_merged ORDER BY avg_score DESC NULLS LAST")
if df.empty:
    st.info("ยังไม่มีข้อมูล — เปิด Airflow (http://localhost:8080) แล้วสั่งรัน DAG `anime_pipeline` ก่อน")
    footer()
    st.stop()

score_cols = list(SCORE_COLUMNS.values()) + ["avg_score", "score_diff"]
df[score_cols] = df[score_cols].astype(float)
df["genres"] = df["genres"].apply(lambda g: g or [])
df["sources"] = df["sources"].apply(lambda s: s or [])

# ---------------- ตัวกรองด้านข้าง ----------------
st.sidebar.header("ตัวกรอง")
source_options = list(SOURCE_LABELS)
picked_sources = st.sidebar.multiselect(
    "เว็บไซต์", source_options, default=source_options, format_func=SOURCE_LABELS.get,
    help="แสดงเฉพาะเรื่องที่พบในเว็บที่เลือก และแสดงคะแนนของเว็บที่เลือกเท่านั้น",
)
all_genres = sorted({g for gs in df["genres"] for g in gs})
picked_genre = st.sidebar.selectbox("แนวเรื่อง (ข้อมูลจาก AniList)", ["ทั้งหมด"] + all_genres)
all_types = sorted(df["anime_type"].dropna().unique())
picked_types = st.sidebar.multiselect("ประเภท", all_types, default=all_types)

year_min, year_max = int(df["year"].min()), int(df["year"].max())
picked_years = st.sidebar.slider("ช่วงปีที่ฉาย", year_min, year_max, (year_min, year_max))
picked_scores = st.sidebar.slider("ช่วงคะแนนเฉลี่ย", 0.0, 10.0, (0.0, 10.0), step=0.1)

# ---------------- กรองข้อมูล ----------------
if not picked_sources:
    st.warning("กรุณาเลือกเว็บไซต์อย่างน้อย 1 เว็บ")
    st.stop()

mask = df["sources"].apply(lambda s: bool(set(s) & set(picked_sources)))
mask &= df["anime_type"].isin(picked_types)
mask &= df["year"].between(*picked_years)
mask &= df["avg_score"].between(*picked_scores)
if picked_genre != "ทั้งหมด":
    mask &= df["genres"].apply(lambda gs: picked_genre in gs)
view = df[mask].copy()

if view.empty:
    st.warning("ไม่พบอนิเมะที่ตรงกับตัวกรอง")
    footer()
    st.stop()

# ---------------- KPI ----------------
c1, c2, c3 = st.columns(3)
c1.metric("จำนวนอนิเมะ (รวมแล้ว)", f"{len(view):,} เรื่อง")
c2.metric("พบครบทั้ง 3 เว็บ", f"{int((view['source_count'] == 3).sum()):,} เรื่อง")
diff_rows = view.dropna(subset=["score_diff"])
if not diff_rows.empty:
    top_diff = diff_rows.loc[diff_rows["score_diff"].idxmax()]
    c3.metric("คะแนนต่างกันมากที่สุด", f"{top_diff['score_diff']:.2f} คะแนน",
              help="ผลต่างระหว่างคะแนนสูงสุดและต่ำสุดของเรื่องนี้ในแต่ละเว็บ")
    c3.caption(f"เรื่อง: **{top_diff['title']}**")
else:
    c3.metric("คะแนนต่างกันมากที่สุด", "—")

st.markdown("**คะแนนเฉลี่ยของแต่ละเว็บ**")
site_cols = st.columns(len(picked_sources))
for col, source in zip(site_cols, picked_sources):
    mean = view[SCORE_COLUMNS[source]].mean()
    col.metric(SOURCE_LABELS[source], f"{mean:.2f}" if pd.notna(mean) else "—")

# ---------------- ตาราง ----------------
st.subheader("ตารางอนิเมะที่รวมแล้ว")
table = view[["title", "anime_type", "episodes", "year", "mal_score", "anilist_score",
              "kitsu_score", "avg_score", "score_diff", "source_count", "genres"]].copy()
table["genres"] = table["genres"].apply(", ".join)
hidden = [SCORE_COLUMNS[s] for s in SOURCE_LABELS if s not in picked_sources]
st.dataframe(
    table.drop(columns=hidden),
    hide_index=True,
    use_container_width=True,
    column_config={
        "title": "ชื่อเรื่อง",
        "anime_type": "ประเภท",
        "episodes": st.column_config.NumberColumn("ตอน", format="%d"),
        "year": st.column_config.NumberColumn("ปี", format="%d"),
        "mal_score": st.column_config.NumberColumn("MyAnimeList", format="%.2f"),
        "anilist_score": st.column_config.NumberColumn("AniList", format="%.2f"),
        "kitsu_score": st.column_config.NumberColumn("Kitsu", format="%.2f"),
        "avg_score": st.column_config.NumberColumn("เฉลี่ย", format="%.2f"),
        "score_diff": st.column_config.NumberColumn("ส่วนต่าง", format="%.2f"),
        "source_count": "จำนวนเว็บ",
        "genres": "แนวเรื่อง",
    },
)

# ---------------- กราฟ ----------------
CHART_LAYOUT = dict(margin=dict(l=8, r=8, t=8, b=8), legend_title_text="")

# 1) เทียบคะแนนรายเรื่อง 20 อันดับแรก (แท่งกลุ่ม)
st.subheader("เทียบคะแนนจากแต่ละเว็บ — 20 อันดับแรก (ตามคะแนนเฉลี่ย)")
top20 = view.nlargest(20, "avg_score")
long = top20.melt(
    id_vars=["title"], value_vars=[SCORE_COLUMNS[s] for s in picked_sources],
    var_name="เว็บ", value_name="คะแนน",
).dropna(subset=["คะแนน"])
long["เว็บ"] = long["เว็บ"].map({v: SOURCE_LABELS[k] for k, v in SCORE_COLUMNS.items()})
fig = px.bar(
    long, x="คะแนน", y="title", color="เว็บ", orientation="h", barmode="group",
    color_discrete_map=SOURCE_COLORS,
    category_orders={"title": top20["title"].tolist(),
                     "เว็บ": [SOURCE_LABELS[s] for s in picked_sources]},
    labels={"title": ""},
)
fig.update_traces(hovertemplate="%{y}<br>%{fullData.name}: %{x:.2f}<extra></extra>")
fig.update_layout(height=760, bargap=0.25, bargroupgap=0.08, xaxis_range=[0, 10],
                  legend=dict(orientation="h", y=1.02, x=0), **CHART_LAYOUT)
st.plotly_chart(fig, use_container_width=True)

left, right = st.columns(2)

# 2) คะแนนเฉลี่ยตามแนวเรื่อง
with left:
    st.subheader("คะแนนเฉลี่ยตามแนวเรื่อง")
    genre_df = (view.explode("genres").dropna(subset=["genres"])
                .groupby("genres").agg(mean_score=("avg_score", "mean"), count=("title", "count"))
                .reset_index().sort_values("mean_score"))
    if genre_df.empty:
        st.info("ไม่มีข้อมูลแนวเรื่อง (ต้องมีข้อมูลจาก AniList)")
    else:
        fig = px.bar(genre_df, x="mean_score", y="genres", orientation="h",
                     custom_data=["count"], labels={"genres": "", "mean_score": "คะแนนเฉลี่ย"})
        fig.update_traces(marker_color=SINGLE_COLOR,
                          hovertemplate="%{y}<br>คะแนนเฉลี่ย %{x:.2f}<br>%{customdata[0]} เรื่อง<extra></extra>")
        fig.update_layout(height=520, bargap=0.3, **CHART_LAYOUT)
        st.plotly_chart(fig, use_container_width=True)

# 3) คะแนนเฉลี่ยตามปี
with right:
    st.subheader("คะแนนเฉลี่ยตามปีที่ฉาย")
    year_df = (view.groupby("year").agg(mean_score=("avg_score", "mean"), count=("title", "count"))
               .reset_index())
    fig = px.line(year_df, x="year", y="mean_score", markers=True, custom_data=["count"],
                  labels={"year": "ปี", "mean_score": "คะแนนเฉลี่ย"})
    fig.update_traces(line=dict(color=SINGLE_COLOR, width=2), marker=dict(size=8),
                      hovertemplate="ปี %{x}<br>คะแนนเฉลี่ย %{y:.2f}<br>%{customdata[0]} เรื่อง<extra></extra>")
    fig.update_layout(height=520, hovermode="x unified", **CHART_LAYOUT)
    st.plotly_chart(fig, use_container_width=True)

# 4) ประวัติคะแนนของเรื่องที่เลือก
st.subheader("ประวัติคะแนนตามเวลา")
choice = st.selectbox("เลือกอนิเมะ", view["merged_id"].tolist(),
                      format_func=dict(zip(view["merged_id"], view["title"])).get)
keys = view.loc[view["merged_id"] == choice, "source_keys"].iloc[0] or []
history = query(
    "SELECT source, snapshot_date, score_10, rank FROM score_history "
    "WHERE source || ':' || source_id = ANY(%s) ORDER BY snapshot_date",
    (list(keys),),
)
history = history[history["source"].isin(picked_sources)]
if history.empty:
    st.info("ยังไม่มีประวัติคะแนนของเรื่องนี้")
else:
    history["score_10"] = history["score_10"].astype(float)
    history["เว็บ"] = history["source"].map(SOURCE_LABELS)
    fig = px.line(history, x="snapshot_date", y="score_10", color="เว็บ", markers=True,
                  color_discrete_map=SOURCE_COLORS, custom_data=["rank"],
                  labels={"snapshot_date": "วันที่", "score_10": "คะแนน (0–10)"})
    fig.update_traces(line=dict(width=2), marker=dict(size=8),
                      hovertemplate="%{fullData.name}: %{y:.2f} (อันดับ %{customdata[0]})<extra></extra>")
    fig.update_layout(height=400, hovermode="x unified", xaxis_tickformat="%d %b %Y",
                      legend=dict(orientation="h", y=1.05, x=0), **CHART_LAYOUT)
    st.plotly_chart(fig, use_container_width=True)
    if history["snapshot_date"].nunique() == 1:
        st.caption("มีข้อมูลเพียง 1 วัน — เส้นกราฟจะเกิดขึ้นเมื่อ DAG รันครบหลายวัน")

footer()
