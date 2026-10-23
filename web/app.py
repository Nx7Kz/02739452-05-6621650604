"""Dashboard แผ่นดินไหวเอเชียตะวันออกเฉียงใต้ — อ่านข้อมูลจาก PostgreSQL โดยตรง"""
import html
import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="SEA Earthquakes", page_icon="🌏", layout="wide")

# ---------- design tokens (ต้องตรงกับ .streamlit/config.toml)
FONT = "IBM Plex Sans Thai, Source Sans Pro, sans-serif"
C = dict(bg="#0b1020", card="#121a2e", border="#1f2a44", text="#e5e7eb", muted="#94a3b8", accent="#f97316")
SRC_COLORS = {"USGS": "#38bdf8", "EMSC": "#a78bfa", "TMD": "#34d399"}
MAG_LABELS = ["M < 3", "M 3–4", "M 4–5", "M ≥ 5"]
MAG_COLORS = dict(zip(MAG_LABELS, ["#fde68a", "#fbbf24", "#f97316", "#dc2626"]))
MAG_SCALE = [[0, "#fde68a"], [0.45, "#f97316"], [1, "#b91c1c"]]
STATUS_COLORS = {"success": "#22c55e", "degraded": "#eab308", "failed": "#ef4444"}
PLOT_CONFIG = {"displayModeBar": False}

st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans+Thai:wght@400;500;600;700&display=swap');
html, body, .stApp, p, li, label, input, textarea, button, h1, h2, h3, h4, h5, h6,
[data-testid="stMarkdownContainer"], [data-testid="stMetricValue"], [data-testid="stMetricLabel"],
div[data-baseweb="select"] div, div[data-baseweb="tab-list"] p {{ font-family: {FONT}; }}
.block-container {{ padding-top: 3.5rem; padding-bottom: 3rem; max-width: 1400px; }}
[data-testid="stVerticalBlockBorderWrapper"]:has(> div > [data-testid="stVerticalBlock"]) {{
  background: {C['card']}; border-color: {C['border']} !important; border-radius: 14px; }}
[data-testid="stSidebar"] {{ border-right: 1px solid {C['border']}; }}
div[data-baseweb="tab-list"] {{ gap: .25rem; }}
div[data-baseweb="tab-list"] p {{ font-size: 1rem; font-weight: 500; }}

.hero {{ padding: 1.4rem 1.6rem; border-radius: 16px; border: 1px solid {C['border']};
  background: radial-gradient(1200px 300px at 0% 0%, rgba(249,115,22,.18), transparent 60%),
              linear-gradient(135deg, #121a2e, #0e1527); margin-bottom: 1rem; }}
.hero .eyebrow {{ color: {C['accent']}; font-size: .78rem; letter-spacing: .14em; font-weight: 600; }}
.hero h1 {{ font-size: 1.9rem; font-weight: 700; margin: .2rem 0 .3rem; padding: 0; color: {C['text']}; }}
.hero p {{ color: {C['muted']}; margin: 0 0 .8rem; }}
.chips {{ display: flex; flex-wrap: wrap; gap: .4rem; }}
.chip {{ font-size: .78rem; padding: .18rem .65rem; border-radius: 999px; border: 1px solid {C['border']};
  color: {C['text']}; background: rgba(255,255,255,.03); }}
.chip i {{ display: inline-block; width: .5rem; height: .5rem; border-radius: 50%; margin-right: .35rem; }}

.kpis {{ display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: .75rem; margin-bottom: 1rem; }}
@media (max-width: 900px) {{ .kpis {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }} }}
.kpi {{ background: {C['card']}; border: 1px solid {C['border']}; border-radius: 14px; padding: .9rem 1rem;
  border-top: 3px solid transparent; }}
.kpi .l {{ color: {C['muted']}; font-size: .82rem; }}
.kpi .v {{ color: {C['text']}; font-size: 1.75rem; font-weight: 700; line-height: 1.25; }}
.kpi .s {{ color: {C['muted']}; font-size: .76rem; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}

.sec {{ font-weight: 600; color: {C['text']}; font-size: 1.02rem; margin-bottom: .1rem; }}
.sec-sub {{ color: {C['muted']}; font-size: .8rem; margin-bottom: .4rem; }}

.top-item {{ display: flex; gap: .75rem; align-items: center; padding: .55rem 0;
  border-bottom: 1px solid {C['border']}; }}
.top-item:last-child {{ border-bottom: 0; }}
.mag {{ min-width: 3.1rem; text-align: center; font-weight: 700; border-radius: 10px; padding: .35rem .2rem;
  color: #111827; font-size: 1.05rem; }}
.top-item .r {{ color: {C['text']}; font-size: .9rem; line-height: 1.3; }}
.top-item .m {{ color: {C['muted']}; font-size: .76rem; }}

.src-card {{ background: {C['card']}; border: 1px solid {C['border']}; border-radius: 14px; padding: 1rem 1.1rem;
  border-left: 4px solid transparent; }}
.src-card .n {{ font-weight: 700; font-size: 1.05rem; color: {C['text']}; }}
.src-card .st {{ font-size: .8rem; font-weight: 600; }}
.src-card .v {{ font-size: 1.6rem; font-weight: 700; color: {C['text']}; }}
.src-card .s {{ color: {C['muted']}; font-size: .78rem; }}
.banner {{ border-radius: 14px; padding: 1rem 1.2rem; margin-bottom: 1rem; border: 1px solid;
  color: {C['text']}; }}
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def engine():
    return create_engine(
        "postgresql+psycopg2://{u}:{p}@{h}:{port}/{d}".format(
            u=os.getenv("DATA_DB_USER", "airflow"), p=os.getenv("DATA_DB_PASSWORD", ""),
            h=os.getenv("DATA_DB_HOST", "localhost"), port=os.getenv("DATA_DB_PORT", "5432"),
            d=os.getenv("DATA_DB_NAME", "quakes"),
        ),
        pool_pre_ping=True,
    )


@st.cache_data(ttl=60)
def q(sql: str) -> pd.DataFrame:
    try:
        with engine().connect() as c:
            return pd.read_sql(text(sql), c)
    except Exception as e:
        st.warning(f"ยังอ่านตารางไม่ได้ (รัน DAG ก่อน): {e.__class__.__name__}")
        return pd.DataFrame()


def style(fig, height):
    """ธีมกราฟเดียวกันทั้งหน้า"""
    fig.update_layout(
        height=height, margin=dict(l=4, r=4, t=4, b=4), font=dict(family=FONT, color=C["muted"], size=12),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", bargap=0.18,
        legend=dict(orientation="h", y=1.02, yanchor="bottom", x=0, title=None, font=dict(color=C["text"])),
        hoverlabel=dict(font_family=FONT, bgcolor=C["card"], bordercolor=C["border"], font_color=C["text"]),
    )
    # plotly.js ใน Streamlit 1.39 ไม่เปิด automargin เอง → ต้องเปิดไม่งั้น tick label หลุดขอบ
    fig.update_xaxes(gridcolor=C["border"], zeroline=False, linecolor=C["border"], automargin=True)
    fig.update_yaxes(gridcolor=C["border"], zeroline=False, linecolor=C["border"], automargin=True)
    return fig


def chart(fig):
    st.plotly_chart(fig, use_container_width=True, theme=None, config=PLOT_CONFIG)


def section(title, sub=""):
    st.markdown(f'<div class="sec">{title}</div>' + (f'<div class="sec-sub">{sub}</div>' if sub else ""),
                unsafe_allow_html=True)


def fmt_th(ts):
    return pd.Timestamp(ts).tz_convert("Asia/Bangkok").strftime("%d %b %Y %H:%M น.")


runs = q("SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT 30")
last_update = fmt_th(runs.iloc[0]["finished_at"]) if not runs.empty and pd.notna(runs.iloc[0]["finished_at"]) else "-"
dot = lambda c: f'<i style="background:{c}"></i>'  # noqa: E731
st.markdown(f"""
<div class="hero">
  <div class="eyebrow">SEA SEISMIC MONITOR · 30 วันล่าสุด</div>
  <h1>🌏 แผ่นดินไหวในเอเชียตะวันออกเฉียงใต้</h1>
  <p>รวมข้อมูลจาก 3 แหล่ง ตัดรายการซ้ำข้ามแหล่ง ด้วย Apache Airflow → PostgreSQL</p>
  <div class="chips">
    <span class="chip">{dot(SRC_COLORS['USGS'])}USGS API</span>
    <span class="chip">{dot(SRC_COLORS['EMSC'])}EMSC API</span>
    <span class="chip">{dot(SRC_COLORS['TMD'])}กรมอุตุนิยมวิทยา (scraping)</span>
    <span class="chip">🕒 อัปเดตล่าสุด {last_update}</span>
  </div>
</div>
""", unsafe_allow_html=True)

tab_dash, tab_status = st.tabs(["📊 Dashboard", "🛠️ Pipeline status"])

with tab_dash:
    df = q("SELECT * FROM earthquakes ORDER BY event_time_utc DESC")
    if df.empty:
        st.info("ยังไม่มีข้อมูล — เปิด Airflow (http://localhost:8080) แล้ว trigger DAG sea_earthquake_pipeline")
    else:
        df["event_time_utc"] = pd.to_datetime(df["event_time_utc"], utc=True)
        df["เวลาไทย"] = df["event_time_utc"].dt.tz_convert("Asia/Bangkok")

        # ---------- filters
        with st.sidebar:
            st.markdown("### 🎚️ ตัวกรอง")
            mmin, mmax = float(df["magnitude"].min()), float(df["magnitude"].max())
            mag = st.slider("ขนาด (magnitude)", mmin, mmax, (mmin, mmax), 0.1)
            dmax = float(df["depth_km"].fillna(0).max()) or 1.0
            depth = st.slider("ความลึก (กม.)", 0.0, dmax, (0.0, dmax), 1.0)
            days = st.slider("ย้อนหลัง (วัน)", 1, 30, 30)
            srcs = st.multiselect("แหล่งข้อมูลที่รายงาน", ["USGS", "EMSC", "TMD"], ["USGS", "EMSC", "TMD"])
            multi_only = st.checkbox("เฉพาะเหตุการณ์ที่ยืนยันจาก ≥2 แหล่ง")
            region_kw = st.text_input("ค้นหาบริเวณ", placeholder="เช่น Myanmar, Indonesia")
            st.caption(f"ข้อมูลทั้งหมด {len(df):,} เหตุการณ์ · อัปเดตล่าสุด {last_update}")

        since = datetime.now(timezone.utc) - timedelta(days=days)
        f = df[
            df["magnitude"].between(*mag)
            & df["depth_km"].fillna(0).between(*depth)
            & (df["event_time_utc"] >= since)
            & df["sources"].apply(lambda s: any(x in s.split(",") for x in srcs))
        ]
        if multi_only:
            f = f[f["source_count"] >= 2]
        if region_kw:
            f = f[f["region"].fillna("").str.contains(region_kw, case=False)]

        if f.empty:
            st.info("ไม่มีเหตุการณ์ที่ตรงกับตัวกรอง — ลองขยายช่วงขนาด/ความลึก/วัน")
        else:
            f = f.assign(
                mag_class=pd.cut(f["magnitude"], [-10, 3, 4, 5, 20], right=False, labels=MAG_LABELS),
                time_str=f["เวลาไทย"].dt.strftime("%d %b %Y %H:%M น."),
                sources_label=f["sources"].str.replace(",", " + "),
            )

            # ---------- KPI
            top = f.loc[f["magnitude"].idxmax()]
            kpis = [
                ("จำนวนเหตุการณ์", f"{len(f):,}", f"ในช่วง {days} วันล่าสุด", C["accent"]),
                ("ขนาดสูงสุด", f"M {top['magnitude']:.1f}", top["region"] or "-", MAG_COLORS["M ≥ 5"]),
                ("ขนาดเฉลี่ย", f"{f['magnitude'].mean():.2f}", f"ลึกเฉลี่ย {f['depth_km'].mean():.0f} กม.", "#fbbf24"),
                ("M ≥ 5.0", f"{int((f['magnitude'] >= 5).sum()):,}", f"M ≥ 4.0: {int((f['magnitude'] >= 4).sum()):,} ครั้ง",
                 "#ef4444"),
                ("ยืนยัน ≥2 แหล่ง", f"{(f['source_count'] >= 2).mean() * 100:.0f}%",
                 f"{int((f['source_count'] >= 2).sum()):,} เหตุการณ์", SRC_COLORS["USGS"]),
            ]
            st.markdown('<div class="kpis">' + "".join(
                f'<div class="kpi" style="border-top-color:{c}"><div class="l">{l}</div><div class="v">{v}</div>'
                f'<div class="s" title="{html.escape(str(s))}">{html.escape(str(s))}</div></div>'
                for l, v, s, c in kpis) + "</div>", unsafe_allow_html=True)

            # ---------- map + top events
            c1, c2 = st.columns([7, 3])
            with c1, st.container(border=True):
                section("แผนที่ศูนย์กลางแผ่นดินไหว", "ขนาดและสีของจุดตามแมกนิจูด · ชี้ที่จุดเพื่อดูรายละเอียด")
                fig = px.scatter_mapbox(
                    f.sort_values("magnitude"), lat="latitude", lon="longitude", color="magnitude",
                    size=f.sort_values("magnitude")["magnitude"].clip(lower=0.5) ** 2, size_max=11,
                    hover_name="region", custom_data=["magnitude", "depth_km", "time_str", "sources_label"],
                    color_continuous_scale=MAG_SCALE, zoom=3.1, center={"lat": 8.5, "lon": 116.5}, opacity=0.75,
                )
                fig.update_traces(hovertemplate="<b>%{hovertext}</b><br>M %{customdata[0]:.1f} · ลึก %{customdata[1]:.0f} กม."
                                                "<br>%{customdata[2]}<br>แหล่ง: %{customdata[3]}<extra></extra>")
                # carto ต้องใช้ API key แล้ว → ใช้ tile Esri Dark Gray (ไม่ต้องใช้ key) แทน
                fig.update_layout(
                    mapbox_style="white-bg",
                    mapbox_layers=[{
                        "below": "traces", "sourcetype": "raster",
                        "sourceattribution": "Tiles © Esri — Esri, HERE, Garmin, © OpenStreetMap contributors",
                        "source": ["https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/"
                                   "World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}"],
                    }],
                    coloraxis_colorbar=dict(title="M", thickness=10, len=0.6, x=0.99, xanchor="right",
                                            bgcolor="rgba(11,16,32,.6)", tickfont=dict(color=C["text"])),
                )
                chart(style(fig, 540))
            with c2, st.container(border=True):
                section("5 เหตุการณ์ใหญ่ที่สุด", "เรียงตามแมกนิจูด")
                items = []
                for _, r in f.nlargest(5, "magnitude").iterrows():
                    items.append(
                        f'<div class="top-item"><div class="mag" style="background:{MAG_COLORS[r["mag_class"]]}">'
                        f'{r["magnitude"]:.1f}</div><div><div class="r">{html.escape(r["region"] or "-")}</div>'
                        f'<div class="m">{r["time_str"]} · ลึก {r["depth_km"]:.0f} กม. · {r["sources_label"]}</div>'
                        f"</div></div>")
                st.markdown("".join(items), unsafe_allow_html=True)
                section("สัดส่วนตามระดับขนาด")
                cls = f["mag_class"].value_counts().reindex(MAG_LABELS).fillna(0).reset_index()
                cls.columns = ["mag_class", "events"]
                fig = px.pie(cls, names="mag_class", values="events", hole=0.62, color="mag_class",
                             color_discrete_map=MAG_COLORS)
                fig.update_traces(textinfo="percent", textfont_color="#111827", sort=False,
                                  hovertemplate="%{label}: %{value:,} ครั้ง<extra></extra>")
                style(fig, 200).update_layout(legend=dict(orientation="v", x=1, y=0.5, yanchor="middle"))
                chart(fig)

            # ---------- trend + distribution
            c3, c4 = st.columns([3, 2])
            with c3, st.container(border=True):
                section("จำนวนเหตุการณ์ต่อวัน", "แยกตามระดับขนาด (เวลาไทย)")
                daily = (f.groupby([f["เวลาไทย"].dt.date.rename("date"), "mag_class"], observed=True)
                         .size().rename("events").reset_index())
                fig = px.bar(daily, x="date", y="events", color="mag_class", color_discrete_map=MAG_COLORS,
                             category_orders={"mag_class": MAG_LABELS}, labels={"date": "", "events": "เหตุการณ์"})
                fig.update_traces(hovertemplate="%{x|%d %b}: %{y} ครั้ง<extra></extra>")
                chart(style(fig, 300))
            with c4, st.container(border=True):
                section("การกระจายของขนาด", "histogram ของแมกนิจูด")
                fig = px.histogram(f, x="magnitude", nbins=30, color="mag_class", color_discrete_map=MAG_COLORS,
                                   category_orders={"mag_class": MAG_LABELS},
                                   labels={"magnitude": "แมกนิจูด", "count": "เหตุการณ์"})
                fig.update_layout(showlegend=False, yaxis_title="เหตุการณ์")
                fig.update_traces(hovertemplate="M %{x}: %{y} ครั้ง<extra></extra>")
                chart(style(fig, 300))

            # ---------- sources + depth
            c5, c6 = st.columns([2, 3])
            with c5, st.container(border=True):
                section("เหตุการณ์ตามแหล่งที่ยืนยัน", "แหล่งที่รายงานเหตุการณ์เดียวกัน (หลังตัดซ้ำ)")
                combo = (f.groupby(["sources_label", "source_count"]).size().rename("events").reset_index()
                         .sort_values("events"))
                combo["ยืนยัน"] = combo["source_count"].map(lambda n: f"{n} แหล่ง")
                fig = px.bar(combo, x="events", y="sources_label", orientation="h", color="ยืนยัน", text="events",
                             color_discrete_map={"1 แหล่ง": "#475569", "2 แหล่ง": "#f59e0b", "3 แหล่ง": "#22c55e"},
                             category_orders={"ยืนยัน": ["1 แหล่ง", "2 แหล่ง", "3 แหล่ง"]},
                             labels={"events": "", "sources_label": ""})
                fig.update_traces(textposition="outside", cliponaxis=False, textfont_color=C["text"],
                                  hovertemplate="%{y}: %{x:,} ครั้ง<extra></extra>")
                fig.update_yaxes(categoryorder="total ascending")
                fig.update_xaxes(range=[0, combo["events"].max() * 1.15], showticklabels=False)
                chart(style(fig, 320))
            with c6, st.container(border=True):
                section("ความลึก vs ขนาด", "สีตามแหล่งหลักที่ใช้ค่า (USGS > EMSC > TMD)")
                fig = px.scatter(f, x="depth_km", y="magnitude", color="primary_source", opacity=0.7,
                                 color_discrete_map=SRC_COLORS, category_orders={"primary_source": list(SRC_COLORS)},
                                 hover_name="region", custom_data=["time_str"],
                                 labels={"depth_km": "ความลึก (กม.)", "magnitude": "แมกนิจูด"})
                fig.update_traces(marker=dict(size=7, line=dict(width=0)),
                                  hovertemplate="<b>%{hovertext}</b><br>M %{y:.1f} · ลึก %{x:.0f} กม.<br>"
                                                "%{customdata[0]}<extra></extra>")
                chart(style(fig, 320))

            # ---------- table
            with st.container(border=True):
                t1, t2 = st.columns([4, 1])
                with t1:
                    section("ตารางข้อมูล", f"{len(f):,} เหตุการณ์ตามตัวกรอง")
                cols = ["เวลาไทย", "magnitude", "mag_type", "depth_km", "latitude", "longitude", "region",
                        "sources", "mag_usgs", "mag_emsc", "mag_tmd"]
                with t2:
                    st.download_button("⬇️ ดาวน์โหลด CSV", f[cols].to_csv(index=False).encode("utf-8-sig"),
                                       "sea_earthquakes.csv", "text/csv", use_container_width=True)
                st.dataframe(
                    f[cols], use_container_width=True, hide_index=True, height=420,
                    column_config={
                        "เวลาไทย": st.column_config.DatetimeColumn("เวลา (ไทย)", format="D MMM YYYY, HH:mm"),
                        "magnitude": st.column_config.ProgressColumn("ขนาด", format="%.1f", min_value=0,
                                                                     max_value=max(8.0, float(f["magnitude"].max()))),
                        "mag_type": st.column_config.TextColumn("ชนิด", width="small"),
                        "depth_km": st.column_config.NumberColumn("ลึก (กม.)", format="%.0f"),
                        "latitude": st.column_config.NumberColumn("ละติจูด", format="%.2f"),
                        "longitude": st.column_config.NumberColumn("ลองจิจูด", format="%.2f"),
                        "region": st.column_config.TextColumn("บริเวณ", width="large"),
                        "sources": st.column_config.TextColumn("แหล่งที่รายงาน"),
                        "mag_usgs": st.column_config.NumberColumn("M USGS", format="%.1f"),
                        "mag_emsc": st.column_config.NumberColumn("M EMSC", format="%.1f"),
                        "mag_tmd": st.column_config.NumberColumn("M TMD", format="%.1f"),
                    },
                )

with tab_status:
    if runs.empty:
        st.info("ยังไม่มีประวัติการรัน pipeline")
    else:
        last = runs.iloc[0]
        sc = STATUS_COLORS.get(last["status"], C["muted"])
        st.markdown(
            f'<div class="banner" style="border-color:{sc};background:{sc}1f">รันล่าสุด: <b style="color:{sc}">{html.escape(str(last["status"]).upper())}</b>'
            f' · เสร็จเมื่อ {last_update} · รวมหลังตัดซ้ำ {int(last["merged_rows"] or 0):,} เหตุการณ์'
            f' · ไม่ผ่าน DQ {int(last["rejected_rows"] or 0):,} แถว · <code>{html.escape(str(last["run_id"]))}</code></div>',
            unsafe_allow_html=True)
        s = st.columns(3)
        for col, name in zip(s, ["usgs", "emsc", "tmd"]):
            status = str(last[f"{name}_status"])
            c = STATUS_COLORS["success"] if status == "success" else STATUS_COLORS["failed"]
            col.markdown(
                f'<div class="src-card" style="border-left-color:{c}"><div class="n">{name.upper()}</div>'
                f'<div class="st" style="color:{c}">● {html.escape(status)}</div><div class="v">{int(last[f"{name}_rows"] or 0):,}</div>'
                f'<div class="s">แถวที่ดึงได้ในรันล่าสุด</div></div>', unsafe_allow_html=True)

        st.write("")
        with st.container(border=True):
            section("จำนวนแถวที่ดึงได้ต่อรัน", "แยกตามแหล่ง — แท่งหายแปลว่าแหล่งนั้นล่ม/ถูกจำลองให้ล่ม")
            hist = runs.sort_values("started_at").tail(15).copy()
            hist["run"] = hist["started_at"].map(lambda t: fmt_th(t) if pd.notna(t) else "-")
            long = hist.melt(id_vars=["run"], value_vars=["usgs_rows", "emsc_rows", "tmd_rows"],
                             var_name="source", value_name="rows")
            long["source"] = long["source"].str.replace("_rows", "").str.upper()
            fig = px.bar(long, x="run", y="rows", color="source", barmode="group", color_discrete_map=SRC_COLORS,
                         labels={"run": "", "rows": "แถว"})
            chart(style(fig, 280))

        with st.container(border=True):
            section("ประวัติการรัน pipeline", "30 รันล่าสุด")
            st.dataframe(
                runs, use_container_width=True, hide_index=True,
                column_config={
                    "run_id": st.column_config.TextColumn("Run ID", width="medium"),
                    "started_at": st.column_config.DatetimeColumn("เริ่ม", format="D MMM, HH:mm:ss"),
                    "finished_at": st.column_config.DatetimeColumn("เสร็จ", format="D MMM, HH:mm:ss"),
                    "status": "สถานะ", "dq_branch": "DQ branch", "message": "ข้อความ",
                    "merged_rows": "หลังตัดซ้ำ", "rejected_rows": "ไม่ผ่าน DQ",
                },
            )

    counts = q("""SELECT 'raw_events' t, count(*) n FROM raw_events
                  UNION ALL SELECT 'earthquakes', count(*) FROM earthquakes
                  UNION ALL SELECT 'dq_rejects', count(*) FROM dq_rejects""")
    if not counts.empty:
        labels = {"raw_events": ("ตาราง raw_events", "ข้อมูลดิบทุกแหล่ง (UPSERT)", "#64748b"),
                  "earthquakes": ("ตาราง earthquakes", "หลังตัดซ้ำ/ผ่าน DQ", C["accent"]),
                  "dq_rejects": ("ตาราง dq_rejects", "quarantine", "#ef4444")}
        st.markdown('<div class="kpis" style="grid-template-columns:repeat(3,minmax(0,1fr))">' + "".join(
            f'<div class="kpi" style="border-top-color:{labels[t][2]}"><div class="l">{labels[t][0]}</div>'
            f'<div class="v">{int(n):,}</div><div class="s">{labels[t][1]}</div></div>'
            for t, n in counts.itertuples(index=False)) + "</div>", unsafe_allow_html=True)
    rej = q("SELECT event_id, problems, rejected_at FROM dq_rejects ORDER BY rejected_at DESC LIMIT 50")
    if not rej.empty:
        with st.container(border=True):
            section("รายการที่ไม่ผ่านการตรวจคุณภาพ (quarantine)")
            st.dataframe(rej, use_container_width=True, hide_index=True)
