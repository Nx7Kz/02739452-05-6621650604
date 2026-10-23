"""
SEA Earthquake pipeline — รวมข้อมูลแผ่นดินไหวเอเชียตะวันออกเฉียงใต้ 30 วันล่าสุด
แหล่งข้อมูล: 1) USGS FDSN API  2) EMSC FDSN API  3) กรมอุตุนิยมวิทยา (web scraping, ไม่ใช้ Selenium)

Flow:
  create_tables
    >> [TaskGroup extract_sources: usgs(extract>>transform) | emsc(...) | tmd(...)]  (รันขนาน)
    >> stage_raw (ALL_DONE: แหล่งไหนล่มก็ไปต่อได้, UPSERT raw_events)
    >> merge_dedupe (ตัดรายการซ้ำข้ามแหล่ง)
    >> dq_check (branch) -> handle_bad_rows | load_curated
    >> load_curated (UPSERT earthquakes)
    >> log_run (ALL_DONE: บันทึกสถานะลง pipeline_runs สำหรับหน้า status บนเว็บ)

ทดสอบกรณีแหล่งล่ม: Trigger DAG w/ config  {"simulate_down": "emsc"}  (คั่นหลายแหล่งด้วย ,)
ทดสอบเส้นทาง DQ:   Trigger DAG w/ config  {"simulate_bad_rows": 4}  (เพิ่มแถวเสียจำลอง → dq_rejects)
"""
from __future__ import annotations

import html
import logging
import math
import os
import re
import time
from datetime import datetime, timedelta, timezone
from urllib import robotparser

import psycopg2
import requests
from psycopg2.extras import execute_values

from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context
from airflow.utils.trigger_rule import TriggerRule

log = logging.getLogger(__name__)

# ---------------------------------------------------------------- config
BBOX = {"minlat": -11.0, "maxlat": 28.5, "minlon": 92.0, "maxlon": 141.5}  # Southeast Asia
DAYS = 30
UA = "KU-02739452-student-project/1.0 (educational, non-commercial)"
HTTP_TIMEOUT = 60
SOURCE_PRIORITY = {"USGS": 0, "EMSC": 1, "TMD": 2}
DEDUP_SECONDS = 90      # เหตุการณ์ต่างแหล่งที่เวลาห่างกัน <= 90 วินาที
DEDUP_KM = 100          # และศูนย์กลางห่างกัน <= 100 กม. ถือเป็นเหตุการณ์เดียวกัน
TMD_BASE = "https://earthquake.tmd.go.th"
TMD_LIST = f"{TMD_BASE}/inside.html"
TMD_MAX_PAGES = 150

SOURCES = ["usgs", "emsc", "tmd"]


def db_conn():
    return psycopg2.connect(
        host=os.getenv("DATA_DB_HOST", "postgres"),
        port=int(os.getenv("DATA_DB_PORT", "5432")),
        dbname=os.getenv("DATA_DB_NAME", "quakes"),
        user=os.getenv("DATA_DB_USER", "airflow"),
        password=os.getenv("DATA_DB_PASSWORD", ""),
    )


def window():
    end = datetime.now(timezone.utc)
    return end - timedelta(days=DAYS), end


def check_simulated_down(source: str):
    ctx = get_current_context()
    conf = (ctx["dag_run"].conf or {}) if ctx.get("dag_run") else {}
    down = conf.get("simulate_down") or ctx["params"].get("simulate_down") or ""
    if source.lower() in [s.strip().lower() for s in str(down).split(",") if s.strip()]:
        raise RuntimeError(f"[simulated outage] source {source} is down")


def simulated_bad_rows(merged: list[dict]) -> list[dict]:
    """สร้างแถวเสียจำลอง N แถว (conf simulate_bad_rows) เพื่อทดสอบเส้นทาง DQ → quarantine"""
    ctx = get_current_context()
    conf = (ctx["dag_run"].conf or {}) if ctx.get("dag_run") else {}
    n = int(conf.get("simulate_bad_rows") or ctx["params"].get("simulate_bad_rows") or 0)
    if n <= 0 or not merged:
        return []
    future = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
    defects = [{"magnitude": 12.5}, {"depth_km": 950.0}, {"event_time_utc": future},
               {"latitude": None, "longitude": None}]
    out = []
    for i in range(n):
        base = merged[i % len(merged)]
        out.append({**base, **defects[i % len(defects)], "event_id": f"SIMULATED:{i + 1}",
                    "region": f"[simulated bad row] {base.get('region') or ''}".strip()})
    log.warning("[simulated bad rows] injected %d bad events", n)
    return out


def in_scope(r: dict, start: datetime) -> bool:
    try:
        t = datetime.fromisoformat(r["event_time_utc"])
    except Exception:
        return True  # ปล่อยให้ dq_check จับ
    return (
        t >= start
        and r["latitude"] is not None and r["longitude"] is not None
        and BBOX["minlat"] <= r["latitude"] <= BBOX["maxlat"]
        and BBOX["minlon"] <= r["longitude"] <= BBOX["maxlon"]
    )


def fnum(v):
    try:
        return None if v is None or v == "" else float(v)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------- USGS
def extract_usgs_fn():
    check_simulated_down("usgs")
    start, end = window()
    params = {
        "format": "geojson", "orderby": "time", "limit": 20000,
        "starttime": start.strftime("%Y-%m-%dT%H:%M:%S"),
        "endtime": end.strftime("%Y-%m-%dT%H:%M:%S"),
        "minlatitude": BBOX["minlat"], "maxlatitude": BBOX["maxlat"],
        "minlongitude": BBOX["minlon"], "maxlongitude": BBOX["maxlon"],
    }
    r = requests.get("https://earthquake.usgs.gov/fdsnws/event/1/query",
                     params=params, headers={"User-Agent": UA}, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    feats = r.json().get("features", [])
    log.info("USGS: %d features", len(feats))
    return feats


def transform_usgs_fn(feats):
    start, _ = window()
    out = []
    for f in feats or []:
        p, g = f.get("properties", {}), (f.get("geometry") or {}).get("coordinates", [None, None, None])
        if p.get("time") is None:
            continue
        rec = {
            "source": "USGS",
            "source_event_id": str(f.get("id")),
            "event_time_utc": datetime.fromtimestamp(p["time"] / 1000, tz=timezone.utc).isoformat(),
            "latitude": fnum(g[1]), "longitude": fnum(g[0]), "depth_km": fnum(g[2]),
            "magnitude": fnum(p.get("mag")), "mag_type": p.get("magType"),
            "region": p.get("place"),
        }
        if in_scope(rec, start):
            out.append(rec)
    return out


# ---------------------------------------------------------------- EMSC
def extract_emsc_fn():
    check_simulated_down("emsc")
    start, end = window()
    params = {
        "format": "json", "orderby": "time", "limit": 20000,
        "start": start.strftime("%Y-%m-%dT%H:%M:%S"), "end": end.strftime("%Y-%m-%dT%H:%M:%S"),
        "minlat": BBOX["minlat"], "maxlat": BBOX["maxlat"],
        "minlon": BBOX["minlon"], "maxlon": BBOX["maxlon"],
    }
    r = requests.get("https://www.seismicportal.eu/fdsnws/event/1/query",
                     params=params, headers={"User-Agent": UA}, timeout=HTTP_TIMEOUT)
    if r.status_code == 204:
        return []
    r.raise_for_status()
    feats = r.json().get("features", [])
    log.info("EMSC: %d features", len(feats))
    return feats


def transform_emsc_fn(feats):
    start, _ = window()
    out = []
    for f in feats or []:
        p = f.get("properties", {})
        t = p.get("time")
        if not t:
            continue
        t = datetime.fromisoformat(t.replace("Z", "+00:00"))
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        depth = fnum(p.get("depth"))
        rec = {
            "source": "EMSC",
            "source_event_id": str(p.get("unid") or f.get("id")),
            "event_time_utc": t.astimezone(timezone.utc).isoformat(),
            "latitude": fnum(p.get("lat")), "longitude": fnum(p.get("lon")),
            "depth_km": abs(depth) if depth is not None else None,  # EMSC บางรายการให้ depth ติดลบ
            "magnitude": fnum(p.get("mag")), "mag_type": p.get("magtype"),
            "region": (p.get("flynn_region") or "").title() or None,
        }
        if in_scope(rec, start):
            out.append(rec)
    return out


# ---------------------------------------------------------------- TMD (web scraping)
TMD_ROW = re.compile(
    r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+UTC\s+"      # UTC time
    r"(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)\s+"  # mag lat lon
    r"(-?\d+(?:\.\d+)?)\s+(\d+)\s+"                          # depth phase
    r"(.+?)(?=\s\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}|\sเหตุการณ์ที่|$)",
    re.S,
)


def html_to_text(raw: str) -> str:
    raw = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", raw)
    txt = html.unescape(re.sub(r"<[^>]+>", " ", raw))
    return re.sub(r"\s+", " ", txt)


def parse_tmd_page(raw_html: str) -> list[dict]:
    rows = []
    for m in TMD_ROW.finditer(html_to_text(raw_html)):
        t, mag, lat, lon, depth, phase, region = m.groups()
        rows.append({
            "utc": t, "mag": mag, "lat": lat, "lon": lon,
            "depth": depth, "phase": phase, "region": region.strip()[:200],
        })
    return rows


def extract_tmd_fn():
    check_simulated_down("tmd")
    # เคารพ robots.txt
    rp = robotparser.RobotFileParser(f"{TMD_BASE}/robots.txt")
    try:
        rp.read()
        if not rp.can_fetch(UA, TMD_LIST):
            raise RuntimeError("robots.txt disallows scraping inside.html")
    except RuntimeError:
        raise
    except Exception as e:  # ไม่มี robots.txt หรืออ่านไม่ได้ = ไม่มีข้อห้าม
        log.warning("robots.txt unreadable (%s) — proceeding politely", e)

    start, _ = window()
    sess = requests.Session()
    sess.headers["User-Agent"] = UA
    first = sess.get(TMD_LIST, timeout=HTTP_TIMEOUT)
    first.raise_for_status()
    first.encoding = first.apparent_encoding or "utf-8"
    all_rows = parse_tmd_page(first.text)
    if not all_rows:
        raise RuntimeError("TMD page parsed 0 rows — layout may have changed")

    # หา parameter ของ pagination จากลิงก์ในหน้า (เช่น pageNum_xxx)
    m = re.search(r"[?&](pageNum_\w+)=\d+", first.text)
    page_param = m.group(1) if m else "pageNum_ShowEQ"
    seen = {r["utc"] + r["lat"] + r["lon"] for r in all_rows}

    for page in range(1, TMD_MAX_PAGES):
        oldest = min(datetime.fromisoformat(r["utc"]).replace(tzinfo=timezone.utc) for r in all_rows)
        if oldest < start:
            break
        time.sleep(1.0)  # polite crawling
        resp = sess.get(TMD_LIST, params={page_param: page}, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding or "utf-8"
        rows = parse_tmd_page(resp.text)
        new = [r for r in rows if r["utc"] + r["lat"] + r["lon"] not in seen]
        if not new:
            log.warning("TMD page %d returned no new rows — stop paging", page)
            break
        for r in new:
            seen.add(r["utc"] + r["lat"] + r["lon"])
        all_rows.extend(new)
    log.info("TMD: scraped %d rows", len(all_rows))
    return all_rows


def transform_tmd_fn(rows):
    start, _ = window()
    out = []
    for r in rows or []:
        t = datetime.strptime(r["utc"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        rec = {
            "source": "TMD",
            "source_event_id": f"{r['utc'].replace(' ', 'T')}_{r['lat']}_{r['lon']}",
            "event_time_utc": t.isoformat(),
            "latitude": fnum(r["lat"]), "longitude": fnum(r["lon"]),
            "depth_km": fnum(r["depth"]), "magnitude": fnum(r["mag"]),
            "mag_type": "ML", "region": r["region"],
        }
        if in_scope(rec, start):
            out.append(rec)
    return out


EXTRACTORS = {"usgs": extract_usgs_fn, "emsc": extract_emsc_fn, "tmd": extract_tmd_fn}
TRANSFORMERS = {"usgs": transform_usgs_fn, "emsc": transform_emsc_fn, "tmd": transform_tmd_fn}


# ---------------------------------------------------------------- dedupe
def haversine_km(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 6371 * 2 * math.asin(math.sqrt(a))


def dedupe(records: list[dict]) -> list[dict]:
    """จับกลุ่มเหตุการณ์เดียวกันข้ามแหล่ง: เวลา <= 90s และระยะ <= 100km, แหล่งละไม่เกิน 1 รายการต่อกลุ่ม"""
    recs = sorted(records, key=lambda r: r["event_time_utc"])
    for r in recs:
        r["_t"] = datetime.fromisoformat(r["event_time_utc"]).timestamp()
    clusters: list[dict] = []
    for r in recs:
        best, best_score = None, None
        for c in reversed(clusters):
            dt = r["_t"] - c["t"]
            if dt > DEDUP_SECONDS:
                break
            if r["source"] in c["members"]:
                continue
            d = haversine_km(r["latitude"], r["longitude"], c["lat"], c["lon"])
            if d <= DEDUP_KM:
                score = abs(dt) / DEDUP_SECONDS + d / DEDUP_KM
                if best is None or score < best_score:
                    best, best_score = c, score
        if best:
            best["members"][r["source"]] = r
        else:
            clusters.append({"t": r["_t"], "lat": r["latitude"], "lon": r["longitude"],
                             "members": {r["source"]: r}})
    merged = []
    for c in clusters:
        m = c["members"]
        primary = min(m.values(), key=lambda x: SOURCE_PRIORITY[x["source"]])
        srcs = sorted(m, key=SOURCE_PRIORITY.get)
        merged.append({
            "event_id": f"{primary['source']}:{primary['source_event_id']}",
            "event_time_utc": primary["event_time_utc"],
            "latitude": primary["latitude"], "longitude": primary["longitude"],
            "depth_km": primary["depth_km"], "magnitude": primary["magnitude"],
            "mag_type": primary["mag_type"],
            "region": (m.get("TMD") or primary)["region"] if "TMD" in m else primary["region"],
            "primary_source": primary["source"], "sources": ",".join(srcs),
            "source_count": len(srcs),
            "mag_usgs": m["USGS"]["magnitude"] if "USGS" in m else None,
            "mag_emsc": m["EMSC"]["magnitude"] if "EMSC" in m else None,
            "mag_tmd": m["TMD"]["magnitude"] if "TMD" in m else None,
        })
    return merged


def dq_problems(e: dict) -> list[str]:
    p = []
    if e["latitude"] is None or e["longitude"] is None:
        p.append("missing_coordinates")
    if e["magnitude"] is None:
        p.append("missing_magnitude")
    elif not (-2 <= e["magnitude"] <= 10):
        p.append("magnitude_out_of_range")
    if e["depth_km"] is not None and not (0 <= e["depth_km"] <= 800):
        p.append("depth_out_of_range")
    try:
        if datetime.fromisoformat(e["event_time_utc"]) > datetime.now(timezone.utc) + timedelta(minutes=5):
            p.append("time_in_future")
    except Exception:
        p.append("bad_time")
    return p


# ---------------------------------------------------------------- DDL
DDL = """
CREATE TABLE IF NOT EXISTS raw_events (
  source TEXT NOT NULL, source_event_id TEXT NOT NULL,
  event_time_utc TIMESTAMPTZ NOT NULL, latitude DOUBLE PRECISION, longitude DOUBLE PRECISION,
  depth_km DOUBLE PRECISION, magnitude DOUBLE PRECISION, mag_type TEXT, region TEXT,
  ingested_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (source, source_event_id));
CREATE TABLE IF NOT EXISTS earthquakes (
  event_id TEXT PRIMARY KEY, event_time_utc TIMESTAMPTZ NOT NULL,
  latitude DOUBLE PRECISION, longitude DOUBLE PRECISION, depth_km DOUBLE PRECISION,
  magnitude DOUBLE PRECISION, mag_type TEXT, region TEXT,
  primary_source TEXT, sources TEXT, source_count INT,
  mag_usgs DOUBLE PRECISION, mag_emsc DOUBLE PRECISION, mag_tmd DOUBLE PRECISION,
  updated_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS dq_rejects (
  event_id TEXT PRIMARY KEY, problems TEXT, payload JSONB, rejected_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS pipeline_runs (
  run_id TEXT PRIMARY KEY, started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ, status TEXT,
  usgs_status TEXT, emsc_status TEXT, tmd_status TEXT,
  usgs_rows INT, emsc_rows INT, tmd_rows INT,
  merged_rows INT, rejected_rows INT, dq_branch TEXT, message TEXT);
"""

RAW_COLS = ["source", "source_event_id", "event_time_utc", "latitude", "longitude",
            "depth_km", "magnitude", "mag_type", "region"]
EQ_COLS = ["event_id", "event_time_utc", "latitude", "longitude", "depth_km", "magnitude",
           "mag_type", "region", "primary_source", "sources", "source_count",
           "mag_usgs", "mag_emsc", "mag_tmd"]


# ---------------------------------------------------------------- DAG
@dag(
    dag_id="sea_earthquake_pipeline",
    description="แผ่นดินไหวเอเชียตะวันออกเฉียงใต้ 30 วัน: USGS + EMSC + TMD",
    schedule="0 */6 * * *",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=20)},
    params={"simulate_down": "", "simulate_bad_rows": 0},
    tags=["02739452", "earthquake", "final"],
)
def sea_earthquake_pipeline():

    @task
    def create_tables():
        with db_conn() as c, c.cursor() as cur:
            cur.execute(DDL)

    @task_group(group_id="extract_sources")
    def extract_sources():
        groups = []
        for src in SOURCES:
            @task_group(group_id=src)
            def one_source(s=src):
                @task(task_id="extract")
                def ex():
                    return EXTRACTORS[s]()

                @task(task_id="transform")
                def tr(raw):
                    rows = TRANSFORMERS[s](raw)
                    log.info("%s transformed rows: %d", s, len(rows))
                    return rows

                return tr(ex())
            groups.append(one_source())
        return groups

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def stage_raw():
        """รวมผลจากทุกแหล่งที่สำเร็จ (แหล่งที่ล่มจะได้ None) แล้ว UPSERT ลง raw_events"""
        ctx = get_current_context()
        ti, dr = ctx["ti"], ctx["dag_run"]
        counts, statuses, rows = {}, {}, []
        for s in SOURCES:
            tid = f"extract_sources.{s}.transform"
            state = dr.get_task_instance(tid).state
            data = ti.xcom_pull(task_ids=tid) if state == "success" else None
            statuses[s] = "success" if data is not None else f"failed ({state})"
            counts[s] = len(data or [])
            rows.extend(data or [])
        if rows:
            with db_conn() as c, c.cursor() as cur:
                execute_values(cur, f"""
                    INSERT INTO raw_events ({",".join(RAW_COLS)}) VALUES %s
                    ON CONFLICT (source, source_event_id) DO UPDATE SET
                      event_time_utc=EXCLUDED.event_time_utc, latitude=EXCLUDED.latitude,
                      longitude=EXCLUDED.longitude, depth_km=EXCLUDED.depth_km,
                      magnitude=EXCLUDED.magnitude, mag_type=EXCLUDED.mag_type,
                      region=EXCLUDED.region, ingested_at=now()""",
                    [tuple(r[k] for k in RAW_COLS) for r in rows], page_size=1000)
        ok = [s for s in SOURCES if statuses[s] == "success"]
        log.info("staged %d rows from %s; statuses=%s", len(rows), ok, statuses)
        return {"counts": counts, "statuses": statuses}

    @task
    def merge_dedupe():
        start, _ = window()
        with db_conn() as c, c.cursor() as cur:
            cur.execute(f"SELECT {','.join(RAW_COLS)} FROM raw_events WHERE event_time_utc >= %s", (start,))
            recs = []
            for row in cur.fetchall():
                r = dict(zip(RAW_COLS, row))
                r["event_time_utc"] = r["event_time_utc"].astimezone(timezone.utc).isoformat()
                recs.append(r)
        recs_ok = [r for r in recs if r["latitude"] is not None and r["longitude"] is not None]
        merged = dedupe(recs_ok)
        # เก็บรายการที่ไม่มีพิกัดไว้ให้ dq_check จับ
        for r in recs:
            if r["latitude"] is None or r["longitude"] is None:
                merged.append({**{k: None for k in EQ_COLS}, **r,
                               "event_id": f"{r['source']}:{r['source_event_id']}",
                               "primary_source": r["source"], "sources": r["source"], "source_count": 1})
        log.info("raw %d -> merged %d (removed %d duplicates)", len(recs), len(merged), len(recs) - len(merged))
        return merged + simulated_bad_rows(merged)

    @task.branch
    def dq_check(merged):
        bad = {e["event_id"]: dq_problems(e) for e in merged}
        bad = {k: v for k, v in bad.items() if v}
        get_current_context()["ti"].xcom_push(key="bad", value=bad)
        log.info("DQ: %d events, %d failed", len(merged), len(bad))
        if len(merged) - len(bad) < 100:
            log.warning("DQ: valid events < 100 (requirement) — check sources")
        return "handle_bad_rows" if bad else "load_curated"

    @task
    def handle_bad_rows(merged):
        import json
        bad = get_current_context()["ti"].xcom_pull(task_ids="dq_check", key="bad") or {}
        rows = [(e["event_id"], ",".join(bad[e["event_id"]]), json.dumps(e, default=str))
                for e in merged if e["event_id"] in bad]
        with db_conn() as c, c.cursor() as cur:
            execute_values(cur, """INSERT INTO dq_rejects (event_id, problems, payload) VALUES %s
                ON CONFLICT (event_id) DO UPDATE SET problems=EXCLUDED.problems,
                payload=EXCLUDED.payload, rejected_at=now()""", rows)
        log.warning("quarantined %d events into dq_rejects", len(rows))

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def load_curated(merged):
        bad = get_current_context()["ti"].xcom_pull(task_ids="dq_check", key="bad") or {}
        good = [e for e in merged if e["event_id"] not in bad]
        start, _ = window()
        with db_conn() as c, c.cursor() as cur:
            if good:
                updates = ",".join(f"{k}=EXCLUDED.{k}" for k in EQ_COLS if k != "event_id")
                execute_values(cur, f"""
                    INSERT INTO earthquakes ({",".join(EQ_COLS)}) VALUES %s
                    ON CONFLICT (event_id) DO UPDATE SET {updates}, updated_at=now()""",
                    [tuple(e[k] for k in EQ_COLS) for e in good], page_size=1000)
            # ลบ event_id เก่าที่ถูกรวมเข้ากลุ่มอื่นแล้ว (ป้องกันแถวงอกเมื่อรันซ้ำ)
            cur.execute("DELETE FROM earthquakes WHERE event_time_utc >= %s AND NOT (event_id = ANY(%s))",
                        (start, [e["event_id"] for e in good]))
            cur.execute("SELECT count(*) FROM earthquakes")
            total = cur.fetchone()[0]
        log.info("loaded %d curated events; table now has %d rows", len(good), total)
        return len(good)

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def log_run(stage_info):
        ctx = get_current_context()
        ti, dr = ctx["ti"], ctx["dag_run"]
        stage_info = stage_info or {"counts": {}, "statuses": {}}
        merged = ti.xcom_pull(task_ids="merge_dedupe") or []
        bad = ti.xcom_pull(task_ids="dq_check", key="bad") or {}
        branch = ti.xcom_pull(task_ids="dq_check")
        load_state = dr.get_task_instance("load_curated").state
        st = stage_info.get("statuses", {})
        degraded = any(v != "success" for v in st.values())
        status = "failed" if load_state != "success" else ("degraded" if degraded else "success")
        cnt = stage_info.get("counts", {})
        with db_conn() as c, c.cursor() as cur:
            cur.execute("""INSERT INTO pipeline_runs VALUES (%s,%s,now(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (run_id) DO UPDATE SET finished_at=now(), status=EXCLUDED.status,
                usgs_status=EXCLUDED.usgs_status, emsc_status=EXCLUDED.emsc_status,
                tmd_status=EXCLUDED.tmd_status, usgs_rows=EXCLUDED.usgs_rows,
                emsc_rows=EXCLUDED.emsc_rows, tmd_rows=EXCLUDED.tmd_rows,
                merged_rows=EXCLUDED.merged_rows, rejected_rows=EXCLUDED.rejected_rows,
                dq_branch=EXCLUDED.dq_branch, message=EXCLUDED.message""",
                (dr.run_id, dr.start_date, status,
                 st.get("usgs"), st.get("emsc"), st.get("tmd"),
                 cnt.get("usgs"), cnt.get("emsc"), cnt.get("tmd"),
                 len(merged) - len(bad), len(bad), branch,
                 f"load_curated={load_state}"))

    ddl = create_tables()
    groups = extract_sources()
    staged = stage_raw()
    ddl >> groups
    for g in groups:
        g >> staged
    merged = merge_dedupe()
    staged >> merged
    branch = dq_check(merged)
    bad = handle_bad_rows(merged)
    loaded = load_curated(merged)
    branch >> [bad, loaded]
    bad >> loaded
    loaded >> log_run(staged)


sea_earthquake_pipeline()
