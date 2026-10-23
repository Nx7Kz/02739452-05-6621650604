# 02739452 Final — แผ่นดินไหวในเอเชียตะวันออกเฉียงใต้ 30 วันล่าสุด

**นิสิต:** นายพงศ์พัทธ์ ดวงจิตต์ · รหัส 6621650604 · หมู่ 820

## หัวข้อ
รวบรวมเหตุการณ์แผ่นดินไหวในกรอบ lat −11° ถึง 28.5°, lon 92° ถึง 141.5° (ครอบคลุมเมียนมา ไทย ลาว กัมพูชา เวียดนาม มาเลเซีย สิงคโปร์ อินโดนีเซีย ฟิลิปปินส์ บรูไน ติมอร์-เลสเต) ย้อนหลัง 30 วัน จาก 3 แหล่ง รวมเป็นชุดข้อมูลเดียว ตัดรายการซ้ำข้ามแหล่ง แล้วแสดงบน dashboard

## แหล่งข้อมูล
| # | แหล่ง | วิธีดึง | หมายเหตุ |
|---|---|---|---|
| 1 | USGS FDSN Event API — `earthquake.usgs.gov/fdsnws/event/1/query` | REST API (GeoJSON) | กรองด้วย bbox + starttime |
| 2 | EMSC FDSN — `www.seismicportal.eu/fdsnws/event/1/query` | REST API (GeoJSON) | |
| 3 | กรมอุตุนิยมวิทยา — `earthquake.tmd.go.th/inside.html` | **Web scraping** ด้วย `requests` + regex (ไม่ใช้ Selenium) | ตรวจ robots.txt ก่อนดึง, หน่วง 1 วินาที/หน้า, ไล่หน้าจนเลย 30 วัน |

ข้อมูลเป็นข้อมูลเหตุการณ์ธรรมชาติ ไม่มีข้อมูลส่วนบุคคล (สอดคล้อง PDPA)

## สถาปัตยกรรม DAG `sea_earthquake_pipeline`
```
create_tables
  └─ TaskGroup extract_sources (ขนาน)
       ├─ usgs.extract → usgs.transform
       ├─ emsc.extract → emsc.transform
       └─ tmd.extract  → tmd.transform
  └─ stage_raw        (trigger_rule=all_done → แหล่งใดล่มก็ไปต่อ, UPSERT raw_events)
  └─ merge_dedupe     (จับคู่เหตุการณ์ข้ามแหล่ง: เวลาห่าง ≤90 วินาที และ ≤100 กม.)
  └─ dq_check (branch) ─┬─ handle_bad_rows → dq_rejects (quarantine)
                        └─ load_curated  (UPSERT earthquakes)
  └─ log_run          (บันทึกสถานะแต่ละแหล่งลง pipeline_runs → หน้า status บนเว็บ)
```
- **Schema เดียว:** ทุกแหล่งแปลงเป็น `source, source_event_id, event_time_utc, latitude, longitude, depth_km, magnitude, mag_type, region`
- **UPSERT:** `ON CONFLICT ... DO UPDATE` ทั้ง `raw_events` และ `earthquakes` — รันซ้ำแล้วจำนวนแถวไม่เพิ่ม
- **ตัดซ้ำข้ามแหล่ง:** ลำดับความน่าเชื่อถือ USGS > EMSC > TMD, เก็บ `sources`, `source_count` และขนาดจากแต่ละแหล่ง (`mag_usgs/mag_emsc/mag_tmd`)
- **Data quality:** ขาดพิกัด/ขนาด, ขนาดนอกช่วง −2..10, ความลึกนอก 0..800 กม., เวลาในอนาคต → แตกเส้นทางไป quarantine
- **ทดสอบเส้นทาง DQ:** Trigger DAG w/ config `{"simulate_bad_rows": 4}` → เพิ่มแถวเสียจำลอง (`event_id` ขึ้นต้น `SIMULATED:`) วนตามปัญหา 4 แบบข้างบน แล้วดูว่า `dq_check` แตกไป `handle_bad_rows` → `dq_rejects` และไม่หลุดเข้า `earthquakes`
- **ทดสอบแหล่งล่ม:** Trigger DAG w/ config `{"simulate_down": "emsc"}` (หรือ `"usgs,tmd"`) แล้วดูว่า DAG ยังโหลดข้อมูลจากแหล่งที่เหลือ และหน้า status ขึ้น `degraded` (ผลทดสอบจริงอยู่ในหัวข้อ [ผลการทดสอบ](#ผลการทดสอบ))

## วิธีรัน
ต้องมี Docker Desktop (เปิดไว้ก่อนรัน) และอินเทอร์เน็ต (ดึงข้อมูล 3 แหล่ง, ฟอนต์ และพื้นแผนที่)
```bash
cp .env.example .env          # แก้รหัสผ่าน
docker compose up -d --build
```
- Airflow UI: http://localhost:8080 (user/pass ตาม `.env`) → DAG `sea_earthquake_pipeline` → Trigger
  (DAG ไม่ได้ pause ตอนสร้าง จึงรันตาม schedule ครั้งแรกเองหลังเปิดระบบ)
- Dashboard: http://localhost:8501
- ปิดระบบ: `docker compose down` (เพิ่ม `-v` ถ้าต้องการลบข้อมูลใน PostgreSQL ด้วย)

ตรวจ UPSERT:
```bash
docker compose exec postgres psql -U airflow -d quakes -c "select count(*) from earthquakes;"
# trigger DAG อีกรอบ แล้วรันคำสั่งเดิม จำนวนต้องเท่าเดิม (หากไม่มีเหตุการณ์ใหม่)
```

ทดสอบแหล่งล่ม / เส้นทาง DQ จาก command line (หรือกรอกช่อง `simulate_down` / `simulate_bad_rows` ในหน้า Trigger DAG ของ Airflow UI):
```bash
docker compose exec airflow-scheduler airflow dags trigger sea_earthquake_pipeline -c '{"simulate_down": "emsc"}'
docker compose exec airflow-scheduler airflow dags trigger sea_earthquake_pipeline -c '{"simulate_bad_rows": 4}'
```

## Dashboard (Streamlit)
ธีมมืด ฟอนต์ IBM Plex Sans Thai ใช้ชุดสีเดียวกันทั้งหน้า — ระดับขนาดไล่เหลือง→แดง, แหล่งข้อมูล USGS ฟ้า · EMSC ม่วง · TMD เขียว

**แท็บ Dashboard**
- KPI 5 ใบ: จำนวนเหตุการณ์, ขนาดสูงสุด (พร้อมบริเวณ), ขนาดเฉลี่ย/ความลึกเฉลี่ย, จำนวน M ≥ 5, สัดส่วนที่ยืนยันจาก ≥ 2 แหล่ง
- แผนที่ศูนย์กลางแผ่นดินไหว (ขนาด/สีตามแมกนิจูด, ชี้จุดเพื่อดูประเทศ เวลาไทย ความลึก แหล่งที่รายงาน) และซูมไปที่ประเทศที่เลือกให้อัตโนมัติ
- 5 เหตุการณ์ใหญ่ที่สุด + สัดส่วนตามระดับขนาด
- กราฟจำนวนเหตุการณ์ต่อวัน (แยกระดับขนาด), histogram ขนาด, เหตุการณ์ตามชุดแหล่งที่ยืนยัน, ความลึก vs ขนาด
- ตารางข้อมูล (มีคอลัมน์ประเทศ) และปุ่มดาวน์โหลด CSV

**แถบตัวกรอง (ด้านซ้าย)** — มีผลกับทุกกราฟ/แผนที่/ตารางทันที
- กล่องสรุปด้านบน: แสดง “กำลังแสดง X / ทั้งหมด” ป้ายตัวกรองที่ใช้อยู่ และปุ่ม **ล้างตัวกรองทั้งหมด**
- 📍 พื้นที่: เลือก **ประเทศ** ได้หลายประเทศ (มีจำนวนเหตุการณ์กำกับ) + ค้นหาชื่อบริเวณ เช่น `Sumatra`, `เชียงใหม่`
- 🗓️ ช่วงเวลา: 24 ชม. / 3 / 7 / 14 / 30 วัน
- 📏 ขนาดและความลึก: ช่วงแมกนิจูด และช่วงความลึก (กม.)
- 🛰️ แหล่งข้อมูล: ติ๊กเลือก USGS / EMSC / TMD และสวิตช์ “เฉพาะที่ยืนยันจาก ≥ 2 แหล่ง”

**การระบุประเทศ** — คำนวณใน dashboard จากข้อความ `region` ของแต่ละแหล่ง (ไม่ได้เก็บเป็นคอลัมน์ในฐานข้อมูล)
- USGS/EMSC: ชื่อประเทศหรือภูมิภาคท้ายข้อความ เช่น `…, Indonesia`, `Mindanao, Philippines`
- TMD: เหตุการณ์ในไทยมีรูปแบบ `ต.… อ.… จ.…` → นับเป็นไทย
- ทะเลภายในอินโดนีเซีย (Flores/Banda/Molucca/Savu/Bali Sea) → อินโดนีเซีย
- เขตชายแดน/ทะเลระหว่างประเทศ (เช่น Myanmar-India Border Region, Celebes Sea) นับให้ทุกประเทศที่เกี่ยวข้อง — ผลรวมรายประเทศจึงอาจมากกว่าจำนวนเหตุการณ์จริงเล็กน้อย
- ข้อความที่จับคู่ไม่ได้จัดเป็น “อื่น ๆ / ไม่ระบุ”

**แท็บ Pipeline status**
- แถบสถานะรันล่าสุด (เขียว success / เหลือง degraded / แดง failed), การ์ดสถานะและจำนวนแถวของแต่ละแหล่ง
- กราฟจำนวนแถวที่ดึงได้ต่อรันแยกแหล่ง (แท่งหาย = แหล่งนั้นล่ม), ประวัติการรัน 30 รันล่าสุด, จำนวนแถวในแต่ละตาราง และรายการ quarantine

**หมายเหตุทางเทคนิค**
- Streamlit 1.39 ฝั่งเบราว์เซอร์ใช้ plotly.js รุ่นเก่า (2.6) ที่รองรับเฉพาะ `scattermapbox` จึงล็อก `plotly>=5.20,<6` ใน `web/requirements.txt` — plotly 6+ ไม่มี `px.scatter_mapbox` แล้ว และ `px.scatter_map` จะแสดงเป็นกราฟว่าง
- พื้นแผนที่ใช้ tile **Esri World Dark Gray** (ไม่ต้องใช้ API key) แทน `carto-positron` เพราะ CARTO basemap ต้องใช้ API key แล้ว (ขึ้นลายน้ำ “API KEY REQUIRED”)
- ธีมตั้งใน `web/.streamlit/config.toml`; ข้อมูลใน dashboard cache 60 วินาที

## ผลการทดสอบ
ทดสอบเมื่อ 23 ต.ค. 2026 บน Docker Desktop (Windows 11)

| การทดสอบ | ผล |
|---|---|
| รันปกติ | `success` — USGS 186, EMSC 1,484, TMD 165 แถว → หลังตัดซ้ำ 1,597 เหตุการณ์, ไม่ผ่าน DQ 0 แถว |
| UPSERT (รันซ้ำ) | `earthquakes` คงที่ 1,597 แถว, `raw_events` คงที่ 1,835 แถว |
| `{"simulate_down": "emsc"}` | DAG `success`, สถานะ `degraded` — EMSC extract ล้ม (retry 1 ครั้ง), USGS 186 + TMD 165 แถวยังโหลดต่อ; หน้า status ขึ้นแถบเหลือง DEGRADED และการ์ด EMSC สีแดง |
| `{"simulate_down": "usgs,tmd"}` | DAG `success`, สถานะ `degraded` — EMSC 1,483 แถวยังโหลดต่อ |
| `{"simulate_bad_rows": 4}` | DAG `success` — `dq_check` แตกไป `handle_bad_rows`; 4 แถวเข้า `dq_rejects` (magnitude_out_of_range, depth_out_of_range, time_in_future, missing_coordinates); `earthquakes` ยัง 1,597 แถว ไม่มีแถว `SIMULATED:` หลุดเข้าไป; หน้า status แสดงรายการ quarantine |
| ข้อมูลเดิมเมื่อแหล่งล่ม | ไม่หาย — `earthquakes` ยังเป็น 1,597 แถว เพราะ `merge_dedupe` อ่านจาก `raw_events` ที่สะสมไว้ |

## โครงสร้าง
```
dags/sea_earthquake_pipeline.py
web/app.py, web/Dockerfile, web/requirements.txt
web/.streamlit/config.toml        # ธีม dashboard
sql/init.sql
docker-compose.yml, requirements.txt, .env.example
```
