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
- **ทดสอบแหล่งล่ม:** Trigger DAG w/ config `{"simulate_down": "emsc"}` (หรือ `"usgs,tmd"`) แล้วดูว่า DAG ยังโหลดข้อมูลจากแหล่งที่เหลือ และหน้า status ขึ้น `degraded`

## วิธีรัน
```bash
cp .env.example .env          # แก้รหัสผ่าน
docker compose up -d --build
```
- Airflow UI: http://localhost:8080 (user/pass ตาม `.env`) → DAG `sea_earthquake_pipeline` → Trigger
- Dashboard: http://localhost:8501 (แท็บ Dashboard: KPI, ตัวกรอง, แผนที่, กราฟ, ตาราง · แท็บ Pipeline status: สถานะรันล่าสุด/แต่ละแหล่ง)

ตรวจ UPSERT:
```bash
docker compose exec postgres psql -U airflow -d quakes -c "select count(*) from earthquakes;"
# trigger DAG อีกรอบ แล้วรันคำสั่งเดิม จำนวนต้องเท่าเดิม (หากไม่มีเหตุการณ์ใหม่)
```

## โครงสร้าง
```
dags/sea_earthquake_pipeline.py
web/app.py, web/Dockerfile, web/requirements.txt
sql/init.sql
docker-compose.yml, requirements.txt, .env.example
```
