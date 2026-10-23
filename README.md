# Weather Fusion Dashboard — Airflow + PostgreSQL

ระบบรวบรวมสภาพอากาศจากหลายแหล่ง รวมข้อมูลเป็น schema เดียว ตรวจคุณภาพข้อมูล UPSERT ลง PostgreSQL และแสดงผลผ่านเว็บ Dashboard

## แหล่งข้อมูล
1. Open-Meteo Forecast API (ไม่ต้องใช้ API key)
2. wttr.in JSON API (ไม่ต้องใช้ API key; เป็นบริการชุมชน ควรเรียกอย่างสุภาพ)
3. Thai Meteorological Department (TMD) WeatherToday API (ตัวอย่าง endpoint สาธารณะที่เผยแพร่ในแค็ตตาล็อกข้อมูล; หาก endpoint/สิทธิ์เปลี่ยน ให้แก้ `TMD_API_URL` ใน `.env`)

หมายเหตุ: Open-Meteo และ wttr.in มีรูปแบบและความถี่ข้อมูลต่างกัน การรวมข้อมูลจะเก็บ `source` ไว้ทุกแถว ไม่ควรเปรียบเทียบข้อมูลที่เวลา/สถานีต่างกันโดยตรง ส่วน API ของ TMD อาจเปลี่ยน schema ได้ จึงมี parser แบบยืดหยุ่นและสถานะ source error บน Dashboard

## วิธีเริ่มต้น (Docker Desktop + Docker Compose)
1. ติดตั้งและเปิด Docker Desktop
2. แตก ZIP ไปยังโฟลเดอร์ที่ไม่มีช่องว่างใน path เช่น `D:\weather-airflow`
3. คัดลอก `.env.example` เป็น `.env` (ค่าเริ่มต้นใช้ได้สำหรับเครื่อง local)
4. เปิด Terminal ในโฟลเดอร์โปรเจกต์ แล้วรัน:

```bash
docker compose up --build airflow-init
docker compose up --build -d
```

5. เปิด:
   - Airflow: http://localhost:8080 (ผู้ใช้ `airflow`, รหัสผ่าน `airflow`)
   - Dashboard: http://localhost:5000
6. ใน Airflow เปิด DAG `weather_multi_source_pipeline` แล้วกด Trigger DAG
7. รีเฟรช Dashboard หลัง DAG ทำงานเสร็จ

หาก Docker Desktop มีปัญหา สามารถรันเว็บและ DAG บน Python environment ของตนเองได้ แต่ต้องติดตั้ง Apache Airflow ให้ตรงเวอร์ชันและตั้งค่า PostgreSQL เอง

## สิ่งที่มีในโปรเจกต์
- TaskGroup แยกแหล่งข้อมูล 3 กลุ่ม ให้ดึงข้อมูลขนานกัน
- source task จับ error ของแหล่งตัวเองและส่งสถานะกลับ แหล่งอื่นจึงทำงานต่อได้
- normalize fields: source, location, observed_at, temperature_c, humidity_pct, rainfall_mm, latitude, longitude
- ตรวจคุณภาพ: ต้องมี location/time อย่างน้อยหนึ่งอย่าง และค่าตัวเลขต้องอยู่ในช่วงที่สมเหตุผล
- ข้อมูลไม่ผ่านบันทึกใน `weather_rejected`
- UPSERT ด้วย unique key `(source, location, observed_at)`
- ตัดข้อมูลซ้ำจากแหล่งเดียวกัน และทำ cross-source duplicate detection แบบ heuristic ในขั้นรวมข้อมูล (เก็บหนึ่งรายการต่อ location/time ใกล้เคียงและค่าตรงกัน) โดยไม่ลบข้อมูลต่างสถานี/เวลาที่อาจมีความหมาย
- Dashboard มี KPI, filter จังหวัด/แหล่งข้อมูล/วันที่, ตาราง, กราฟอุณหภูมิ และ Pipeline Status

## โครงสร้าง
- `dags/weather_pipeline.py` — Airflow DAG
- `dashboard/app.py` — Flask dashboard/API
- `docker-compose.yml` — PostgreSQL, Airflow, Dashboard
- `Dockerfile.airflow`, `Dockerfile.dashboard`
- `.env.example`

## ทดสอบเกณฑ์ระดับ 3
- รัน DAG ปกติ แล้วตรวจ KPI/ตาราง
- รัน DAG ซ้ำ: จำนวนแถวไม่ควรเพิ่มซ้ำสำหรับ key เดิม
- จำลอง source ล่ม: ตั้ง `ENABLE_WTTR=false` หรือ `ENABLE_TMD=false` ใน `.env` แล้ว recreate containers จากนั้นรัน DAG; แหล่งอื่นควรยังโหลดต่อได้
- ตรวจข้อมูลไม่ผ่าน: task quality จะส่งแถวที่ไม่ผ่านไป `weather_rejected`
- หน้า Pipeline Status แสดงสถานะรอบ DAG ล่าสุดและสถานะของแต่ละ source

## หมายเหตุสำคัญ
นี่เป็นโครงงานตัวอย่างสำหรับพัฒนาและสอบ ต้องทดสอบ API จริงบนเครือข่ายของตนเองก่อนส่งงาน โดยเฉพาะ endpoint ของ TMD ซึ่งอาจต้องปรับตามเอกสาร/สิทธิ์ล่าสุด และควรระบุแหล่งที่มาในรายงาน


## ครอบคลุม 77 จังหวัด

เวอร์ชันนี้ปรับแหล่ง Open-Meteo ให้ไล่ชื่อจังหวัดไทยทั้ง 77 จังหวัด แล้วค้นหาพิกัดผ่าน Open-Meteo Geocoding API ก่อนขอ current weather ด้วยพิกัดนั้น ๆ ข้อมูลที่แสดงใน Dashboard จะเป็นจังหวัดที่ API ส่งข้อมูลจริงมาได้ในรอบนั้น ไม่ใช่การเติมข้อมูลจำลอง หากจังหวัดใดค้นหาพิกัดหรือดึงข้อมูลไม่สำเร็จ DAG จะบันทึก log และทำจังหวัดอื่นต่อไป ตรวจสอบ log ของ task `open_meteo_group.extract` เพื่อดูบรรทัด `Open-Meteo province coverage: x/77`

หมายเหตุ: wttr.in ยังคงดึงตัวอย่าง 5 พื้นที่เดิม และ TMD ขึ้นกับ API endpoint/สิทธิ์และ schema ที่ใช้งานได้จริง ดังนั้นการครอบคลุมทั้ง 77 จังหวัดในรุ่นนี้อาศัย Open-Meteo เป็นหลัก ไม่ควรตีความว่าทั้งสามแหล่งมีข้อมูลครบทุกจังหวัด
