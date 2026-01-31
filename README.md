# retail_transactions – ETL Pipeline

## 📋 Project Overview
Project ini adalah solusi untuk **Assessment Data Engineering (Tugas No. 1)**.  
Proyek ini mengimplementasikan **ETL Pipeline** otomatis menggunakan **Apache Airflow** yang berjalan di dalam lingkungan **Docker**.

Pipeline ini dirancang untuk melakukan sinkronisasi data transaksi dari tabel operasional  
(`retail_transactions_dummy`) ke tabel Data Warehouse  
(`dwh_retail_transactions`) setiap jam dengan menangani:
- **Upsert**
- **Soft Delete**

---

## 🏗 Project Structure
Pastikan direktori proyek Anda memiliki susunan file sebagai berikut:

```text
lion-parcel-etl/
├── dags/
│   └── lionparcel_etl.py    # Logika ETL & Definisi DAG Airflow
├── Dockerfile               # Instruksi build image custom Airflow
├── docker-compose.yaml      # Orchestrator Airflow & PostgreSQL
├── requirements.txt         # Library Python (Pandas, SQLAlchemy, dll)
└── README.md                # Dokumentasi Proyek
```

---

## 🐳 Docker Setup

### Dockerfile
Digunakan untuk membuat custom image Apache Airflow dan menginstall dependency Python.

### docker-compose.yml
Menjalankan service berikut:
- PostgreSQL (source & DWH)
- Airflow Webserver
- Airflow Scheduler

---

## ▶️ How to Run

### 1. Build Docker Image
```bash
docker-compose build
```

### 2. Jalankan Container
```bash
docker-compose up -d
```

Default credentials Airflow:
- **Username**: admin  
- **Password**: admin  

### 3. Akses Airflow UI
```text
http://localhost:8080
```
