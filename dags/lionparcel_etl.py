from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text

# 1. Konfigurasi Koneksi (Sesuai parameter yang kamu minta)
# Gunakan host 'postgres' karena Airflow & DB berjalan di container Docker yang sama
postgres_user = "postgres"
postgres_password = "postgres"
postgres_host = "postgres" 
postgres_port = "5432"
postgres_db = "postgres"

# URL SQLAlchemy untuk koneksi antar container
SRC_CONN = f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}"
DWH_CONN = SRC_CONN

# Nama tabel sesuai yang sudah ada di DBeaver kamu
table_source = "retail_transactions_dummy"
table_destination = "dwh_retail_transactions"

def move_data():
    src_engine = create_engine(SRC_CONN)
    dwh_engine = create_engine(DWH_CONN)

    print(f"Mengambil data dari: {table_source}")

    # 2. Query data yang berubah dalam 1 jam terakhir
    query = f"""
        SELECT * FROM {table_source} 
        WHERE updated_at >= NOW() - INTERVAL '1 hour'
    """
    
    try:
        df = pd.read_sql(query, src_engine)

        if not df.empty:
            # 3. Pindahkan data ke staging table (temporary)
            df.to_sql('stg_retail_transactions', dwh_engine, if_exists='replace', index=False)
            
            # 4. Proses Upsert (Insert or Update) ke tabel DWH
            upsert_sql = f"""
                -- Pastikan tabel tujuan sudah ada (hanya jika belum dibuat manual)
                CREATE TABLE IF NOT EXISTS {table_destination} (
                    id INT PRIMARY KEY,
                    customer_id INT,
                    last_status VARCHAR(50),
                    pos_origin VARCHAR(100),
                    pos_destination VARCHAR(100),
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    deleted_at TIMESTAMP
                );

                INSERT INTO {table_destination} 
                SELECT * FROM stg_retail_transactions
                ON CONFLICT (id) DO UPDATE SET
                    customer_id = EXCLUDED.customer_id,
                    last_status = EXCLUDED.last_status,
                    pos_origin = EXCLUDED.pos_origin,
                    pos_destination = EXCLUDED.pos_destination,
                    updated_at = EXCLUDED.updated_at,
                    deleted_at = EXCLUDED.deleted_at;
            """
            with dwh_engine.connect() as conn:
                conn.execute(text(upsert_sql))
                conn.commit()
            
            print(f"Berhasil sinkronisasi {len(df)} data ke {table_destination}.")
        else:
            print("Tidak ada data baru dalam 1 jam terakhir.")
            
    except Exception as e:
        print(f"Error saat menjalankan ETL: {e}")
        raise e

with DAG(
    'lionparcel_etl_job',
    start_date=datetime(2026, 1, 1), # Menyesuaikan tahun saat ini
    schedule_interval='@hourly', 
    catchup=False
) as dag:

    task_etl = PythonOperator(
        task_id='run_etl_process',
        python_callable=move_data
    )