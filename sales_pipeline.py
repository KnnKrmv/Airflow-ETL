from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text
import pandas as pd
from datetime import datetime
import os
import shutil
import pendulum

INPUT_DIR = "/opt/bitnami/airflow/input"
ARCHIVE_DIR = "/opt/bitnami/airflow/archive"
TABLE_NAME = "sales_data"
META_TABLE = "sales_meta"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 9, 15, 18, 0, tz="Asia/Baku"),
    'retries': 0,
}

dag = DAG(
    'sales_pipeline',
    default_args=default_args,
    schedule_interval='0 */8 * * *',
    catchup=False
)

# 1. Fayl yoxlama
def check_file(**context):
    files = [f for f in os.listdir(INPUT_DIR) if f.endswith('.csv')]  
    if not files:
        raise ValueError('Qovluqda yeni fayl yoxdur')
    context['ti'].xcom_push(key='files', value=files)

# 2. Faylları DB-ə yazma
def insert_file(**context):
    hook = PostgresHook(postgres_conn_id='postgres_default')
    engine = hook.get_sqlalchemy_engine()

    with engine.begin() as conn:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {META_TABLE} (
                batch_id SERIAL PRIMARY KEY,
                filename TEXT,
                row_count INT,
                load_date TIMESTAMP DEFAULT NOW()
            );        
        """) 
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                order_id INT,
                product_id VARCHAR(50),
                orderqty INT,
                subtotal NUMERIC(10,2),
                batch_id INT REFERENCES {META_TABLE}(batch_id)
            );                
        """)
        
    files = context['ti'].xcom_pull(key='files', task_ids='check_file')
    processed_files = []

    for f in files:
        filepath = os.path.join(INPUT_DIR, f)
        try:
            df = pd.read_csv(filepath, header=0, dtype={
                'order_id': int,
                'product_id': str,
                'orderqty': int,
                'subtotal': float
            })  
            
            with engine.begin() as conn:
                result = conn.execute(
                    text(f"SELECT batch_id FROM {META_TABLE} WHERE filename = :filename"),
                    {"filename": f}
                )
                existing_batch_id = result.scalar()

                if existing_batch_id:
                    conn.execute(
                        text(f"DELETE FROM {TABLE_NAME} WHERE batch_id = :batch_id"),
                        {"batch_id": existing_batch_id}
                    )
                    batch_id = existing_batch_id
                    conn.execute(
                        text(f"""
                            UPDATE {META_TABLE}
                            SET row_count = :row_count, load_date = NOW()
                            WHERE batch_id = :batch_id        
                        """),
                        {"row_count": len(df), "batch_id": batch_id}
                    )
                else:
                    result = conn.execute(
                        text(f"""
                            INSERT INTO {META_TABLE} (filename, row_count)
                            VALUES (:filename, :row_count)
                            RETURNING batch_id;
                        """),
                        {"filename": f, "row_count": len(df)}
                    )
                    batch_id = result.scalar()

            df['batch_id'] = batch_id
            df.to_sql(TABLE_NAME, engine, if_exists='append', index=False, method='multi')
            processed_files.append(f)
        
        except Exception as e:
            print(f"Xəta!: {f}, {e}")

    context['ti'].xcom_push(key='processed_files', value=processed_files)

# 3. Faylları arxivləmə
def archive_file(**context):
    processed_files = context['ti'].xcom_pull(key='processed_files', task_ids='insert_file')
    for f in processed_files:
        shutil.move(os.path.join(INPUT_DIR, f), os.path.join(ARCHIVE_DIR, f))
        print(f"Fayl arxivə olundu: {f}")

# Tasklar
check_task = PythonOperator(
    task_id='check_file',
    python_callable=check_file,
    provide_context=True,
    dag=dag
)

insert_task = PythonOperator(
    task_id='insert_file',
    python_callable=insert_file,
    provide_context=True,
    dag=dag
)

archive_task = PythonOperator(
    task_id='archive_file',
    python_callable=archive_file,
    provide_context=True,
    dag=dag
)

check_task >> insert_task >> archive_task
