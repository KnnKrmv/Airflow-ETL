from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2

def load_csv_to_postgres():
    df = pd.read_csv('/opt/bitnami/airflow/data/MOCK_DATA (2).csv')

    conn = psycopg2.connect(
        dbname="airflowdb",
        user="airflow",
        password="airflow",
        host="postgresql",
        port="5432"
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS people (
            id INTEGER,
            first_name TEXT,
            last_name TEXT,
            email TEXT,
            gender TEXT,
            ip_address TEXT
        )
    """)

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO people (id, first_name, last_name, email, gender, ip_address)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            int(row['id']),
            row['first_name'],
            row['last_name'],
            row['email'],
            row['gender'],
            row['ip_address']
        ))

    conn.commit()
    cur.close()
    conn.close()

# DAG qurulması
with DAG(
    dag_id='csv_to_postgres',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@hourly',
    catchup=False
) as dag:
    
    load_task = PythonOperator(
        task_id='load_csv',
        python_callable=load_csv_to_postgres
    )
    
    load_task  