from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Thông số mặc định cho DAG
default_args = {
    'owner': 'ndtien',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['htien225@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(hours=1),
}

# Định nghĩa DAG
with DAG(
    'loading_to_postgres',
    default_args=default_args,
    description='Thu thập dữ liệu vào cuối ngày',
    schedule_interval='0 23 * * *',  # Chạy lúc 23:00 hàng ngày
    catchup=False,  # Không chạy bù cho các lần trước
    tags=['daily', 'batch', 'python'],
) as dags:
    run_spark_job = BashOperator(
        task_id='run_spark_python_script',
        bash_command='python /opt/airflow/dags/loading_daily.py',
    )

run_spark_job