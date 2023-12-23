#from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from main import get_top_trending_coins, move_dataframe_to_postgres
import pendulum

TIMEZONE = pendulum.timezone("UTC")  # Correct usage of 'timezone' method


# Define default_args for the DAG
default_args = {
    'owner': 'omni',
    'depends_on_past': False,
    'start_date':pendulum.datetime(2021, 1, 1, tz="UTC"),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Create a DAG
dag = DAG(
    'trending_coins_workflow',
    default_args=default_args,
    description='Fetch and store data for top trending coins',
    schedule_interval=timedelta(hours=1)
    )

# Task to get top trending coins data
get_coins_task = PythonOperator(
    task_id='get_top_trending_coins',
    python_callable=get_top_trending_coins,
    provide_context=True,
    dag=dag,
)

# Task to move DataFrame to PostgreSQL
move_to_postgres_task = PythonOperator(
    task_id='move_dataframe_to_postgres',
    python_callable=move_dataframe_to_postgres,
    provide_context=True,
    op_args=['{{ ti.xcom_pull(task_ids="get_top_trending_coins") }}'],  # Pass the result of the previous task
    dag=dag,
)

# Set task dependencies
get_coins_task >> move_to_postgres_task
