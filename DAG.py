from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging
import random

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'data_minimization_dag',
    default_args=default_args,
    description='A DAG for data minimization process',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
)

# Define the functions used in each task
def field_suppression():
    logging.info("Starting field suppression...")
    # Example logic: remove sensitive fields from dataset
    data = {'name': 'John Doe', 'age': 30, 'ssn': '123-45-6789'}
    minimized_data = {k: v for k, v in data.items() if k != 'ssn'}
    logging.info(f"Suppressed data: {minimized_data}")
    return minimized_data

def data_sampling(sample_percentage, **kwargs):
    logging.info("Starting data sampling...")
    # Example: sample a percentage of data
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='field_suppression_task')
    sample_size = int(len(data) * (sample_percentage / 100))
    sampled_data = random.sample(list(data.items()), sample_size)
    logging.info(f"Sampled data: {sampled_data}")
    return dict(sampled_data)

def data_filtering(**kwargs):
    logging.info("Starting data filtering...")
    # Example: filter data based on some criteria
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='data_sampling_task')
    filtered_data = {k: v for k, v in data.items() if k != 'age'}
    logging.info(f"Filtered data: {filtered_data}")
    return filtered_data

def data_aggregation(**kwargs):
    logging.info("Starting data aggregation...")
    # Example: aggregate data, e.g., summarize it
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='data_filtering_task')
    aggregated_data = {'count': len(data), 'fields': list(data.keys())}
    logging.info(f"Aggregated data: {aggregated_data}")
    return aggregated_data

def save_to_lower_environment(**kwargs):
    logging.info("Saving data to the lower environment...")
    # Example: save aggregated data to a lower environment
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='data_aggregation_task')
    # Simulate saving data
    logging.info(f"Data saved: {data}")

# Define tasks
field_suppression_task = PythonOperator(
    task_id='field_suppression_task',
    python_callable=field_suppression,
    dag=dag,
)

data_sampling_task = PythonOperator(
    task_id='data_sampling_task',
    python_callable=data_sampling,
    op_kwargs={'sample_percentage': 50},  # Example: take a 50% sample
    provide_context=True,
    dag=dag,
)

data_filtering_task = PythonOperator(
    task_id='data_filtering_task',
    python_callable=data_filtering,
    provide_context=True,
    dag=dag,
)

data_aggregation_task = PythonOperator(
    task_id='data_aggregation_task',
    python_callable=data_aggregation,
    provide_context=True,
    dag=dag,
)

save_to_lower_environment_task = PythonOperator(
    task_id='save_to_lower_environment_task',
    python_callable=save_to_lower_environment,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
field_suppression_task >> data_sampling_task >> data_filtering_task >> data_aggregation_task >> save_to_lower_environment_task
