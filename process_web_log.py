
#program tasks
#Task 1 Define DAG arguments (Directed Acyclic Graph)
#Task 2 Define DAG
#Task 3 Extract IP addresses from text file
#Task 4 transform-filter 198.46.149.143
#task 5 load - archive file
#task 6 define pipeline


#import airflow libraries
from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

#DAG arguments
default_args = {
'owner': 'airflow',
'start_date': days_ago(0),
'email': ['nussb003@csusm.edu'],
'retries': 1,
'retry_delay': timedelta(minutes=5),
}

#define the DAG
dag = DAG('process_web_log',
   default_args=default_args,
   description='Remove IP from web log',
   schedule_interval=timedelta(days=1),
)

file_path = "/home/project/data"


#DAG task1 Extract IP addresses from text file
extract_ip = BashOperator(
    task_id='extract_ip',
    bash_command=f"cut -d' ' -f1 /home/project/airflow/dags/capstone/accesslog.txt > {file_path}/extracted_data.txt",
    dag=dag,
)

#DAG task 2 transform-filter 198.46.149.143
transform_data = BashOperator(
    task_id='transform_data',
    bash_command=f"""grep -v "198.46.149.143" {file_path}/extracted_data.txt > {file_path}/transformed_data.txt""",  
    dag=dag,
)

#DAG task 3 load - archive file
load_data = BashOperator(
    task_id='load_data',
    bash_command=f"tar -czf {file_path}/weblog.tgz -C {file_path} transformed_data.txt",
    dag=dag,
)

#DAG task pipeline
extract_ip >> transform_data >> load_data




