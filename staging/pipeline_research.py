from datetime import datetime, timedelta
import pandas as pd
import yaml
import sys
import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator


# in research DAG, we only perform the query under parameters

# read working directory
with open(os.path.dirname(__file__) + '/config_params.yaml') as f:
    params = yaml.safe_load(f)
    work_dir = params['working.directory']

# Setup dag
default_args = {
    'owner': 'shaojun',
    'depends_on_past': False,
    'start_date': datetime.now(), # time in utc
    'email': ['sjlocke.1989@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3)
}

dag_main = DAG(
    'mobile_COVID_research',
    description='Research pipeline',
    schedule_interval= "@once",
    max_active_runs = 1,
    default_args=default_args
)

# Start of the Running
t_start = BashOperator(
    task_id='running_start',
    bash_command='echo Running Start! Time: $(date +"%T")',
    priority_weight = 100, # this is main pipeline
    dag=dag_main
)

# ======================Start of Main Pipeline ================#

t2 = BashOperator(
    task_id='patient_track',
    bash_command=f'python {work_dir}/src/track_patient.py',
    dag=dag_main
)
t3 = BashOperator(
    task_id='close_contact_track',
    bash_command=f'python {work_dir}/src/track_close_contact.py',
    dag=dag_main
)

t3_1 = BashOperator(
    task_id='contact_between_patient',
    depends_on_past=True, # must depend on upstream to complete
    bash_command=f'python {work_dir}/src/track_patient_contact.py',
    dag=dag_main,
)

# ======================End of main pipeline=====================

# Ending Task
t_end = BashOperator(
    task_id='running_end',
    bash_command='echo Running End! Time: $(date +"%T")',
    trigger_rule= 'one_success', # if one branches is done
    dag=dag_main
)

# Assemble main running pipline
dag_main >> t_start >> t2 >> t3 >> t3_1 >> t_end