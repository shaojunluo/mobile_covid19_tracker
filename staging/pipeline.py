from datetime import datetime, timedelta
import pandas as pd
import yaml
import sys
import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator

# read working directory
with open(os.path.dirname(__file__) + '/config_params.yaml') as f:
    params = yaml.safe_load(f)
    work_dir = params['working.directory']

# Setup dag
default_args = {
    'owner': 'shaojun',
    'depends_on_past': False,
    'start_date': datetime.now()-timedelta(hours = 1), # time in utc
    'email': ['sjlocke.1989@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'max_threads':8
}

dag_main = DAG(
    'mobile_COVID_tracker',
    description='Running pipline for ELT',
    schedule_interval= timedelta(hours = 1),
    max_active_runs = 1,
    default_args=default_args
)

# Start of the Running
t_start = BashOperator(
    task_id='running_start',
    bash_command='echo Running Start! Time: $(date +"%T")',
    priority_weight = 100, # this is main pipeline
    dag=dag_main,
)

# Python executable to check the updates and trigger the main pipeline
def check_latest(**kwargs):
    with open(os.path.dirname(__file__) + '/config_status.yaml') as f:
        file_status = yaml.safe_load(f)
        # find the trigger of data/parameter change
        trigger = False
        for group in file_status['last.update'].values():
            trigger |= pd.Timestamp(group) > pd.Timestamp(file_status['last.run'])
        if trigger: # trigger the main pipeline
            return 'data_ingest'
        else:
            return 'null_execution'

# Brancing for trigger 
monitor = BranchPythonOperator(
    task_id = 'monitor_update',
    python_callable = check_latest,
    provide_context=True,
    dag=dag_main
)

# null branch for run nothing.
check = BashOperator(
    task_id='null_execution',
    bash_command='echo File up tp date. Nothing executed',
    priority_weight = 100, # this is main pipeline
    trigger_rule= 'one_success', # if one branches is done
    retries = 3,
    dag=dag_main
)

# ======================Start of Main Pipeline ================#
t1 = BashOperator(
    task_id='data_ingest',
    bash_command=f'python {work_dir}/src/data_ingestion.py',
    dag=dag_main,
)

t2 = BashOperator(
    task_id='patient_track',
    bash_command=f'python {work_dir}/src/track_patient.py',
    dag=dag_main,
)
t3 = BashOperator(
    task_id='close_contact_track',
    bash_command=f'python {work_dir}/src/track_close_contact.py',
    dag=dag_main,
)

# t3_1 = BashOperator(
#     task_id='contact_between_patient',
#     depends_on_past=True, # must depend on upstream to complete
#     bash_command=f'python {work_dir}/src/track_patient_contact.py',
#     dag=dag_main,
# )

t4 = BashOperator(
    task_id='risky_contact',
    bash_command= f'python {work_dir}/src/find_risky_contact.py',
    dag=dag_main,
)

t5 = BashOperator(
    task_id='red_zone',
    bash_command= f'python {work_dir}/src/find_red_zones.py',
    dag=dag_main,
)

wrap_up= BashOperator(
    task_id='wrap_up',
    bash_command= f'python {work_dir}/src/update_run_time.py',
    dag=dag_main,
)
# ======================End of main pipeline=====================

# Ending Task
t_end = BashOperator(
    task_id='running_end',
    bash_command='echo Running End! Time: $(date +"%T")',
    trigger_rule= 'one_success', # if one branches is done
    dag=dag_main,
)

# Assemble main running pipline
dag_main >> t_start >> monitor >> t1 >> t2 >> t3 >> t4 >> t5 >> wrap_up >> t_end
# Branch if nothing to execute (no updates)
monitor >> check >> t_end

# DAG docs:
dag_main.doc_md = __doc__

t1.doc_md = """\
#### Task Documentation
Data Ingestion of files. It ingest two parts.
"""