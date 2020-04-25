from datetime import datetime, timedelta
import pandas as pd
import yaml
import sys
import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import BranchPythonOperator

default_args = {
    'owner': 'shaojun',
    'depends_on_past': False,
    'start_date': days_ago(0, minute = 3), # time in utc
    'email': ['sjlocke.1989@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# running pipelines
dag_main = DAG(
    'mobile_COVID_tracker',
    default_args=default_args,
    description='Running pipline for ELT',
    schedule_interval= timedelta(minutes = 5),
    max_active_runs = True
)

# start command
t_start = BashOperator(
    task_id='running_start',
    bash_command='echo Running Start! Time: $(date +"%T")',
    priority_weight = 100, # this is main pipeline
    dag=dag_main,
)

t1 = BashOperator(
    task_id='data_ingest',
    depends_on_past=False,
    bash_command='python /Users/shaojun/Projects/COVID19/src/data_ingestion.py',
    priority_weight = 100, # this is main pipeline
    retries=1,
    dag=dag_main,
)
t2 = BashOperator(
    task_id='patient_track',
    depends_on_past=True, # run anyway if ingestion failed
    bash_command='python /Users/shaojun/Projects/COVID19/src/track_patient.py',
    priority_weight = 100, # this is main pipeline
    retries=1,
    dag=dag_main,
)
t3 = BashOperator(
    task_id='close_contact_track',
    depends_on_past=True, # must depend on patient_track to complete
    bash_command='python /Users/shaojun/Projects/COVID19/src/track_close_contact.py',
    retries = 1,
    priority_weight = 100, # this is main pipeline
    dag=dag_main,
)

t3_1 = BashOperator(
    task_id='contact_between_patient',
    depends_on_past=True, # must depend on upstream to complete
    bash_command='python /Users/shaojun/Projects/COVID19/src/track_patient_contact.py',
    retries = 1,
    priority_weight = 1, # not very important execute after main pipeline
    dag=dag_main,
)

t4 = BashOperator(
    task_id='risky_contact',
    depends_on_past=True, # must depend on patient_track to complete
    bash_command='python /Users/shaojun/Projects/COVID19/src/find_risky_contact.py',
    retries = 1,
    priority_weight = 100, # this is main pipeline
    dag=dag_main,
)

t5 = BashOperator(
    task_id='red_zone',
    depends_on_past=True, # must depend on patient_track to complete
    bash_command='python /Users/shaojun/Projects/COVID19/src/find_red_zones.py',
    retries = 1,
    priority_weight = 100, # this is main pipeline
    dag=dag_main,
)

wrap_up= BashOperator(
    task_id='wrap_up',
    depends_on_past=True, # must depend on patient_track to complete
    bash_command='python /Users/shaojun/Projects/COVID19/src/update_run_time.py',
    retries = 1,
    priority_weight = 100, # this is main pipeline
    dag=dag_main,
)

check = BashOperator(
    task_id='check_ok',
    bash_command='echo File up tp date. Nothing executed',
    priority_weight = 100, # this is main pipeline
    trigger_rule= 'one_success', # if one branches is done
    dag=dag_main,
)
# End Command
t_end = BashOperator(
    task_id='running_end',
    bash_command='echo Running End! Time: $(date +"%T")',
    priority_weight = 100, # this is main pipeline
    trigger_rule= 'one_success', # if one branches is done
    dag=dag_main,
)

# check the whether the lasted file version is updated and trigger the next
def check_latest(**kwargs):
    with open(os.path.dirname(__file__) + '/config_status.yaml') as f:
        file_status = yaml.safe_load(f)
        # find the trigger of data change
        trigger = False
        for status in file_status['data'].values():
            trigger |= pd.Timestamp(status['last_update']) > pd.Timestamp(file_status['last_run'])
        if trigger: # trigger the main pipeline
            return 'data_ingest'
        else:
            return 'check_ok'

# monitor the branching
monitor = BranchPythonOperator(
    task_id = 'monitor_update',
    python_callable = check_latest,
    provide_context=True,
    priority_weight = 10, 
    dag=dag_main
)

# main running pipline
dag_main >> t_start >> monitor >> t1 >> t2 >> t3 >> t3_1 >> t4 >> t5 >> wrap_up >> t_end
# branch if nothing to execute
monitor >> check >> t_end

# dag docs listed here
dag_main.doc_md = __doc__

t1.doc_md = """\
#### Task Documentation
Data Ingestion of files 
"""