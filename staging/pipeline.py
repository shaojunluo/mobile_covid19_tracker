from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'shaojun',
    'depends_on_past': False,
    'start_date': datetime(2020, 4, 23, 20, 5, 0), # time in utc
    'email': ['sjlocke.1989@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

# running pipelines
dag = DAG(
    'mobile_COVID_tracker',
    default_args=default_args,
    description='Running pipline for ELT',
    #schedule_interval=timedelta(days=1),
    schedule_interval= '@once',
)

# start command
t_start = BashOperator(
    task_id='running_start',
    bash_command='echo Running Start! Time: $(date +"%T")',
    priority_weight = 100, # this is main pipeline
    dag=dag,
)

t1 = BashOperator(
    task_id='data_ingest',
    depends_on_past=False,
    bash_command='python /Users/shaojun/Projects/COVID19/src/data_ingestion.py',
    priority_weight = 100, # this is main pipeline
    retries=1,
    dag=dag,
)
t2 = BashOperator(
    task_id='patient_track',
    depends_on_past=False, # run anyway if ingestion failed
    bash_command='python /Users/shaojun/Projects/COVID19/src/track_patient.py',
    priority_weight = 100, # this is main pipeline
    retries=1,
    dag=dag,
)
t3 = BashOperator(
    task_id='close_contact_track',
    depends_on_past=True, # must depend on patient_track to complete
    bash_command='python /Users/shaojun/Projects/COVID19/src/track_close_contact.py',
    retries = 1,
    priority_weight = 100, # this is main pipeline
    dag=dag,
)

t3_1 = BashOperator(
    task_id='contact_between_patient',
    depends_on_past=True, # must depend on upstream to complete
    bash_command='python /Users/shaojun/Projects/COVID19/src/track_patient_contact.py',
    retries = 0,
    priority_weight = 1, # not very important execute after main pipeline
    dag=dag,
)

# End Command
t_end = BashOperator(
    task_id='running_end',
    bash_command='echo Running Start! Time: $(date +"%T")',
    priority_weight = 100, # this is main pipeline
    dag=dag,
)

# main running pipline
dag >> t_start >> t1 >> t2 >> t3 >> t_end

# branch pipeline for parameter calculation
t3_1.set_upstream(t3)

# dag docs listed here
dag.doc_md = __doc__

t1.doc_md = """\
#### Task Documentation
Data Ingestion of files 
"""