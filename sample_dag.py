from datetime import datetime
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.utils.dates import days_ago
from kubernetes.client import models as k8s_models


default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "wait_for_downstream": True,
    "start_date": days_ago(0),
    "email": ["airflow@test.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "email_on_success": False,
    "queue": "default",
}

logger = logging.getLogger("airflow.task")


def print_args(**kwargs):
    print(datetime.now())

    return "ok"


dag = DAG(
    "sample_dag",
    default_args=default_args,
    description="테스트 DAG",
    schedule_interval=None,
)
dag.doc_md = __doc__

task1 = BashOperator(
    task_id="task1",
    bash_command="date && pwd && date >> /home/airflow/test.txt && date >> /opt/airflow/test.txt",
    dag=dag,
)
task1.doc_md = "### 테스트 Task ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)"

task3 = PythonOperator(task_id="task3", python_callable=print_args, dag=dag)

task1 >> task3
