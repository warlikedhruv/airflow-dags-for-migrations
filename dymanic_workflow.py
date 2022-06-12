
import airflow
from airflow.operators.python_operator import PythonOperator
import os
from airflow.models import Variable
import logging
from airflow import configuration as conf
from airflow.models import DagBag, TaskInstance
from airflow import DAG, settings
from airflow.operators.bash_operator import BashOperator



def resetTasksStatus(task_id, execution_date):
    logging.info("Resetting: " + task_id + " " + execution_date)


    dag_folder = conf.get('core', 'DAGS_FOLDER')
    dagbag = DagBag(dag_folder)
    check_dag = dagbag.dags[dag_id]
    session = settings.Session()


    my_task = check_dag.get_task(task_id)
    ti = TaskInstance(my_task, execution_date)
    state = ti.current_state()
    logging.info("Current state of " + task_id + " is " + str(state))
    ti.set_state(None, session)
    state = ti.current_state()
    logging.info("Updated state of " + task_id + " is " + str(state))
