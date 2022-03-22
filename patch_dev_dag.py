from datetime import timedelta

from airflow.api.common.experimental import get_task_instance


def get_execution_date(execution_dt):
    task_instance = get_task_instance.get_task_instance("dag_name", "start",
                                                        execution_dt)  # change the 'dag_name' with the current dag name and "start" will be Dummy operatopr = Start
    execution_start_date = task_instance.start_date
    return execution_start_date.date() - timedelta(1)


def transaction_report_email(**kwargs):
    transaction_dt = get_task_instance(kwargs['execution_date'] - timedelta(0))
    """
    Rest will be the same 
    """
