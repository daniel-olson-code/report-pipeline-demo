"""ETL process for multiple reports and regions using Apache Airflow.

This module defines an Airflow DAG that performs ETL (Extract, Transform, Load)
operations for multiple reports and regions. It uses task groups to organize
tasks for each region and report combination.

The DAG includes tasks for requesting reports, checking report status,
downloading reports, transforming data, and loading data.

Attributes:
    default_args (dict): Default arguments for the DAG and its tasks.
    config (dict): Configuration loaded from a YAML file.
    dag_version (str): Version string for the DAG.

Note:
    This script requires Apache Airflow and its dependencies to be installed.
"""

import os
import datetime
import yaml
from airflow.decorators import task, dag, task_group
from airflow.sensors.base import PokeReturnValue
from request_report import request_report
from check_report_status import check_report_status
from download_report import download_report
from transform_report import transform_report
from load_report import load_report

# Fix for Mac machines
os.environ['NO_PROXY'] = '*'

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': datetime.datetime(2024, 7, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(seconds=5),
}

with open('config/etl_config.yaml') as file:
    config = yaml.safe_load(file)

def fix_task_id(task_id):
    """
    Sanitize task ID to ensure it only contains valid characters.

    Args:
        task_id (str): The original task ID.

    Returns:
        str: The sanitized task ID.
    """
    return ''.join(char for char in task_id if char.isalnum() or char in ['-', '_']).lower()

dag_version = '_v23'

@dag(
    f'report_etl{dag_version}',
    default_args=default_args,
    schedule_interval='@daily',
)
def mydag():
    """Define the main DAG for the ETL process."""

    def create_etl_tasks(region, report):
        """
        Create a task group for a specific region and report combination.

        Args:
            region (str): The region for which to create tasks.
            report (str): The report type for which to create tasks.

        Returns:
            TaskGroup: A group of tasks for the specified region and report.
        """
        region_for_ids = fix_task_id(region)
        report_for_ids = fix_task_id(report)

        @task_group(group_id=f'{region_for_ids}_{report_for_ids}_etl{dag_version}')
        def this_group():
            """Define a group of tasks for a specific region and report."""

            @task(task_id=f'request_{report_for_ids}{dag_version}', multiple_outputs=True)
            def request(url, region, report, execution_date=None):
                """
                Request a report from the API.

                Args:
                    url (str): The API URL.
                    region (str): The region for the report.
                    report (str): The type of report.
                    execution_date (datetime, optional): The execution date.

                Returns:
                    dict: A dictionary containing the URL and report ID.
                """
                return {
                    'url': url,
                    'report_id': request_report(url, region, report, execution_date.to_date_string())
                }

            @task.sensor(
                task_id=f'check_{report_for_ids}_status{dag_version}',
                mode='poke',
                timeout=3600,
                poke_interval=300,
            )
            def check_status(url, report_id):
                """
                Check the status of a requested report.

                Args:
                    url (str): The API URL.
                    report_id (str): The ID of the requested report.

                Returns:
                    PokeReturnValue: An object indicating whether the report is ready.
                """
                return PokeReturnValue(
                    is_done='ready' == check_report_status(url, report_id),
                    xcom_value={'url': url, 'report_id': report_id}
                )

            @task(task_id=f'download_{report_for_ids}{dag_version}')
            def download(data):
                """
                Download the report data.

                Args:
                    data (dict): A dictionary containing the URL and report ID.

                Returns:
                    Any: The downloaded report data.
                """
                return download_report(data['url'], data['report_id'])

            @task(task_id=f'transform_{report_for_ids}{dag_version}')
            def transform(report_data):
                """
                Transform the report data.

                Args:
                    report_data (Any): The raw report data.

                Returns:
                    Any: The transformed report data.
                """
                return transform_report(report_data)

            @task(task_id=f'load_{report_for_ids}{dag_version}')
            def load(report_data):
                """
                Load the transformed report data.

                Args:
                    report_data (Any): The transformed report data.

                Returns:
                    Any: The result of the load operation.
                """
                return load_report(report_data)

            kwargs = {
                'url': config['api_url'],
                'region': region,
                'report': report
            }

            request_result = request(**kwargs)
            status_result = check_status(request_result)
            download_result = download(status_result)
            transform_result = transform(download_result)
            load(transform_result)

        return this_group()

    for region in config['region']:
        @task_group(group_id=f'{fix_task_id(region)}_etl{dag_version}')
        def account_group():
            """Create task groups for each report in the current region."""
            for report in config['reports']:
                create_etl_tasks(region, report)
        account_group()

mydag()


# # fix for mac machines
# import os
# os.environ['NO_PROXY'] = '*'
#
#
# from airflow.decorators import task, dag, task_group
# from airflow.sensors.base import PokeReturnValue
# import datetime
# import yaml
# from request_report import request_report
# from check_report_status import check_report_status
# from download_report import download_report
# from transform_report import transform_report
# from load_report import load_report
#
#
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': True,
#     'start_date': datetime.datetime(2024, 7, 25),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': datetime.timedelta(seconds=5),
# }
#
# with open('config/etl_config.yaml') as file:
#     config = yaml.safe_load(file)
#
#
# def fix_task_id(task_id):
#     return ''.join(char for char in task_id if char.isalnum() or char in ['-', '_'])
#
#
# dag_version = '22'
# dag_version = f'_v{dag_version}'
#
#
# @dag(
#     f'report_etl{dag_version}',
#     default_args=default_args,
#     schedule_interval='@daily',
# )
# def mydag():
#     """
#     ETL process for multiple reports and accounts
#     """
#     def create_etl_tasks(region, report):
#         region_for_ids = fix_task_id(region)
#         report_for_ids = fix_task_id(report)
#
#         @task_group(group_id=f'{region_for_ids}_{report_for_ids}_etl{dag_version}')
#         def this_group():
#             """
#             Some Group
#             """
#             @task(task_id=f'request_{report_for_ids}{dag_version}', multiple_outputs=True)
#             def request(url, region, report, execution_date=None):
#                 return {
#                     'url': url,
#                     'report_id': request_report(url, region, report, execution_date.to_date_string())
#                 }
#
#             @task.sensor(
#                 task_id=f'check_{report_for_ids}_status{dag_version}',
#                 mode='poke',
#                 timeout=3600,
#                 poke_interval=300,
#             )
#             def check_status(url, report_id):  # (data):
#                 return PokeReturnValue(
#                     is_done='ready' == check_report_status(url, report_id),#(data['url'], data['report_id']),
#                     xcom_value={'url': url, 'report_id': report_id}
#                 )
#
#             @task(task_id=f'download_{report_for_ids}{dag_version}')
#             def download(data):
#                 return download_report(data['url'], data['report_id'])
#
#             @task(task_id=f'transform_{report_for_ids}{dag_version}')
#             def transform(report_data):
#                 return transform_report(report_data)
#
#             @task(task_id=f'load_{report_for_ids}{dag_version}')
#             def load(report_data):
#                 return load_report(report_data)
#
#             kwargs = {
#                 'url': config['api_url'],
#                 'region': region,
#                 'report': report
#             }
#
#             request_result = request(**kwargs)
#             status_result = check_status(request_result)
#             download_result = download(status_result)
#             transform_result = transform(download_result)
#             load(transform_result)
#
#         return this_group()
#
#     for region in config['region']:
#         @task_group(group_id=f'{fix_task_id(region)}_etl{dag_version}')
#         def account_group():
#             for report in config['reports']:
#                 create_etl_tasks(region, report)
#         account_group()
#
#
# mydag()
#
# # # multi_report_etl_dag.py
# # from airflow import DAG
# # from airflow.operators.python import PythonOperator, BranchPythonOperator
# # from airflow.operators.empty import EmptyOperator
# # from airflow.sensors.python import PythonSensor
# # from airflow.utils.task_group import TaskGroup
# # from airflow.utils.trigger_rule import TriggerRule
# # from datetime import datetime, timedelta
# # import yaml
# # from request_report import request_report
# # from check_report_status import check_report_status
# # from download_report import download_report
# # from transform_report import transform_report
# # from load_report import load_report
# #
# # default_args = {
# #     'owner': 'airflow',
# #     'depends_on_past': True,
# #     'start_date': datetime(2024, 8, 1),
# #     'email_on_failure': False,
# #     'email_on_retry': False,
# #     'retries': 1,
# #     'retry_delay': timedelta(days=1),
# # }
# #
# # dag = DAG(
# #     'multi_report_etl',
# #     default_args=default_args,
# #     description='ETL process for multiple reports and accounts',
# #     schedule_interval='@daily',
# #     max_active_runs=1,
# # )
# #
# # # with open('config/etl_config.yaml') as file:
# # #     config = yaml.safe_load(file)
# # config = yaml.safe_load('''api_url: "http://localhost:8000"
# #
# # accounts:
# #   - account1
# #   - account2
# #   - account3
# #
# # reports:
# #   - "Ad Campaigns"
# #   - "Ad Groups"
# #   - "Product Ads"
# #   - "Keywords"
# #   - "Product Targets"''')
# #
# #
# # def fix_task_id(task_id):
# #     # only alphanumeric, characters, dashes and underscores
# #     return ''.join(char for char in task_id if char.isalnum() or char in ['-', '_'])
# #
# #
# # def create_etl_tasks(account, report):
# #     account_for_ids = fix_task_id(account)
# #     report_for_ids = fix_task_id(report)
# #
# #     def _decide_next_step(**kwargs):
# #         ti = kwargs['ti']
# #         status = ti.xcom_pull(task_ids=f'check_{report_for_ids}_status')
# #         if status == 'ready':
# #             return f'download_{report_for_ids}'
# #         elif status == 'failed':
# #             return f'report_{report_for_ids}_failed'
# #         return f'wait_{report_for_ids}'
# #
# #     with TaskGroup(group_id=f'{account_for_ids}_{report_for_ids}_etl', dag=dag) as tg:
# #         request = PythonOperator(
# #             task_id=f'request_{report_for_ids}',
# #             python_callable=request_report,
# #             op_kwargs={'url': config['api_url'], 'account': account, 'report': report},
# #         )
# #
# #         wait = EmptyOperator(task_id=f'wait_{report_for_ids}')
# #
# #         check_status = PythonSensor(
# #             task_id=f'check_{report_for_ids}_status',
# #             python_callable=check_report_status,
# #             op_kwargs={'url': config['api_url'], 'report_id': "{{ task_instance.xcom_pull(task_ids='request_" + report_for_ids + "') }}"},
# #             mode='poke',
# #             timeout=3600,  # 1 hour timeout
# #             poke_interval=300,  # Check every 5 minutes
# #         )
# #
# #         decide_next = BranchPythonOperator(
# #             task_id=f'decide_next_{report_for_ids}',
# #             python_callable=_decide_next_step,
# #         )
# #
# #         download = PythonOperator(
# #             task_id=f'download_{report_for_ids}',
# #             python_callable=download_report,
# #             op_kwargs={'url': config['api_url'], 'report_id': "{{ task_instance.xcom_pull(task_ids='request_" + report_for_ids + "') }}"},
# #         )
# #
# #         transform = PythonOperator(
# #             task_id=f'transform_{report_for_ids}',
# #             python_callable=transform_report,
# #             op_kwargs={'report_data': "{{ task_instance.xcom_pull(task_ids='download_" + report_for_ids + "') }}"},
# #         )
# #
# #         load = PythonOperator(
# #             task_id=f'load_{report_for_ids}',
# #             python_callable=load_report,
# #             op_kwargs={'report_data': "{{ task_instance.xcom_pull(task_ids='transform_" + report_for_ids + "') }}"},
# #         )
# #
# #         report_failed = EmptyOperator(task_id=f'report_{report_for_ids}_failed')
# #
# #         complete = EmptyOperator(
# #             task_id=f'complete_{report_for_ids}',
# #             trigger_rule=TriggerRule.ONE_SUCCESS,
# #         )
# #
# #         request >> check_status >> decide_next >> [wait, download, report_failed]
# #         wait >> check_status
# #         download >> transform >> load >> complete
# #         report_failed >> complete
# #
# #     return tg
# #
# #
# # for account in config['accounts']:
# #     with TaskGroup(group_id=f'{fix_task_id(account)}_etl', dag=dag) as account_group:
# #         for report in config['reports']:
# #             create_etl_tasks(account, report)
# #
# #
# # # from airflow import DAG
# # # from airflow.operators.python import PythonOperator, BranchPythonOperator
# # # from airflow.operators.dummy import DummyOperator
# # # from airflow.sensors.python import PythonSensor
# # # from airflow.utils.task_group import TaskGroup
# # # from airflow.utils.trigger_rule import TriggerRule
# # # from datetime import datetime, timedelta
# # # import yaml
# # # from scripts.request_report import request_report
# # # from scripts.check_report_status import check_report_status
# # # from scripts.download_report import download_report
# # # from scripts.transform_report import transform_report
# # # from scripts.load_report import load_report
# #
# # # default_args = {
# # #     'owner': 'airflow',
# # #     'depends_on_past': True,
# # #     'start_date': datetime(2024, 8, 1),
# # #     'email_on_failure': False,
# # #     'email_on_retry': False,
# # #     'retries': 1,
# # #     'retry_delay': timedelta(minutes=5),
# # # }
# #
# # # dag = DAG(
# # #     'multi_report_etl',
# # #     default_args=default_args,
# # #     description='ETL process for multiple reports and accounts',
# # #     schedule_interval='@daily',
# # #     max_active_runs=1,
# # # )
# #
# # # with open('/path/to/config/etl_config.yaml', 'r') as file:
# # #     config = yaml.safe_load(file)
# #
# # # def create_etl_tasks(account, report):
# # #     def _decide_next_step(**kwargs):
# # #         ti = kwargs['ti']
# # #         status = ti.xcom_pull(task_ids=f'check_{report}_status')
# # #         if status == 'ready':
# # #             return f'download_{report}'
# # #         elif status == 'failed':
# # #             return f'report_{report}_failed'
# # #         return f'wait_{report}'
# #
# # #     with TaskGroup(group_id=f'{account}_{report}_etl', dag=dag) as tg:
# # #         request = PythonOperator(
# # #             task_id=f'request_{report}',
# # #             python_callable=request_report,
# # #             op_kwargs={'account': account, 'report': report},
# # #         )
# #
# # #         wait = DummyOperator(task_id=f'wait_{report}')
# #
# # #         check_status = PythonSensor(
# # #             task_id=f'check_{report}_status',
# # #             python_callable=check_report_status,
# # #             op_kwargs={'account': account, 'report': report},
# # #             mode='poke',
# # #             timeout=3600,  # 1 hour timeout
# # #             poke_interval=300,  # Check every 5 minutes
# # #         )
# #
# # #         decide_next = BranchPythonOperator(
# # #             task_id=f'decide_next_{report}',
# # #             python_callable=_decide_next_step,
# # #         )
# #
# # #         download = PythonOperator(
# # #             task_id=f'download_{report}',
# # #             python_callable=download_report,
# # #             op_kwargs={'account': account, 'report': report},
# # #         )
# #
# # #         transform = PythonOperator(
# # #             task_id=f'transform_{report}',
# # #             python_callable=transform_report,
# # #             op_kwargs={'account': account, 'report': report},
# # #         )
# #
# # #         load = PythonOperator(
# # #             task_id=f'load_{report}',
# # #             python_callable=load_report,
# # #             op_kwargs={'account': account, 'report': report},
# # #         )
# #
# # #         report_failed = DummyOperator(task_id=f'report_{report}_failed')
# #
# # #         complete = DummyOperator(
# # #             task_id=f'complete_{report}',
# # #             trigger_rule=TriggerRule.ONE_SUCCESS,
# # #         )
# #
# # #         request >> check_status >> decide_next >> [wait, download, report_failed]
# # #         wait >> check_status
# # #         download >> transform >> load >> complete
# # #         report_failed >> complete
# #
# # #     return tg
# #
# # # for account in config['accounts']:
# # #     with TaskGroup(group_id=f'{account}_etl', dag=dag) as account_group:
# # #         for report in config['reports']:
# # #             create_etl_tasks(account, report)