"""
### Run a dbt Core project as a task group with Cosmos

Simple DAG showing how to run a dbt project as a task group, using
an Airflow connection and injecting a variable into the dbt project.
"""

from airflow.decorators import dag
from cosmos.providers.dbt.task_group import DbtTaskGroup
from pendulum import datetime
from airflow.utils.task_group import TaskGroup
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from cosmos.providers.dbt.core.operators import (
    DbtTestOperator,
    DbtDepsOperator
  )

CONNECTION_ID = "snowflake_conn"
DB_NAME = "TELECOM_DATABASE"
SCHEMA_NAME = "CLEANSED"
DBT_PROJECT_NAME = "my_simple_dbt_project"
# the path where Cosmos will find the dbt executable
# in the virtual environment created in the Dockerfile
DBT_EXECUTABLE_PATH = "/usr/local/airflow/dbt_venv/bin/dbt"
# The path to your dbt root directory
DBT_ROOT_PATH = "/usr/local/airflow/dags/dbt"
SNOWFLAKE_CONN_ID ="snowflake_conn_id"


@dag(
    start_date=datetime(2023, 6, 1),
    schedule=None,
    catchup=False,
)
def my_simple_dbt_dag():

    with TaskGroup(group_id='s3_to_snowflake') as tg_s3_to_snowflake:

        copy_into_crm1_table = S3ToSnowflakeOperator(
            task_id="copy_into_crm1",
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            s3_keys=["crm1.csv"],
            table="crm1",
            stage="my_s3_stage",
            file_format="(type = csv ,field_delimiter = ',',skip_header=1,null_if = ('NULL', 'null'), empty_field_as_null = true,FIELD_OPTIONALLY_ENCLOSED_BY='\"')"
        )

        copy_into_devices1_table = S3ToSnowflakeOperator(
            task_id="copy_into_device1",
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            s3_keys=["device1.csv"],
            table="device1",
            stage="my_s3_stage",
            file_format="(type = csv ,field_delimiter = ',',skip_header=1,null_if = ('NULL', 'null'), empty_field_as_null = true,FIELD_OPTIONALLY_ENCLOSED_BY='\"')"
        )

        copy_into_rev1_table = S3ToSnowflakeOperator(
            task_id="copy_into_rev1",
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            s3_keys=["rev1.csv"],
            table="rev1",
            stage="my_s3_stage",
            file_format="(type = csv ,field_delimiter = ',',skip_header=1,null_if = ('NULL', 'null'), empty_field_as_null = true,FIELD_OPTIONALLY_ENCLOSED_BY='\"')"
        )

        [copy_into_crm1_table, copy_into_devices1_table,copy_into_rev1_table ]

    transform_data=DbtTaskGroup(
            group_id="transform_data",
            dbt_project_name=DBT_PROJECT_NAME,
            conn_id=CONNECTION_ID,
            dbt_root_path=DBT_ROOT_PATH,
            dbt_args={
                "dbt_executable_path": DBT_EXECUTABLE_PATH,
                "schema": SCHEMA_NAME,
                 "install_deps" : True
            },
        )

#     installingDbtExpectations = DbtDepsOperator(
#            task_id = "dbt_expectations_test",
#            dbt_executable_path=DBT_EXECUTABLE_PATH,
#            project_dir = DBT_ROOT_PATH,
#        )

    with TaskGroup(group_id='dbt_expectations_test') as tg_dbt_expectations_test:
        dbt_expectations_revenue_test = DbtTestOperator(
           task_id = "dbt_expectations_revenue_test",
           conn_id = CONNECTION_ID,
           dbt_executable_path=DBT_EXECUTABLE_PATH,
           project_dir = "/usr/local/airflow/dags/dbt/my_simple_dbt_project",
           models = 'Cleansed_revenue1',
           install_deps = True
        )

        dbt_expectations_crm_test = DbtTestOperator(
               task_id = "dbt_expectations_crm_test",
               conn_id = CONNECTION_ID,
               dbt_executable_path=DBT_EXECUTABLE_PATH,
               project_dir = "/usr/local/airflow/dags/dbt/my_simple_dbt_project",
               models = 'Cleansed_crm1',
               install_deps = True
           )

        dbt_expectations_device_test = DbtTestOperator(
               task_id = "dbt_expectations_device_test",
               conn_id = CONNECTION_ID,
               dbt_executable_path=DBT_EXECUTABLE_PATH,
               project_dir = "/usr/local/airflow/dags/dbt/my_simple_dbt_project",
               models = 'Cleansed_device1',
               install_deps = True
           )


        [dbt_expectations_revenue_test, dbt_expectations_crm_test,dbt_expectations_device_test ]


    tg_s3_to_snowflake>>tg_dbt_expectations_test>>transform_data


my_simple_dbt_dag()