from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'devops',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['devops@hginsights.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

variables = {
    'fac_script_path': 's3://hg-code/jars/functional-area-classifier/release/PDx2w2BB/spark-harness.py',
    'fac_egg_path':    's3://hg-code/jars/functional-area-classifier/release/PDx2w2BB/fac.egg'
}

def initialize_variable(variable_name, variable_value):
    actual_value = Variable.setdefault(variable_name, default = variable_value)
    if actual_value == variable_value:
      return f"{variable_name} <- {variable_value}"
    else:
      Variable.set(variable_name, variable_value)
      return f"{variable_name} <- {variable_value} (was previously {actual_value})"

with DAG(
    'init',
    default_args=default_args,
    description='Initializes all base variables for the Airflow server.  Expected to be called manually if variables updated.',
    schedule_interval='@once',
    catchup=False
) as dag:

    end = DummyOperator(task_id = "end")

    for variable_name, default_value in variables.items():
        op = PythonOperator(
            task_id=f"init_var.{variable_name}",
            python_callable=initialize_variable,
            op_kwargs={
                'variable_name': variable_name,
                'variable_value': default_value,
            }
        )
        op >> end
