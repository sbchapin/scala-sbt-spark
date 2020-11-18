"""
This is the DAG for running the spark-intent-app within an AWS EMR Pipeline, using airflow.
"""
from datetime import timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.utils.dates import days_ago
from airflow.models import Variable

DEFAULT_ARGS = {
    'owner': 'core-ip',
    'depends_on_past': False,
    'email': ['core-ip@hginsights.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

EMR_STEPS = [
    {
        'Name': 'calculate_pi',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['/usr/lib/spark/bin/run-example', 'SparkPi', '10'],
        },
    }
]

JOB_FLOW_OVERRIDES = {
    'Name': f"{DEFAULT_ARGS['owner']}-intent",
    'ReleaseLabel': 'emr-5.31.0',
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Master node',
                'Market': 'SPOT',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm1.medium',
                'InstanceCount': 1,
            },
            {
                'Name': 'Core node',
                'Market': 'SPOT',
                'InstanceRole': 'CORE',
                'InstanceType': 'm1.medium',
                'InstanceCount': 1,
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
    },
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
}

with DAG(
    dag_id='emr_example',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    start_date=days_ago(2),
    schedule_interval='0 3 * * *'
) as dag:

    cluster_creator = EmrCreateJobFlowOperator(
        task_id='create_job_flow',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='aws_default',
        emr_conn_id='emr_default',
    )

    step_adder = EmrAddStepsOperator(
        task_id='add_steps',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=EMR_STEPS,
    )

    step_checkers = list(
        EmrStepSensor(
            task_id=f'watch_step_{i}',
            job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
            step_id=f"{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[{i}] }}",
            aws_conn_id='aws_default',
        ) for i, step in enumerate(EMR_STEPS)
    )

    cluster_remover = EmrTerminateJobFlowOperator(
        task_id='remove_cluster',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id='aws_default',
    )

    cluster_creator >> step_adder >> step_checkers >> cluster_remover
