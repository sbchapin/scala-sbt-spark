"""
This is the DAG for running the spark-intent-app within an AWS EMR Pipeline, using airflow.
"""
from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.sensors.s3_prefix_sensor import S3PrefixSensor
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.sensors.time_delta_sensor import TimeDeltaSensor
from airflow.utils.dates import days_ago
from airflow.models import Variable

# TODO: Still needs...
# - Input & Output path parameterization
# - Public subnet & static security group references (consider vars)

DEFAULT_ARGS = {
    'owner': 'core-ip',
    'depends_on_past': False,
    'email': ['core-ip@hginsights.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

EMR_STEPS = [
    {
        'Name': 'run_intent',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'client',
                '{{ var.value.intent_jar_path }}',
                '--input', "{{ task_instance.xcom_pull(task_ids='wait_for_delivery', key='return_value') }}",
                '--output', "s3://hg-testing/intent/46/hudi/"
             ],
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
    dag_id='intent',
    default_args=DEFAULT_ARGS,
    start_date=days_ago(2),
    schedule_interval='0 0 * * 6' # At 00:00 on Saturday.
) as dag:

    block_on_var = ExternalTaskSensor(
        task_id="init_var.intent_jar_path",
        external_dag_id="init",
        external_task_id="init_var.intent_jar_path",
        mode="reschedule"
    )

    wait_for_delivery = S3PrefixSensor(
        task_id="wait_for_delivery",
        bucket_name = 'hg-raw-docs',
        timeout = timedelta(days=1).total_seconds(), # if it isn't delivered after a day, give up
        poke_interval = timedelta(hours=1).total_seconds(), # check hourly
        prefix = 'intent/46/ingress/{{ ds }}/',
        aws_conn_id = "aws_default"
    )

    wait_for_raw_copy_completion = TimeDeltaSensor(
        task_id='wait_five_minutes',
        delta=timedelta(minutes=5),
    )

    create_job_flow = EmrCreateJobFlowOperator(
        task_id='create_job_flow',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='aws_default',
        emr_conn_id='emr_default',
    )


    add_steps = EmrAddStepsOperator(
        task_id='add_steps',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=EMR_STEPS,
    )

    step_watchers = list(
        EmrStepSensor(
            task_id=f'watch_step_{i}',
            job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
            step_id=f"{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[{i}] }}",
            aws_conn_id='aws_default',
        ) for i, step in enumerate(EMR_STEPS)
    )

    terminate_job_flow = EmrTerminateJobFlowOperator(
        task_id='terminate_job_flow',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id='aws_default',
    )

    [block_on_var, wait_for_delivery] >> create_job_flow >> add_steps >> step_watchers >> terminate_job_flow
    wait_for_delivery >> wait_for_raw_copy_completion >> add_steps