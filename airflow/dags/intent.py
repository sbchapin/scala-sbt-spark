"""
This is the DAG for running the spark-intent-app within an AWS EMR Pipeline, using airflow.
"""
import logging
from datetime import timedelta
from os import getenv
from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.sensors.s3_prefix_sensor import S3PrefixSensor
from airflow.contrib.operators.s3_list_operator import S3ListOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.sensors.time_delta_sensor import TimeDeltaSensor
from airflow.utils.dates import days_ago
from airflow.models import Variable

DEFAULT_ARGS = {
    'owner': 'core-ip',
    'depends_on_past': True, # to guarantee no two hudi committers overwrite
    'email': ['core-ip@hginsights.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

ENV = getenv('ENV', default='staging')

DELIVERY_BUCKET = 'hg-raw-docs'
DELIVERY_PREFIX = 'intent/46/ingress/{{ ds }}/'

TRANSFORMED_BUCKET = 'hg-transformed-docs'
TRANSFORMED_PREFIX = 'intent/46/hudi/'

MRD_PIPELINE_BUCKET = 'hg-mrd-pipeline'
MRD_PIPELINE_URL_ALIAS_PREFIX = 'delivery/{{ execution_date.year }}-{{ execution_date.month }}-20/data_ops/alternate_urls/'

EMR_STEPS = [
    {
        'Name': 'run_url_alias_deltify',
        'ActionOnFailure': 'TERMINATE_CLUSTER',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'client',
                '--class', 'com.hgdata.spark.Main',
                '{{ var.value.intent_jar_path }}',
                'url-alias-deltify',
                '--input', '{{ ti.xcom_pull(task_ids="s3_alternate_url_deliveries") }}',
                '--output', 's3://hg-core-ip/hudi/alternate_urls',
                '--output-database', 'hg_intent',
             ],
        },
    },
    {
        'Name': 'run_intent',
        'ActionOnFailure': 'TERMINATE_CLUSTER',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode', 'client',
                '--class', 'com.hgdata.spark.Main',
                '{{ var.value.intent_jar_path }}',
                'intent-prep',
                '--input', f"s3://{DELIVERY_BUCKET}/{DELIVERY_PREFIX}",
                '--output', f"s3://{TRANSFORMED_BUCKET}/{TRANSFORMED_PREFIX}",
                '--output-database', 'hg_intent',
             ],
        },
    }
]

JOB_FLOW_OVERRIDES = {
    'Name': f"{DEFAULT_ARGS['owner']}-{ENV}-intent",
    'ReleaseLabel': 'emr-5.31.0',
    'LogUri': 's3://hg-logs/emr-logs/',
    'Tags': [
        {
            'Key': 'owner',
            'Value': DEFAULT_ARGS['owner']
        }, {
            'Key': 'env',
            'Value': ENV
        },
    ],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Master node',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5a.xlarge',
                'InstanceCount': 1,
                'EbsConfiguration': {
                    'EbsBlockDeviceConfigs': [
                        {
                            'VolumeSpecification': { 'SizeInGB': 64, 'VolumeType': 'gp2' },
                            'VolumesPerInstance': 1
                        }
                    ]
                }
            },
            {
                'Name': 'Core node',
                'Market': 'SPOT',
                'InstanceRole': 'CORE',
                'InstanceType': 'r5.2xlarge',
                'InstanceCount': 5,
                'EbsConfiguration': {
                    'EbsBlockDeviceConfigs': [
                        {
                            'VolumeSpecification': { 'SizeInGB': 400, 'VolumeType': 'gp2' },
                            'VolumesPerInstance': 1
                        }
                    ]
                }
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
        'Ec2KeyName': f"{DEFAULT_ARGS['owner']}",
        "Ec2SubnetId": '{{ var.value.emr_subnet }}',
        "ServiceAccessSecurityGroup": '{{ var.value.emr_service_access_sg }}',
        "EmrManagedMasterSecurityGroup": '{{ var.value.emr_managed_master_sg }}',
        "EmrManagedSlaveSecurityGroup": '{{ var.value.emr_managed_slave_sg }}',
        "AdditionalMasterSecurityGroups":[ '{{ var.value.emr_additional_master_sg }}' ]
    },
    'Applications': [
        { 'Name': 'Spark' },
        { 'Name': 'Hadoop' },
        { 'Name': 'Hive' },
        { 'Name': 'Ganglia' },
        { 'Name': 'Tez' },
    ],
    'Configurations': [
        {
            'Classification': 'spark',
            'Properties': { 'maximizeResourceAllocation': 'true' }
        },
        {
            'Classification': 'hive-site',
            'Properties': { 'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory' }
        },
        {
            'Classification': 'spark-hive-site',
            'Properties': { 'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory' }
        },
    ],
    'JobFlowRole': '{{ var.value.emr_job_flow_role }}',
    'ServiceRole': '{{ var.value.emr_service_role }}',
}

def get_last_prefix(bucket, prefix='', delimiter='/', aws_conn_id='aws_default'):
    requested_path = f"s3://{bucket}/{prefix}"
    logging.info(f"Querying for prefixes in {requested_path}, delimited by '{delimiter}'...")
    prefixes = S3Hook(aws_conn_id).list_prefixes(bucket, prefix, delimiter)
    if prefixes is None: raise ValueError(f"No sub-prefix found in {requested_path} delimited by '{delimiter}'")
    last_path = f"s3://{bucket}/{prefixes[-1]}"
    logging.info(f"Found {last_path}")
    return last_path

with DAG(
    dag_id='intent',
    default_args=DEFAULT_ARGS,
    start_date=days_ago(2),
    schedule_interval='0 0 * * 6' # At 00:00 on Saturday.
) as dag:

    s3_alternate_url_deliveries = PythonOperator(
        task_id='s3_alternate_url_deliveries',
        python_callable=get_last_prefix,
        op_kwargs={
            'bucket': MRD_PIPELINE_BUCKET,
            'prefix': MRD_PIPELINE_URL_ALIAS_PREFIX,
        }
    )

    s3_wait_for_intent_delivery = S3PrefixSensor(
        task_id="s3_wait_for_intent_delivery",
        bucket_name = DELIVERY_BUCKET,
        timeout = timedelta(days=1).total_seconds(), # if it isn't delivered after a day, give up
        poke_interval = timedelta(hours=1).total_seconds(), # check hourly
        prefix = DELIVERY_PREFIX,
        aws_conn_id = "aws_default"
    )

    emr_create_job_flow = EmrCreateJobFlowOperator(
        task_id='emr_create_job_flow',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='aws_default',
        emr_conn_id='emr_default',
    )

    emr_add_steps = EmrAddStepsOperator(
        task_id='emr_add_steps',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='emr_create_job_flow', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=EMR_STEPS,
    )

    emr_step_watchers = list(
        EmrStepSensor(
            task_id=f'emr_watch_step_{i}',
            job_flow_id="{{ task_instance.xcom_pull('emr_create_job_flow', key='return_value') }}",
            step_id=f"{{{{ task_instance.xcom_pull(task_ids='emr_add_steps', key='return_value')[{i}] }}}}",
            aws_conn_id='aws_default',
        ) for i, step in enumerate(EMR_STEPS)
    )

    emr_terminate_job_flow = EmrTerminateJobFlowOperator(
        task_id='emr_terminate_job_flow',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='emr_create_job_flow', key='return_value') }}",
        aws_conn_id='aws_default',
    )

    [s3_wait_for_intent_delivery, s3_alternate_url_deliveries] >> emr_create_job_flow >> emr_add_steps >> emr_step_watchers >> emr_terminate_job_flow
