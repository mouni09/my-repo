import awswrangler as wr
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.databricks.operators.databricks import (
    DatabricksSubmitRunOperator,
    DatabricksRunNowOperator,
)

import boto3, json, time
from utils.slack_alert import task_fail_slack_alert_with_owner, custom_slack_alert
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


EMR_CLIENT = boto3.client('emr', region_name='ap-southeast-1')

TAGS=[
            {
                'Key': 'environment',
                'Value': 'prod'
            },
            {
                'Key': 'role',
                'Value': 'de_prod'
            },
            {
                'Key': 'application',
                'Value': 'emr'
            },
            {
                'Key': 'Name',
                'Value': 'cdc_scylla'
            },
            {
                'Key': 'cost',
                'Value': 'de'
            }
        ]  


args = {
    'owner': 'Abhay',
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'on_failure_callback': task_fail_slack_alert_with_owner
}

# def generate_streams_func(*args, **kwargs):
#     cluster = Cluster()
#     contact_points = ['node-0.aws-ap-southeast-1.8bf2a5bcc9a7e1af587a.clusters.scylla.cloud']
#     auth_provider = PlainTextAuthProvider(username='scylla', password='ghiUrN0wO7kcy1o')

#     cluster = Cluster(contact_points, auth_provider=auth_provider)
#     session = cluster.connect()
#     ts = list(session.execute("SELECT max(time) FROM system_distributed.cdc_generation_timestamps WHERE key = 'timestamps'"))
#     dt = list(ts[0])[0].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + '+0000'
#     streams = list(session.execute(f"SELECT streams FROM system_distributed.cdc_streams_descriptions_v2 WHERE time = '{dt}';"))

#     all_streams = []
#     for i in streams:
#         for byte_string in list(i)[0]:
#             all_streams.append(hex(int.from_bytes(byte_string, byteorder='big')))

#     x= { "streams" : all_streams }

#     import boto3

#     # Serialize the dictionary to JSON
#     json_data = json.dumps(x)

#     # Specify S3 bucket and file path
#     bucket_name = 'prod-pocketfm-de-databricks'
#     file_key = 'bronze/cdc/scylla/scripts/streams.json'  # Adjust the file name as needed

#     # Upload JSON data to S3
#     s3 = boto3.client('s3')
#     s3.put_object(Bucket=bucket_name, Key=file_key, Body=json_data) 


def modify(job_list, date):
    for job in job_list:
        if "HadoopJarStep" in job and "Args" in job["HadoopJarStep"]:
            args = job["HadoopJarStep"]["Args"]
            for i in range(len(args)):
                if args[i] == "--date":
                    args.insert(i + 1, date)
                    break
    return job_list 

def prepare_data_func(*args, **kwargs):
    """
    Function to create seed data for 
    
    return : None
    """
    ti = kwargs['ti']
    step_info = modify(json.loads(kwargs["step_info"]),kwargs['date'])
    job_flow_id = ti.xcom_pull(key='job_flow_id', task_ids=['create_emr_cluster'])[0]
    if job_flow_id:
        EMR_CLIENT.add_job_flow_steps(JobFlowId=job_flow_id, Steps=step_info)
        
# def training_test_func(*args, **kwargs):
#     """
#     Function to create training data
    
#     return : None
#     """
#     ti = kwargs['ti']
#     TRAINING_DATA_COINS_IN_HI = json.loads(kwargs['TRAINING_DATA_COINS_IN_HI'])
#     job_flow_id = ti.xcom_pull(key='job_flow_id', task_ids=['create_emr_cluster'])[0]
#     if job_flow_id:
#         EMR_CLIENT.add_job_flow_steps(JobFlowId=job_flow_id, Steps=TRAINING_DATA_COINS_IN_HI)
        
# def post_processing_func(*args, **kwargs):
#     """
#     Function to Apply processing Step
    
#     return : None
#     """
#     ti = kwargs['ti']
#     POST_PROCESSING_COINS_IN_HI = json.loads(kwargs['POST_PROCESSING_COINS_IN_HI'])
#     job_flow_id = ti.xcom_pull(key='job_flow_id', task_ids=['create_emr_cluster_2'])[0]
#     if job_flow_id:
#         EMR_CLIENT.add_job_flow_steps(JobFlowId=job_flow_id, Steps=POST_PROCESSING_COINS_IN_HI)
        
def get_instance_fleets(
    EMR_CONFIG,
    KAFKA_EMR_CONFIG,
    INSTANCE_FLEETS,
    FLEET_TYPE
    ):
    """
    Helper function to generate INSTANCE FLEETS

    return : INSTANCE_FLEETS
    """
    EMR_CONFIG = json.loads(EMR_CONFIG)
    KAFKA_EMR_CONFIG = json.loads(KAFKA_EMR_CONFIG)
    INSTANCE_FLEETS = json.loads(INSTANCE_FLEETS)
    if FLEET_TYPE == 'PRIMARY':
        INSTANCE_FLEETS[0]['InstanceTypeConfigs'][0]['Configurations'] = EMR_CONFIG
        for i in INSTANCE_FLEETS[1]['InstanceTypeConfigs']:
            i['Configurations']=EMR_CONFIG
        for i in INSTANCE_FLEETS[2]['InstanceTypeConfigs']:
            i['Configurations']=EMR_CONFIG
    elif(FLEET_TYPE == 'SECONDARY'):
        INSTANCE_FLEETS[0]['InstanceTypeConfigs'][0]['Configurations'] = KAFKA_EMR_CONFIG
        # INSTANCE_FLEETS[1]['InstanceTypeConfigs'][0]['Configurations'] = KAFKA_EMR_CONFIG
        for i in INSTANCE_FLEETS[1]['InstanceTypeConfigs']:
            i['Configurations']=KAFKA_EMR_CONFIG
        for i in INSTANCE_FLEETS[2]['InstanceTypeConfigs']:
            i['Configurations']=EMR_CONFIG
    return INSTANCE_FLEETS

def check_emr_status_func(*args, **kwargs):
    """
    Function to check the staus of EMR cluster
    
    return : rasies an exception if the cluster terminates with error
    """
    time.sleep(20)
    ti = kwargs['ti']
    count = 0
    STATUS = kwargs['STATUS']
    task_id = kwargs["task_id"]
    job_flow_id = ti.xcom_pull(key='job_flow_id', task_ids=[task_id])[0]
    state = EMR_CLIENT.describe_cluster(ClusterId=job_flow_id)['Cluster']['Status']['State']
    print(count, job_flow_id)
    while state not in [STATUS, 'TERMINATED','TERMINATED_WITH_ERRORS']:
        state = EMR_CLIENT.describe_cluster(ClusterId=job_flow_id)['Cluster']['Status']['State']
        count+=1
        print(state)  
        time.sleep(10)
  
    if state == 'TERMINATED_WITH_ERRORS':
        raise Exception("TERMINATED_WITH_ERRORS")

def terminate_emr_func(*args, **kwargs):
    """
    Function to treminate EMR cluster of given EMR cluster ID
        
    return : None
    """
    ti = kwargs['ti']
    task_id = kwargs["task_id"]
    job_flow_id = ti.xcom_pull(key='job_flow_id', task_ids=[task_id])[0]
    response = EMR_CLIENT.terminate_job_flows(JobFlowIds=[job_flow_id])

def create_emr_cluster_func(*args, **kwargs):
    """
    Function to create emr cluster

    returns: None
    """
    
    ti = kwargs['ti']
    INSTANCE_FLEETS = get_instance_fleets(
        kwargs["EMR_CONFIG"],
        kwargs["KAFKA_EMR_CONFIG"],
        kwargs["INSTANCE_FLEETS"],
        kwargs["FLEET_TYPE"]
    )
    KAFKA_EMR_CONFIG = json.loads(kwargs["KAFKA_EMR_CONFIG"])

    Instances = {
        'InstanceFleets': INSTANCE_FLEETS,
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
        'Ec2KeyName': 'crawling_machine_key_pair',
        'Ec2SubnetId': 'subnet-02bdb9d44f70f1dc7',
        'AdditionalMasterSecurityGroups': ['sg-00b0eeb42e958119a'],
        'AdditionalSlaveSecurityGroups': ['sg-00b0eeb42e958119a']
    }
    
    Applications=[
                    {'Name': 'Spark'},
                    {'Name': 'Ganglia'}
                ]

    Tags=[
            {
                'Key': 'cost',
                'Value': 'de'
            },
            {
                'Key': 'role',
                'Value': 'de_prod'
            },
            {
                'Key': 'application',
                'Value': 'emr',
            },
            {
                'Key': 'Name',
                'Value': 'CDC SYCLLA',
            }
        ]

    ManagedScalingPolicy={
        'ComputeLimits': {
            'UnitType': 'InstanceFleetUnits',
            'MinimumCapacityUnits': 16,
            'MaximumCapacityUnits': 144,
            'MaximumOnDemandCapacityUnits': 16,
            'MaximumCoreCapacityUnits': 64
        }
    }

    BootstrapActions=[
            {
                'Name': 'Bootstrap Action Default Config',
                'ScriptBootstrapAction': {
                    'Path': 's3://prod-pocketfm-de-databricks/bronze/cdc/scylla/scripts/scylla_req.sh'
                }
            }
        ]

    cluster = EMR_CLIENT.run_job_flow(
        Name=kwargs['name'],
        VisibleToAllUsers=True,
        Configurations=KAFKA_EMR_CONFIG,
        ReleaseLabel='emr-6.2.1',
        Instances=Instances,
        Applications=Applications,
        Tags=Tags,
        BootstrapActions=BootstrapActions,
        ManagedScalingPolicy=ManagedScalingPolicy,
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        LogUri=kwargs['EMR_LOGS_PATH']
    )

    job_flow_id = cluster.get('JobFlowId')
    ti.xcom_push(key='job_flow_id', value=job_flow_id)

def check_each_step_func(*args, **kwargs):
    """
    Function to check each step in the EMR cluster

    returns : Throws an exception if there is an error which generates slack alerts
    """
    time.sleep(20)
    ti = kwargs['ti']
    task_id = kwargs["task_id"]
    fail_flag = False
    job_flow_id = ti.xcom_pull(key='job_flow_id', task_ids=[task_id])[0]
    for i in EMR_CLIENT.list_steps(ClusterId=job_flow_id)['Steps']:
        step_id = i['Id']
        step_name = i['Name']
        status = i['Status']['State']
        print(step_name, status)
        if status != 'COMPLETED':
            custom_slack_alert(
                step_name = step_name,
                step_state=status,
                cluster_id=job_flow_id,
                step_id = step_id,
                custom_msg = False
            )
            fail_flag = True
            if status == 'FAILED':
                summary = f"The JOB {step_name} has failed"
                raise_pager_duty_alert( summary, "critical", step_name )

    if fail_flag == True:
        raise Exception("Some steps failed")
        

with DAG(
        dag_id='cdc_scylla',
        default_args=args,
        start_date=datetime(2024, 3, 14),
        schedule_interval='0 1 * * *',
        # schedule_interval=None,
        catchup=False,
        tags=['DE', 'PROD', 'Scylla']
) as dag:
    
    # generate_streams = PythonOperator(
    #     task_id='generate_streams',
    #     python_callable=generate_streams_func,
    #     depends_on_past=False,
    #     provide_context=True,
    #     op_kwargs={
    #     }
    # )
    
    create_emr_cluster = PythonOperator(
        task_id='create_emr_cluster',
        python_callable=create_emr_cluster_func,
        depends_on_past=False,
        provide_context=True,
        op_kwargs={
            "name" : "CDC SCYLLA",
            "TAGS" : TAGS,
            "KAFKA_EMR_CONFIG" : "{{ var.value.KAFKA_EMR_CONFIG }}",
            "INSTANCE_FLEETS" : "{{ var.value.PRIMARY_INSTANCE_FLEETS }}",
            "EMR_CONFIG" : "{{var.value.EMR_CONFIG}}",
            "FLEET_TYPE" : "PRIMARY",
            "EMR_LOGS_PATH" : "{{var.value.EMR_LOGS_PATH}}"
        }
    )
    
    device_experiments = PythonOperator(
        task_id='device_experiments',
        python_callable = prepare_data_func,
        depends_on_past=False,
        provide_context=True,
        op_kwargs={
            "step_info" : "{{var.value.cdc_scylla_device_experiments }}",
            #"date" : "2024-04-29"
            "date" : '{{ (logical_date).strftime("%Y-%m-%d") }}'
        }
    )
    
    user_experiments = PythonOperator(
        task_id='user_experiments',
        python_callable = prepare_data_func,
        depends_on_past=False,
        provide_context=True,
        op_kwargs={
            "step_info" : "{{var.value.cdc_scylla_user_experiments }}",
            # "date" : "2024-04-04"
            "date" : '{{ (logical_date).strftime("%Y-%m-%d") }}'
        }
    )

    survey_submissions = PythonOperator(
        task_id='survey_submissions',
        python_callable = prepare_data_func,
        depends_on_past=False,
        provide_context=True,
        op_kwargs={
            "step_info" : "{{var.value.cdc_scylla_survey_submissions }}",
            #"date" : "2025-01-08"
            "date" : '{{ (logical_date).strftime("%Y-%m-%d") }}'
        }
    )

    survey_submissions_answers = PythonOperator(
        task_id='survey_submissions_answers',
        python_callable = prepare_data_func,
        depends_on_past=False,
        provide_context=True,
        op_kwargs={
            "step_info" : "{{var.value.cdc_scylla_survey_submissions_answers }}",
            #"date" : "2025-01-06"
            "date" : '{{ (logical_date).strftime("%Y-%m-%d") }}'
        }
    )
        
    watch_step_1 = PythonOperator(
        task_id='watch_step_1',
        python_callable=check_emr_status_func,
        depends_on_past=False,
        provide_context=True,
        op_kwargs = {
            "STATUS" : "WAITING",
            "task_id" : "create_emr_cluster"
        }
    )
    
    check_step_1 = PythonOperator(
        task_id='check_step_1',
        python_callable=check_each_step_func,
        depends_on_past=False,
        provide_context=True,
        op_kwargs = {
            "task_id" : "create_emr_cluster"

        }
    )

    terminate_emr_cluster = PythonOperator(
        task_id='terminate_emr_cluster',
        python_callable=terminate_emr_func,
        depends_on_past=False,
        provide_context=True,
        op_kwargs = {
            "task_id" : "create_emr_cluster"
        }
    )  

    trigger_device_experiments_post_processing_workflow_dbs = DatabricksRunNowOperator(
        job_id=689927326637738,
        task_id="trigger_device_experiments_post_processing_workflow_dbs",
        databricks_conn_id="pocketfm-databricks",
    )  

    trigger_user_experiments_post_processing_workflow_dbs = DatabricksRunNowOperator(
        job_id=10143480943362,
        task_id="trigger_user_experiments_post_processing_workflow_dbs",
        databricks_conn_id="pocketfm-databricks",
    )

    trigger_survey_submission_answers_workflow_dbs = DatabricksRunNowOperator(
        job_id=214860777943643,
        task_id="trigger_survey_submission_answers_workflow_dbs",
        databricks_conn_id="pocketfm-databricks",
    )  

    trigger_survey_submssions_workflow_dbs = DatabricksRunNowOperator(
        job_id=916193605252541,
        task_id="trigger_survey_submssions_workflow_dbs",
        databricks_conn_id="pocketfm-databricks",
    )
    
    
(
    create_emr_cluster >> 
    device_experiments >> 
    user_experiments >>
    survey_submissions >>
    survey_submissions_answers >>
    watch_step_1 >> 
    check_step_1>>
    terminate_emr_cluster 
)
check_step_1>>trigger_device_experiments_post_processing_workflow_dbs
check_step_1>>trigger_user_experiments_post_processing_workflow_dbs
check_step_1>>trigger_survey_submission_answers_workflow_dbs
check_step_1>>trigger_survey_submssions_workflow_dbs