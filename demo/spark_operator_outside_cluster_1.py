from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from google.oauth2 import service_account
from airflow.utils.dates import days_ago
default_args = {
    'owner': 'spark-operator',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 0
}
dag = DAG(
    dag_id="spark_operator_outside_cluster_1",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    tags=['pod','outside','spark-operator'],
    template_searchpath=["/usr/local/spark/spark-operator/"]
)
gcp_hook = GoogleCloudBaseHook(gcp_conn_id='google_cloud_default')
credentials = service_account.Credentials.from_service_account_file(
    '/usr/local/google/service_account.json',
    scopes=['https://www.googleapis.com/auth/cloud-platform']
)
delegated_credentials = credentials.with_subject('airflow-identity@artur-bolt-development.iam.gserviceaccount.com')
spark_operator = SparkKubernetesOperator(
    task_id='spark_pi_submit',
    namespace='operators',
    application_file="spark_application_1.yaml",
    #kubernetes_conn_id='kubernetes_target',
    delegate=delegated_credentials,
    dag=dag,
    api_group='sparkoperator.k8s.io'
)
sensor = SparkKubernetesSensor(
    task_id='spark_pi_submit_sensor',
    namespace="operators",
    application_name="{{ task_instance.xcom_pull(task_ids='spark_pi_submit')['metadata']['name'] }}",
    #kubernetes_conn_id="kubernetes_target",
    delegate=delegated_credentials,
    dag=dag,
    api_group="sparkoperator.k8s.io",
    attach_log=True
)
spark_operator >> sensor
