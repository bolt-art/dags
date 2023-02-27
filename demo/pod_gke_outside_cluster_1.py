from airflow import DAG
from airflow.providers.google.cloud.operators.kubernetes_engine import GKEStartPodOperator
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from google.oauth2 import service_account
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
now = datetime.now()
default_args = {
    "owner": "k8s-kubectl",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    'max_active_runs': 1,
    'retries': 0
}
dag = DAG(
        dag_id='pod_gke_outside_cluster_1',
        default_args=default_args, 
        schedule_interval=timedelta(days=1),
        tags=['pod','outside','bitnami']
    )
gcp_hook = GoogleCloudBaseHook(gcp_conn_id='google_cloud_default')
credentials = service_account.Credentials.from_service_account_file(
    '/usr/local/google/service_account.json',
    scopes=['https://www.googleapis.com/auth/cloud-platform']
)
delegated_credentials = credentials.with_subject('airflow-identity@artur-bolt-development.iam.gserviceaccount.com')
start_pod = GKEStartPodOperator(
    namespace="infra",
    image="bitnami/kubectl",
    cmds=["sh", "-c", "kubectl exec -ti --namespace operators spark-master-0 -- spark-submit --master spark://spark-master-svc.operators.svc.cluster.local:7077 --class org.apache.spark.examples.SparkPi https://artur-bolt-spark-jars.s3.eu-central-1.amazonaws.com/spark-examples_2.12-3.3.2.jar 1000"],
    name="kubectl-pod",
    do_xcom_push=False,
    is_delete_operator_pod=True,
    task_id="start-kubectl-pod",
    get_logs=True,        
    location="europe-central2",
    cluster_name="k8s-development",
    in_cluster=False,
    project_id="artur-bolt-development",
    delegated_credentials=delegated_credentials,
    dag=dag
)
start_pod
