from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
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
        dag_id='spark_app_chart_in_cluster_1',
        default_args=default_args, 
        schedule_interval=timedelta(days=1),
        tags=['pod','inside','bitnami']
    )
start_pod = KubernetesPodOperator(
    task_id="deploy_helm_pod",
    name="deploy_helm_pod",
    namespace="operators",
    image="alpine/helm",
    cmds=['/bin/sh', '-c', 'helm repo add bitnami https://charts.bitnami.com/bitnami && helm upgrade -i install tomcat bitnami/tomcat'],
    service_account_name="airflow-identity",
    do_xcom_push=False,
    is_delete_operator_pod=True,
    get_logs=True,
    kubernetes_conn_id="kubernetes_default",
    dag=dag
)
sensor = SparkKubernetesSensor(
    task_id='sensor_helm_app',
    namespace="operators",
    application_name="{{ task_instance.xcom_pull(task_ids='spark_pi_submit')['metadata']['name'] }}",
    kubernetes_conn_id="kubernetes_default",
    dag=dag,
    api_group="sparkoperator.k8s.io",
    attach_log=True
)
start_pod
