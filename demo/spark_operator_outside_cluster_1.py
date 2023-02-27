from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
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
spark_operator = SparkKubernetesOperator(
    task_id='spark_pi_submit',
    namespace='operators',
    application_file="spark_application_1.yaml",
    kubernetes_conn_id='kubernetes_target',
    dag=dag,
    api_group='sparkoperator.k8s.io'
)
sensor = SparkKubernetesSensor(
    task_id='spark_pi_submit_sensor',
    namespace="operators",
    application_name="{{ task_instance.xcom_pull(task_ids='spark_pi_submit')['metadata']['name'] }}",
    kubernetes_conn_id="kubernetes_target",
    dag=dag,
    api_group="parkoperator.k8s.io",
    attach_log=True
)
spark_operator >> sensor
