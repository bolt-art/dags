from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.utils.dates import days_ago
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
    'retries': 0
}
dag = DAG(
    dag_id="spark_operator_outside_cluster",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    tags=['pod','outside','spark-operator'],
    template_searchpath=["/usr/local/spark/spark-operator/"]
)
spark_operator = SparkKubernetesOperator(
    task_id='spark_pi_submit',
    namespace='operators',
    application_file="spark_application_1.yaml",
    kubernetes_conn_id="custom_gke",
    dag=dag,
    api_group="sparkoperator.k8s.io"
)
start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)
start >> spark_operator >> end
