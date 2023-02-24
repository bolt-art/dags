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
    dag_id="spark_operator_outside_cluster_1",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    tags=['pod','outside','spark-operator'],
    template_searchpath=["/usr/local/spark/spark-operator/"]
)
spark_operator = SparkKubernetesOperator(
    task_id='spark_pi_submit',
    namespace='operators',
    application_file = """
apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: SparkApplication
metadata:
  name: k8s-spark-pi
spec:
  sparkConfigMap: configmap-spark-config
  type: Scala
  mode: cluster
  image: "gcr.io/spark-operator/spark:v3.1.1"
  imagePullPolicy: Always
  mainClass: org.apache.spark.examples.SparkPi
  mainApplicationFile: "local:///opt/spark/examples/jars/spark-examples_2.12-3.1.1.jar"
  sparkVersion: "3.1.1"
  restartPolicy:
    type: OnFailure
  driver:
    cores: 1
    coreLimit: "1200m"
    memory: "512m"
    serviceAccount: spark-pi
  executor:
    cores: 1
    instances: 1
    memory: "512m"
""",
    kubernetes_conn_id='kubernetes_target',
    dag=dag,
    api_group='sparkoperator.k8s.io'
)
start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)
start >> spark_operator >> end
