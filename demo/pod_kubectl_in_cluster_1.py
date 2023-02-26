from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
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
        dag_id='pod_kubectl_in_cluster_1',
        default_args=default_args, 
        schedule_interval=timedelta(days=1),
        tags=['pod','inside','bitnami']
    )
start_pod = KubernetesPodOperator(
    namespace="operators",
    image="bitnami/kubectl",
    cmds=["sh", "-c", "kubectl exec -ti --namespace operators spark-master-0 -- spark-submit --master spark://spark-master-svc.operators.svc.cluster.local:7077 --class org.apache.spark.examples.SparkPi https://artur-bolt-spark-jars.s3.eu-central-1.amazonaws.com/spark-examples_2.12-3.3.2.jar 1000"],
    name="kubectl-pod",
    service_account_name="airflow-kubectl-pod",
    do_xcom_push=False,
    is_delete_operator_pod=True,
    in_cluster=True,
    task_id="start-kubectl-pod",
    get_logs=True,
    #kubernetes_conn_id="kubernetes_default",
    env_vars={
        "EXAMPLE_VAR": "/example/value",
        "GOOGLE_APPLICATION_CREDENTIALS": "/usr/local/google/service_account.json",
    },
    dag=dag
)
start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)
start >> start_pod >> end
