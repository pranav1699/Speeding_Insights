import os
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id="formula_1_nessie_minio_spark",
    default_args={
        "owner": "pranav",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval="@daily"
)

start = PythonOperator(
    task_id="start",
    python_callable=lambda: print("Jobs started"),
    dag=dag
)

python_job = SparkSubmitOperator(
    task_id="python_job",
    conn_id="spark-conn",
    application=f"{os.environ['AIRFLOW_HOME']}/include/spark_nessie_minio.py",
    packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.4_2.12:0.78.0,org.apache.hadoop:hadoop-aws:3.3.2,org.apache.iceberg:iceberg-aws-bundle:1.4.2",
    conf={
        "spark.driver.memory": "1g",  # Adjust as per your requirement
        "spark.executor.memory": "1g",  # Adjust as per your requirement
        "spark.executor.instances": "1"  # Adjust as per your requirement
    },
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable=lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> python_job >> end