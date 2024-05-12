from airflow.decorators import dag,task
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os


@dag(
    dag_id="spark_submit_demo_iceberg",
    schedule='@daily',
    start_date=datetime(2024,1,1),
    catchup=False
)


def spark_submit_demo():
    
        start = PythonOperator(
                task_id="start",
                python_callable=lambda: print("Jobs started")
            )

        spark_submit_iceberg = SparkSubmitOperator(
                task_id="spark_submit_nessie_job",
                conn_id="spark-conn",
                application=f"{os.environ['AIRFLOW_HOME']}/include/spark_nessie_minio.py",
                packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.4_2.12:0.79.0,org.apache.hadoop:hadoop-aws:3.3.2,org.apache.iceberg:iceberg-aws-bundle:1.5.0",
                conf={
                    "spark.driver.memory": "1g",  
                    "spark.executor.memory": "1g", 
                    "spark.executor.instances": "1"  
                }
        )

        stop = PythonOperator(
                task_id="stop",
                python_callable=lambda: print("Jobs started")
            )

        start >> spark_submit_iceberg >> stop


spark_submit_demo()

