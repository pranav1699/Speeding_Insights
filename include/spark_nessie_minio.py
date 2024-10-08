import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, DateType, StructType, StructField, TimestampType
from pyspark.sql.functions import col, year
import os

# DEFINE SENSITIVE VARIABLES
NESSIE_URI = os.environ.get("NESSIE_URI", "http://nessie-server:19120/api/v1")  # Nessie Server URI
WAREHOUSE = os.environ.get("WAREHOUSE", "s3a://datalake")  # BUCKET TO WRITE DATA TOO
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY", 'minio')  # AWS CREDENTIALS
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_KEY", 'minioadmin')  # AWS CREDENTIALS
AWS_S3_ENDPOINT = os.environ.get("AWS_S3_ENDPOINT", 'http://172.21.0.10:9000')  # MINIO ENDPOINT

print(AWS_S3_ENDPOINT)
print(NESSIE_URI)
print(WAREHOUSE)

spark = (
    SparkSession
    .builder
    .appName('app_name')
    # .config('spark.packages',
    #         'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,'
    #         'org.apache.hadoop:hadoop-aws:3.3.2,'
    #         'org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.78.0,'
    #         'org.apache.iceberg:iceberg-aws-bundle:1.4.2')
    .config(
        'spark.sql.extensions',
        'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,'
        'org.projectnessie.spark.extensions.NessieSparkSessionExtensions')
    .config('spark.sql.catalog.nessie', 'org.apache.iceberg.spark.SparkCatalog')
    .config('spark.sql.catalog.nessie.uri', NESSIE_URI)
    .config('spark.sql.catalog.nessie.ref', 'main')
    .config('spark.sql.catalog.nessie.authentication.type', 'NONE')
    .config('spark.sql.catalog.nessie.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog')
    .config('spark.sql.catalog.nessie.s3.endpoint', "http://172.21.0.10:9000")
    .config('spark.sql.catalog.nessie.s3.access.key', "minio")
    .config('spark.sql.catalog.nessie.s3.secret.key', "minio123")
    .config('spark.sql.catalog.nessie.warehouse', "s3a://datalake")
    .config('spark.sql.catalog.nessie.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
    .config("spark.hadoop.fs.s3a.endpoint", "http://172.21.0.10:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
    .config("spark.executor.extraJavaOptions", "-Daws.accessKeyId=minio -Daws.secretAccessKey=minioadmin")
    .config("spark.driver.extraJavaOptions", "-Daws.accessKeyId=minio -Daws.secretAccessKey=minioadmin")
    # .config("spark.driver.host", "localhost")
    # .config("spark.driver.port", "7077")
    # .config("spark.hadoop.fs.s3a.signing-algorithm", "v4")
    # .config("spark.hadoop.fs.s3a.aws.credentials.provider",
    #         "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


def cars_stats_data():
    global new_rows_count
    car_schema = StructType([
                StructField("driver_number", IntegerType(), False),
                StructField("rpm", IntegerType(), False),
                StructField("speed", IntegerType(), False),
                StructField("n_gear", IntegerType(), False),
                StructField("throttle", IntegerType(), False),
                StructField("brake", IntegerType(), False),
                StructField("drs", IntegerType(), False),
                StructField("date", TimestampType(), False),
                StructField("session_key", IntegerType(), False),
                StructField("meeting_key", IntegerType(), False)
            ])

    car_data_df = spark.read.schema(car_schema).option("recursiveFileLookup", "true").csv("s3a://datalake/raw_data/car_data", header=True,
                                                    inferSchema=True)
    spark.sql("""create database if not exists nessie.demo""").show()
    # Checking The table exists or not
    if spark.catalog.tableExists("nessie.demo.formula_one_car"):
        car_iceberg = spark.table("nessie.demo.formula_one_car")
        print("$$$$$")
        car_iceberg.show(5)
        # if exist getting the max date
        last_updated_time = car_iceberg.selectExpr("max(date)").collect()[0][0]
        print("$$$$$")
        print(last_updated_time)

        # filtering the data using the max date value to get only the incremental and newly updated values
        car_data_df = car_data_df.where(col("date") > last_updated_time)
        new_rows_count = car_data_df.count()
        print("NEWLY ADDED ROWS : ", car_data_df.count())

        return car_data_df, new_rows_count
    else:
        return car_data_df, 0


# Defining a function to extract the driver details
def driver_details():
    driver_schema = StructType([
        StructField("broadcast_name", IntegerType(), nullable=True),
        StructField("country_code", StringType(), nullable=True),
        StructField("driver_number", StringType(), nullable=True),
        StructField("first_name", StringType(), nullable=True),
        StructField("full_name", StringType(), nullable=True),
        StructField("headshot_url", StringType(), nullable=True),
        StructField("last_name", StringType(), nullable=True),
        StructField("meeting_key", StringType(), nullable=True),
        StructField("name_acronym", StringType(), nullable=True),
        StructField("session_key", StringType(), nullable=True),
        StructField("team_colour", StringType(), nullable=True),
        StructField("team_name", StringType(), nullable=True)
    ])
    driver_data_df = spark.read.schema(driver_schema).csv("s3a://datalake/raw_data/driver_data.csv", header=True)

    return driver_data_df


def session_data_track():
    session_schema = StructType([
        StructField("location", StringType(), nullable=True),
        StructField("country_key", IntegerType(), nullable=True),
        StructField("country_code", StringType(), nullable=True),
        StructField("country_name", StringType(), nullable=True),
        StructField("circuit_key", StringType(), nullable=True),
        StructField("circuit_short_name", StringType(), nullable=True),
        StructField("session_type", StringType(), nullable=True),
        StructField("session_name", StringType(), nullable=True),
        StructField("date_start", TimestampType(), nullable=True),
        StructField("date_end", TimestampType(), nullable=True),
        StructField("gmt_offset", StringType(), nullable=True),
        StructField("session_key", IntegerType(), nullable=True),
        StructField("meeting_key", StringType(), nullable=True),
        StructField("year", StringType(), nullable=True)
    ])
    sessions_data_df = spark.read.schema(session_schema).csv("s3a://datalake/raw_data/sessions.csv", header=True)

    return sessions_data_df


def main(car_data, driver_data, sessions_data):
    enriched_car_data = (car_data
    .join(driver_data, driver_data['driver_number'] == car_data['driver_number'])
    .join(sessions_data, car_data['session_key'] == sessions_data['session_key'])
    .select(
        driver_data.full_name,
        driver_data.team_name,
        car_data.rpm,
        car_data.speed,
        car_data.n_gear,
        car_data.brake,
        car_data.drs,
        car_data.date,
        sessions_data.location,
        sessions_data.country_name
    )
    )
    print("Writing the enriched data ...")

    if spark.catalog.tableExists("nessie.demo.formula_one_car"):
        print("TABLE ALREADY EXISTS SO APPENDING THE DATA")
        (
            enriched_car_data
            .withColumn("year", year("date"))
            .writeTo("nessie.demo.formula_one_car")
            .using("iceberg")
            .partitionedBy("year","location", "full_name")
            .append()
        )

    else:
        print("TABLE NOT PRESENT CREATING THE TABLE")
        (
            enriched_car_data
            .withColumn("year", year("date"))
            .writeTo("nessie.demo.formula_one_car")
            .using("iceberg")
            .partitionedBy("year","location", "full_name")
            .create()

        )

    print("Done ...")


def read_enriched_data():
    # car_enriched_data = spark.read.parquet("lakehouse/formula-one/car_data/")
    car_enriched_data = spark.table("nessie.demo.formula_one_car")
    # car_enriched_data.show(5)
    print("TOTAL NUMBER OF ROWS IN THE TABLE : ", car_enriched_data.count())


if __name__ == '__main__':
    car_data, new_rows_count = cars_stats_data()
    driver_data = driver_details()
    sessions_data = session_data_track()
    if new_rows_count > 0 or spark.catalog.tableExists("nessie.demo.formula_one_car") == False:
        main(car_data, driver_data, sessions_data)
        read_enriched_data()
    else:
        print("NO NEW ROWS WERE ADDED")

# IF SPARK SUBMIT DOESNT WORK THEN ADD AWS_REGION IN UR ENVIRONMENT "export AWS_REGION=us-east-1"


# spark-submit --master spark://localhost:7077 --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.2,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.78.0,software.amazon.awssdk:bundle:2.17.178,software.amazon.awssdk:url-connection-client:2.17.178 spark_nessie_minio.py

# spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.projectnessie.nessie-integr
# ations:nessie-spark-extensions-3.4_2.12:0.78.0,org.apache.hadoop:hadoop-aws:3.3.2,org.apache.iceberg:iceberg-aws-bundle:1.4.2 --conf spark.executor.extraJavaOptions="-Daws.accessKe
# yId=minio -Daws.secretAccessKey=minio123" --conf spark.driver.extraJavaOptions="-Daws.accessKeyId=minio -Daws.secretAccessKey=minio123"  --master spark://localhost:7077  spark_nessie_minio.py
