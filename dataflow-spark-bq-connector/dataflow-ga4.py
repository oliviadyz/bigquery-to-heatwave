from pyspark.sql import SparkSession

import oci
import os
import logging
import argparse

def get_dataflow_spark_session(app_name="big-query", spark_config={}):
    """
    Get a Spark session
    """
    spark_builder = SparkSession.builder.appName(app_name)
    # Add in extra configuration.
    for key, val in spark_config.items():
        spark_builder.config(key, val)

    # Create the Spark session.
    session = spark_builder.getOrCreate()
    return session

def main(date, credential, bucket, namespace, project, dataset, table):
    # Set up Spark.
    spark = get_dataflow_spark_session()

    # Initialize Log4j Logger from SparkContext
    log4j_logger = spark._jvm.org.apache.log4j.LogManager.getLogger(__name__)
    log4j_logger.info("PySpark Application started.")

    if namespace is None:
        log4j_logger.error("OCI namespace is not set...")
    else:
        log4j_logger.info("OCI namespace file is set..." + namespace)

    if bucket is None:
        log4j_logger.error("OCI bucket is not set...")
    else:
        log4j_logger.info("OCI bucket file is set..." + bucket)

    # Prepare the Google Service Account credential file in the Data Flow executable path python/lib
    log4j_logger.info("Credential is in " + credential)
    if credential is None:
        log4j_logger.error("Credential file is not here...")
    else:
        log4j_logger.info("Credential file is here...")

    # Read the Google Big Query table
    table_name = "{0}.{1}".format(dataset, table)
    df = spark.read.format('bigquery').option('project',project).option('parentProject',project).option("credentialsFile", credential).option('table', table_name).load()

    # Write to OCI Object Storage in Parquet format
    destination = "oci://{0}@{1}/bigquery/dataset/parquet/{2}".format(bucket, namespace, date)
    # Write to a single parquet file to reduce the output number
    df.coalesce(1).write.format("parquet").mode("overwrite").save(destination)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark Job - Google Analytics 4 to OCI")
    parser.add_argument("-date", type=str, required=True, help="Date to extract from Google BigQuery")
    parser.add_argument("-credential", type=str, required=True, help="credential file path of Google BigQuery")
    parser.add_argument("-bucket", type=str, required=True, help="OCI bucket name to store the exported data")
    parser.add_argument("-namespace", type=str, required=True, help="namespace name of the OCI bucket")
    parser.add_argument("-project", type=str, required=True, help="Project ID of the Google BigQuery")
    parser.add_argument("-dataset", type=str, required=True, help="Dataset ID of the Google BigQuery")
    parser.add_argument("-table", type=str, required=True, help="Table name of the Google BigQuery")
    args = parser.parse_args()
    main(args.date, args.credential, args.bucket, args.namespace, args.project, args.dataset, args.table)

