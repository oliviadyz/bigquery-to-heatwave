import io
import json
import logging
import mysql.connector

from fdk import response
from datetime import datetime, timedelta
import oci
import os


def handler(ctx, data: io.BytesIO = None):
    logger = logging.getLogger()
    try:

        # MySQL database connection details
        db_config = {
            "host": os.environ.get("DB_HOST"),
            "port": os.environ.get("DB_PORT"),
            "user": os.environ.get("DB_USER"),
            "password": os.environ.get("DB_PASS"),
            "database": os.environ.get("DB_NAME")
        }

        # OCI Bucket details
        bucket_value = os.environ.get("OCI_BUCKET")
        region_value = os.environ.get("OCI_REGION")
        namespace_value = os.environ.get("OCI_NAMESPACE")
        
        # Connect to MySQL database
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(buffered=True)

        # HeatWave MySQL script
        mysql_script = """
        SET @input_list = '[
          {
            "db_name": "big_query",
            "tables": [
              "ga_events_json",
              {
                "table_name": "ga_events_json",
                "engine_attribute": {
                  "dialect": {"format": "json"},
                  "file":[{"bucket": "OCI_BUCKET", "region" : "OCI_REGION", "namespace": "OCI_NAMESPACE"}]
                }
              }
            ]
          }
        ]';
        SET @options = JSON_OBJECT('mode', 'normal', 'refresh_external_tables', TRUE);
        CALL sys.heatwave_load(CAST(@input_list AS JSON), @options);
        """
       
        mysql_script = mysql_script.replace('OCI_BUCKET', f'{bucket_value}')
        mysql_script = mysql_script.replace('OCI_REGION', f'{region_value}')
        mysql_script = mysql_script.replace('OCI_NAMESPACE', f'{namespace_value}')

        # Execute MySQL script
        for statement in mysql_script.split(";"):
            if statement.strip():
                cursor.execute(statement)

        cursor.close()
        conn.close()

        return {"message": "HeatWave load executed successfully!"}

    except Exception as e:
        return {"error": str(e)}

