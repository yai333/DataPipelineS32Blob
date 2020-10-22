import json
import logging
import os
from datetime import datetime, timedelta
import awswrangler as wr

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

DATABASE_NAME = "s3datademo"
TABLE_NAME = "dailyobjects"


def handler(event, context):
    logger.info("Received event: " + json.dumps(event, indent=2))

    if DATABASE_NAME not in wr.catalog.databases().values:
        wr.catalog.create_database(DATABASE_NAME)

    event_date = datetime.strptime(
        event["Records"][0]["eventTime"], "%Y-%m-%dT%H:%M:%S.%fZ")

    partition_dt = f'{(event_date - timedelta(days=1)).strftime("%Y-%m-%d")}-00-00'
    previous_partition_dt = f'{(event_date - timedelta(days=2)).strftime("%Y-%m-%d")}-00-00'

    logger.debug(f"partition_dt: {partition_dt}")

    if not wr.catalog.does_table_exist(database=DATABASE_NAME, table=TABLE_NAME):
        table_query_exec_id = wr.athena.start_query_execution(s3_output=f"s3://{os.getenv('DESTINATION_BUCKET_NAME')}/athena_output",
                                                              sql=f"CREATE EXTERNAL TABLE {TABLE_NAME}( \
                                                                    `bucket` string, \
                                                                    key string, \
                                                                    version_id string, \
                                                                    is_latest boolean, \
                                                                    is_delete_marker boolean, \
                                                                    size bigint, \
                                                                    last_modified_date timestamp, \
                                                                    e_tag string \
                                                                    ) \
                                                                    PARTITIONED BY(dt string) \
                                                                    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' \
                                                                    STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat' \
                                                                    OUTPUTFORMAT  'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat' \
                                                                    LOCATION 's3://{os.getenv('DESTINATION_BUCKET_NAME')}/{os.getenv('SOURCE_BUCKET_NAME')}/demoDataBucketInventory0/hive/';",
                                                              database=DATABASE_NAME)

        wr.athena.wait_query(query_execution_id=table_query_exec_id)

    partition_query_exec_id = wr.athena.start_query_execution(
        sql=f"ALTER TABLE {TABLE_NAME} ADD IF NOT EXISTS PARTITION (dt=\'{partition_dt}\');",
        s3_output=f"s3://{os.getenv('DESTINATION_BUCKET_NAME')}/athena_output",
        database=DATABASE_NAME)

    wr.athena.wait_query(query_execution_id=partition_query_exec_id)

    select_query_exec_id = wr.athena.start_query_execution(sql='SELECT DISTINCT bucket as "' +
                                                           os.getenv('SOURCE_BUCKET_NAME') +
                                                           '" , key as "dump.txt" FROM ' +
                                                           TABLE_NAME +
                                                           " where dt = '" +
                                                           partition_dt +
                                                           "' and is_delete_marker = false" +
                                                           " except " +
                                                           'SELECT DISTINCT bucket as "' +
                                                           os.getenv('SOURCE_BUCKET_NAME') +
                                                           '" , key as "dump.txt" FROM ' +
                                                           TABLE_NAME +
                                                           " where dt = '" +
                                                           previous_partition_dt +
                                                           "' and is_delete_marker = false ;",
                                                           database=DATABASE_NAME,
                                                           s3_output=f"s3://{os.getenv('DESTINATION_BUCKET_NAME')}/csv_manifest/dt={partition_dt}")
    return select_query_exec_id
