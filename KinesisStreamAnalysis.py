# -*- coding: utf-8 -*-
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from pyflink.table.window import Tumble
import os
import json

"""
~~~~~~~~~~~~~~~~~~~
1. create table environment
2. read from Kinesis Data Stream
3. create sink stream table
3. execute window
4. sink result to target S3 Bucket
"""

# 1. create Table Environment
env_settings = (
    EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
)
table_env = StreamTableEnvironment.create(environment_settings=env_settings)
statement_set = table_env.create_statement_set()

def create_source_table(table_name, stream_name, region, stream_initpos):
    
    return """ CREATE TABLE {0} (
                lambert_azimuthal_equal_area VARCHAR(50),
                realization VARCHAR(50),
                bnds VARCHAR(50),
                projection_y_coordinate DOUBLE,
                projection_y_coordinate_bnds DOUBLE,
                projection_x_coordinate DOUBLE,
                projection_x_coordinate_bnds DOUBLE,
                forecast_period DOUBLE,
                forecast_reference_time TIMESTAMP(3),
                time TIMESTAMP(3),
                WATERMARK FOR time AS time - INTERVAL '5' SECOND

              )
              PARTITIONED BY (lambert_azimuthal_equal_area)
              WITH (
                'connector' = 'kinesis',
                'stream' = '{1}',
                'aws.region' = '{2}',
                'scan.stream.initpos' = '{3}',
                'format' = 'json'
              ) """.format(
        table_name, stream_name, region, stream_initpos
    )
              
def create_sink_table(table_name, bucket_name):
    return """ CREATE TABLE {0} (
                time TIMESTAMP(3),
                realization VARCHAR(6),
                projection_x_coordinate DOUBLE,
                projection_y_coordinate DOUBLE,
                bnds VARCHAR(6),
                WATERMARK FOR time AS time - INTERVAL '5' SECOND

              )
              PARTITIONED BY (time)
              WITH (
                  'connector'='filesystem',
                  'path'='s3://{1}/',
                  'format'='csv',
                  'sink.partition-commit.policy.kind'='success-file',
                  'sink.partition-commit.delay' = '1 min'
              ) """.format(
        table_name, bucket_name)


def count_by_word(input_table_name):
    # use Table API
    input_table = table_env.from_path(input_table_name)

    tumbling_window_table = (
        input_table.window(
            Tumble.over("1.minute").on("time").alias("one_minute_window")
        )
        .group_by("group by fields")
        .select("select fields from input_table_name")
    )

    return tumbling_window_table

def main():
    
    input_stream = "kinesis-datastream"
    
    input_region = "ap-southeast-1"

    # define stream table
    input_table_name = "input_stream_table"
    output_table_name = "output_stream_table"

    stream_initpos = "LATEST"

    output_bucket_name = "kinesis-datastream-bucket/kinesis_output"

    # 2. create Kinesis Data Stream
    table_env.execute_sql(
        create_source_table(
            input_table_name, input_stream, input_region, stream_initpos
        )
    )
    # 3. crate sink stream table
    create_sink = create_sink_table(
        output_table_name, output_bucket_name
    )
    table_env.execute_sql(create_sink)

    # 4. execute window
    tumbling_window_table = count_by_word(input_table_name)

    # 5. write window result to target table
    tumbling_window_table.execute_insert(output_table_name).wait()

    statement_set.execute()
    
if __name__ == "__main__":
    main()
