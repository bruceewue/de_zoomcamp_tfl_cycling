#!/usr/bin/env python
# coding: utf-8

import argparse
from pyspark.sql import SparkSession


YOUR_DATAPROC_TEMPORARYGCSBUCKET_NAME = "YOUR_DATAPROC_TEMPORARYGCSBUCKET_NAME"

parser = argparse.ArgumentParser()

parser.add_argument("--input", required=True)
parser.add_argument("--output", required=True)

args = parser.parse_args()

input = args.input
output = args.output


spark = SparkSession.builder.appName("test").getOrCreate()

spark.conf.set("temporaryGcsBucket", YOUR_DATAPROC_TEMPORARYGCSBUCKET_NAME)

df = spark.read.parquet(input)

df.registerTempTable("cycling_data")

df_result = spark.sql(
    """
SELECT *
FROM
    cycling_data
"""
)

df_result.write.format("bigquery").option("table", output).save()
