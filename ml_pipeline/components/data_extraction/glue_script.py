from collections import defaultdict
import sys
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, List

from pyspark.sql.functions import col
import boto3

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession


# generic query
unformatted_query_str = """
SELECT DISTINCT 
...

FROM {0}.{1} readout 
INNER JOIN {0}.{2} xyz
ON xyz.id = readout.id
WHERE 1=1
;
"""


# util funcs
def get_query_str(model: str, start_date: datetime, end_date: datetime):
    return unformatted_query_str.format(
        args["db_name"],
        args["table_name_1"],
        args["table_name_2"],
        model,
        start_date,
        end_date,
    )


def write_spark_df_to_s3(sql_df, bucket, *subdirs: str, mode="overwrite", header=True):
    p = Path()
    for s in subdirs:
        p = p / s.lstrip("/")
    path = f"s3://{bucket / p}/"
    return sql_df.write.csv(path, mode=mode, header=header)


# Setup Spark
spark = SparkSession.builder.getOrCreate()
glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)

# Get Data Scope Parameters
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "RUN_TIMESTAMP",
        "db_name",
        "table_name_1",
        "table_name_2",
        "data_scope_dir_prefix",
        "should_extract_dict_location",
        "bucket",
        "destination_path",
    ],
)
print(f"extracting data based on {args['should_extract_dict_location']}")
obj = boto3.resource("s3").Object(args["bucket"], args["should_extract_dict_location"])
should_extract_dict: Dict[str, bool] = json.loads(obj.get()["Body"].read())
print(f"should_extract_dict: {should_extract_dict}")
missing_extraction = [k for k, v in should_extract_dict.items() if v]
assert missing_extraction, "dates missing extraction is empty"

# nested dict of data to extract
tmp_extract_dict = defaultdict(lambda: defaultdict(lambda: []))
for data_scope in missing_extraction:
    model, abc, date_range = data_scope.split("/")
    tmp_extract_dict[model][date_range].append(abc)

# convert to regular dicts (so it prints nicely)
extract_dict: Dict[str, Dict[str, List[str]]] = dict((k, dict(d)) for k, d in tmp_extract_dict.items())
print(f"extract_dict: {extract_dict}")


# Initialise Job
job.init(args["JOB_NAME"], args)

# MAIN LOOP
# FOR EACH MODEL
for model, dates_dict in extract_dict.items():
    # get relevant end dates
    start_date_str = list(dates_dict.keys())[0].split("_")[0]
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")

    end_dates_str = [k.split("_")[1] for k in dates_dict.keys()]
    # sort in reverse, that is, latest time to earliest time
    end_dates_str = sorted(end_dates_str, reverse=True)
    latest_end_date = datetime.strptime(end_dates_str[0], "%Y-%m-%d")

    # Execute Spark Query
    query = get_query_str(model, start_date, latest_end_date)
    df_all_trigger_complete = spark.sql(query).cache()

    # we can progressively filter based only on the previous time range, instead of the querring the db again
    df = df_all_trigger_complete
    for end_date_str in end_dates_str:
        # recover the dict key
        key = f"{start_date_str}_{end_date_str}"
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        df = df.filter(col("readout_timestamp") < end_date)

        # FOR EACH ABC
        for abc in dates_dict[key]:
            print(f"{datetime.now()}: {model} - {key} - {abc}")
            data_scope = f"{model}/{abc}/{key}"

            # Filter events to specific abc based on data scope parameters
            df_event_history = df.filter(df.abc == abc)

            # Store events regarding data scope
            write_spark_df_to_s3(
                df_event_history.coalesce(1),
                args["bucket"],
                args["destination_path"],
                args["data_scope_dir_prefix"],
                data_scope,
            )

job.commit()
