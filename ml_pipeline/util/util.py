import io
import os
import json
import time
import logging
from functools import wraps
from datetime import datetime, timedelta
from pathlib import PurePosixPath, Path
from typing import Any, Dict, Union

import pandas as pd
import boto3
from cloudpathlib import S3Path, CloudPath

logger = logging.getLogger("set_mining")


def load_data_local(path) -> Union[pd.DataFrame, None]:
    path = (Path(__file__).parent.parent.parent / path).resolve()
    _, extension = os.path.splitext(path)
    if extension == ".csv":
        data = pd.read_csv(path)
    if extension == ".txt":
        with open(path) as f:
            data = f.read()
    if extension == ".json":
        with open(path, "r") as f:
            data = json.load(f)

    return data


def load_data_s3(s3: Any, bucket: str, data_key) -> pd.DataFrame:
    obj = s3.Object(bucket, data_key)
    _, extension = os.path.splitext(data_key)
    if extension == ".csv":
        df = pd.read_csv(io.BytesIO(obj.get()["Body"].read()), sep=",")
    elif extension == ".xlsx":
        df = pd.read_excel(io.BytesIO(obj.get()["Body"].read()))
    else:
        df = None
    return df


def upload_data_s3(data: Union[dict, pd.DataFrame, str, bytes], bucket: str, data_key: str):
    buffer = io.StringIO()
    boto3.setup_default_session(region_name="eu-west-1")
    s3 = boto3.resource("s3")
    _, extension = os.path.splitext(data_key)
    if extension == ".csv":
        data.to_csv(buffer, index=False, sep=",")
        s3.Object(bucket, data_key).put(Body=buffer.getvalue())
    elif extension == ".json":
        s3.Object(bucket, data_key).put(Body=(json.dumps(data).encode("UTF-8")))
    else:
        s3.Object(bucket, data_key).put(Body=data)


def check_columns(s3df: pd.DataFrame, newdf: pd.DataFrame):
    """
    makes sure the new dataframe has at least all the columns present en the s3 dataframe
    """
    assert all(
        col in newdf.columns for col in s3df.columns
    ), f"dataframe is missing the following columns: {set(s3df.columns) - set(newdf.columns)}"


def get_files_in_s3_directory(s3, bucket: str, *subdirs: str, recursive=False, debug=False):
    """
    gets all files (not folders!) matching the subdirs prefix.
    """
    bucket_s3 = s3.Bucket(bucket)
    p = PurePosixPath()
    for s in subdirs:
        p = p / s.lstrip("/")
    path = str(p)
    files = list(filter(lambda f: not f.key.endswith("/"), bucket_s3.objects.filter(Prefix=path)))
    if debug:
        print(f"found {len(files)} files in {path}")
    if recursive:
        return files
    else:
        return list(filter(lambda f: str(PurePosixPath(f.key).parent) == path, files))


def logging_setup(config: Dict):
    """
    setup logging based on the configuration

    :param config: the parsed config tree
    """
    log_conf = config["logging"]

    if log_conf["enabled"]:
        level = logging._nameToLevel[log_conf["level"].upper()]
    else:
        level = logging.NOTSET

    logger.setLevel(level)


def timed(func):
    """This decorator prints the execution time for the decorated function."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        msg = f"""Finished function {func.__name__} successfully in {round(end - start, 2)} seconds"""
        logger.info(msg)
        return result

    return wrapper


def convert_dec_to_hex(decimal_num):
    """Convert a decimal number into hexadecimal with leading 0x and paddin 0 where necessary

    Args:
        decimal_num (int):

    Returns:
        str: hexadecimal number, with leading 0x, a padding zero where necessary and uppercase characters
    """
    hex_num = format(decimal_num, "x")
    if len(hex_num) < 6:
        hex_num = "0" + hex_num
    final_hex_num = "0x" + hex_num.upper()
    return final_hex_num


def load_logger(config: Dict):
    # LOGGING
    logger = logging.getLogger("smart_diagnosis")
    # create console handler, set level and add to logger
    ch = logging.StreamHandler()
    logger.addHandler(ch)

    # create file handler, set level and add to logger
    log_stringio_obj = io.StringIO()
    s3_handler = logging.StreamHandler(log_stringio_obj)
    logger.addHandler(s3_handler)

    logging_setup(config)

    return logger, log_stringio_obj


def set_pipeline_step_log_level(level: str = "INFO", logging_format: str = None):
    if not logging_format:
        logging_format = "%(asctime)s - %(threadName)s - "
        logging_format += "%(name)s:%(lineno)d - %(levelname)s - %(message)s"
    logging.basicConfig(format=logging_format, level=level)


def pipeline_logging_config(_func=None, *, level: str = "INFO", logging_format: str = None):
    def decorator(func, *args, **kwargs):
        @wraps(func)
        def wrapper(*args, **kwargs):
            set_pipeline_step_log_level(level=level, logging_format=logging_format)
            result = func(*args, **kwargs)
            return result

        return wrapper

    if _func is None:
        return decorator
    else:
        return decorator(_func)


def get_cumulative_weeks(start_date: str, end_date: str):
    dates: list[datetime] = pd.date_range(
        start=datetime.strptime(start_date, "%Y-%m-%d"), end=datetime.strptime(end_date, "%Y-%m-%d"), freq="w-mon"
    ).to_pydatetime()
    return dates


def get_last_week():
    today = datetime.now()
    mon = datetime(today.year, today.month, today.day) - timedelta(days=today.weekday())
    return mon - timedelta(days=7), mon


# debug mode
def set_debug_mode(mode=True):
    os.environ["DEBUG"] = str(mode).lower()


def is_debug_mode():
    return "DEBUG" in os.environ and os.environ["DEBUG"] == "true"


def write_spark_df_to_s3(sql_df, bucket, *subdirs: str, mode="overwrite", header=True):
    p = Path()
    for s in subdirs:
        p = p / s.lstrip("/")
    path = f"s3://{bucket / p}/"
    return sql_df.write.csv(path, mode=mode, header=header)


def upload_result_files(location: CloudPath):
    """utility function to upload data/result_files to a specified s3 location. Can only be called locally"""
    result_files_folder = Path(__file__).parent.parent.parent / "data/result_files"
    assert result_files_folder.exists(), "can't upload output files: data/result_files/ not found"

    # print(f"uploading files to {location}")
    for file in result_files_folder.iterdir():
        # print(f"uploading {file.name}")
        (location / file.name).upload_from(file, force_overwrite_to_cloud=True)


def copy_result_files(location: CloudPath, origin: CloudPath):
    """utility function to copy template data/result_files to another s3 location"""
    for file in origin.iterdir():
        file.copy(location / file.name, force_overwrite_to_cloud=True)


def read_s3_csv(file: CloudPath):
    assert file.exists(), f"{file} not found!"
    return pd.read_csv(StringIO(file.read_text()))
