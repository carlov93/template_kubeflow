import pandas as pd
import datetime
from typing import Tuple, Dict
import collections
import numpy as np
import boto3
import logging

from ml_pipeline.util.data_class import KnownPattern, EventHistory, MetaData
from ml_pipeline.util.util import check_columns, load_data_s3, upload_data_s3, get_files_in_s3_directory
from ml_pipeline.util.util import timed

logger = logging.getLogger("smart_diagnosis")


@timed
def load_input_data_preprocessing() -> Tuple[EventHistory, KnownPattern, MetaData]:
    """This function loads data from S3 that is necessary for this pipeline step."""
    # Load Data From S3

    # Type conversion

    # Create Data Classes

    return dc_data_1, dc_data_2, dc_data_3, dc_data_4


@timed
def upload_output_data_preprocessing() -> bool:
    """This function collects kpis of all dataclasses and uploads all processed data to S3 that is needed in
    the following steps.
    """
    # Upload Data for next pipeline step

    # Load current tables

    # Concat old data with new data

    # Upload Data

    return True


@timed
def convert_dtyps_input_data():

    return dc_data_1, dc_data_2, dc_data_3, dc_data_4


@timed
def drop_duplicates(dc_data_1: EventHistory) -> EventHistory:
    """Currently there is a bug in the event history data. A Event is unique per dimension_a, dimension_b and system-time.
        This function drops the duplicates.

    Args:
        dc_data_1: DataClass containing the event history data

    Returns: DataClass containing the event history data without duplicates
    """

    return dc_data_1


def preprocessing_step_1(dc_data_1):

    return dc_data_1
