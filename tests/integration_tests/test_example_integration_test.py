from itertools import zip_longest

import pandas as pd
import boto3

from ml_pipeline.util.util import load_data_s3, get_files_in_s3_directory
from config.config import account


def test_output_pipeline_run(run_pipeline):
    pass


def test_output_files_present(run_pipeline, result_file_names):
    """This integration tests verifies whether all result files are pretent in the right place within the S3 bucket.

    :param run_pipeline: Fixture that run on a module level in order to execute the main pipeline
    :return:
    """
    # Get location of pipeline output from fixture
    # dir_test_data_pipeline_output = run_pipeline.dir_pipeline_output

    # Define expected file names
    expected_files = [f"{run_pipeline.dir_pipeline_output}{filename}" for filename in result_file_names]

    # Get files present in s3
    files_in_s3 = [
        file.key
        for file in get_files_in_s3_directory(run_pipeline.s3, run_pipeline.bucket, run_pipeline.dir_pipeline_output)
    ]
    # Assert
    difference = set(expected_files) - set(files_in_s3)
    assert not difference, f"missing following files in s3: {difference}"


def test_output_files_have_correct_columns(run_pipeline, result_file_columns):
    s3 = boto3.resource("s3")
    for filename, columns in result_file_columns.items():
        s3_cols = load_data_s3(s3, account.data_bucket_name, run_pipeline.dir_pipeline_output + filename).columns
        assert all(
            x == y for x, y in zip_longest(columns, s3_cols)
        ), f"in file {filename} expected the following columns:\n{columns}\nInstead found in s3:\n{s3_cols}"


def test_data_correctly_stored(run_pipeline, result_file_columns, test_output_data):
    """tests whether the old entries were not overwritten and the new entries are present."""
    s3 = boto3.resource("s3")
    for filename, df in test_output_data.items():
        s3df = load_data_s3(s3, account.data_bucket_name, run_pipeline.dir_pipeline_output + filename)
        # test that all old test data is present
        mrg = pd.merge(df, s3df, how="left", on=list(result_file_columns[filename]), indicator=True)
        assert all(mrg["_merge"] == "both"), f"some test data was lost in {filename}!"
