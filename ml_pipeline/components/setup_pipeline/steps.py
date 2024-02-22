import boto3
import pandas as pd

from ml_pipeline.util.util import load_data_s3, upload_data_s3, check_columns
from config.config import SetupPipelineConfig, PipelineConfigTuple

pipeline_names = {
    0: "Individual Run",
    1: "Simulation Run",
    2: "Recurring Run",
}


def upload_runinfo_to_output_tables(
    date: str,
    run_type: int,
    pipeline_version: str,
    pipeline_release: str,
    run_id: str,
    kf_run_id: str,
    config: SetupPipelineConfig,
) -> bool:
    """This function uploads the meta-data of the pipeline run to the run_info table.

    Args:
        date: Date of the execution
        run_type: String of the way how the pipeline will handle the data scope (one run, cumulative over weeks, ...)
        pipeline_version: Hash value created by sending the pipeline to the Kubeflow pipeline server
        pipeline_release: Release Number
        run_id: ID of the pipeline run
        kf_run_id: ID generated by kubeflow for the pipeline session
        config: config containing parameters for setup_pipeline step

    Returns: True if upload was successful
    """
    # load current table
    s3 = boto3.resource("s3")
    df_run_info = load_data_s3(
        s3,
        config["bucket"],
        config["dir_pipeline_output"] + config["output_data"]["run_info"],
    )

    # append new run info
    run_info = {
        "run_id": run_id,
        "date": date,
        "pipeline_name": pipeline_names[int(run_type)],
        "pipeline_version": pipeline_version,
        "release": pipeline_release,
        "flag_successful_run": False,
        "kf_run_id": kf_run_id,
    }
    # Concate df_run_info with new run_info
    new_df = pd.DataFrame(run_info, index=[0])
    check_columns(df_run_info, new_df)
    df_run_info = pd.concat([df_run_info, new_df], ignore_index=True)

    # upload updated table
    upload_data_s3(df_run_info, config["bucket"], config["dir_pipeline_output"] + config["output_data"]["run_info"])

    return True


def upload_hyperparam_to_output_tables(
    run_id: str,
    data_scope_param1: str,
    start_date: str,
    end_date: str,
    data_scope_param2: str,
    config: PipelineConfigTuple,
) -> bool:
    """This function uploads the hyperparameter of the pipeline run to the hyperparam_info table.
    Args:
        run_id: ID that is unique within a kubeflow run and identifies a run for a specific data scope
                (=iteration of a for loop)
        data_scope_param1: ID of data_scope_param1
        start_date: Start date of the data scope
        end_date: End date of the data scope
        data_scope_param2: name of data_scope_param2
        config: config containing parameters pipline steps

    Returns: True if upload was successful

    """
    # load current table
    s3 = boto3.resource("s3")
    df_hyperparameter = load_data_s3(
        s3,
        config.setup_pipeline["bucket"],
        config.setup_pipeline["dir_pipeline_output"] + config.setup_pipeline["output_data"]["hyperparam_info"],
    )

    # append new run hyperparameter
    used_hyperparameter = {
        "run_id": run_id,
        "data_scope_param1": data_scope_param1,
        "start_date": start_date,
        "end_date": end_date,
        "data_scope_param2": data_scope_param2,
        "max_gab_event_trigger_event": config.preprocessing["params"]["max_gab_event_trigger_event"],
        "max_time_diff_after_trigger_event": config.preprocessing["params"]["max_time_diff_after_trigger_event"],
        "deleted_event_classes": config.preprocessing["params"]["classes_to_be_deleted"],
        "threshold_min_freq": config.preprocessing["params"]["threshold_min_freq"],
        "clustering_approach": config.feature_engineering["params"]["clustering_approach"],
        "window_length": config.feature_engineering["params"]["window_length"],
        "min_support": config.set_mining["params"]["min_support"],
    }
    # concate df_run_info with new run_info
    new_df = pd.DataFrame(used_hyperparameter, index=[0])
    check_columns(df_hyperparameter, new_df)
    df_hyperparameter = pd.concat([df_hyperparameter, new_df], ignore_index=True)

    # upload updated table
    upload_data_s3(
        df_hyperparameter,
        config.setup_pipeline["bucket"],
        config.setup_pipeline["dir_pipeline_output"] + config.setup_pipeline["output_data"]["hyperparam_info"],
    )

    return True


def check_data_scope(
    bucket: str,
    location_s3: str,
    data_scope_dir: str,
) -> bool:
    """
    This function checks if the data scope was already extracted by a previous run of the pipeline.
    :param bucket: Name of the S3 bucket
    :param location_s3: S3 path to extracted data by glue jobs
    :param data_scope_dir: path to specific data scope data
    :return:
    """
    # Check if Glue job already extracted data with the given data scope
    s3 = boto3.resource("s3")
    bucket_s3 = s3.Bucket(bucket)
    s3_dir_extraction = location_s3 + data_scope_dir
    files_in_s3 = [file.key for file in bucket_s3.objects.filter(Prefix=s3_dir_extraction)]

    flag_already_extracted = bool(len(files_in_s3))  # False if 0, else True

    return flag_already_extracted