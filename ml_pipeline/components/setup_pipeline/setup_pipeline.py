from typing import NamedTuple
import uuid
from datetime import datetime

from kfp.components import func_to_container_op
from cloudpathlib import S3Path

from config.util import config_from_dict, config_to_dict, update_config
from ml_pipeline.components.setup_pipeline.steps import (
    upload_runinfo_to_output_tables,
    upload_hyperparam_to_output_tables,
)
from ml_pipeline.util.s3util import copy_result_files
from ml_pipeline.util.util import timed, pipeline_logging_config

SetupOutput = NamedTuple(
    "SetupOutput",
    [
        ("run_id", str),
        ("data_scope_param1", str),
        ("data_scope_param2", str),
        ("config_preprocessing", dict),
        ("config_feature_engineering", dict),
        ("config_set_mining", dict),
        ("config_postprocessing", dict),
    ],
)


@timed
@pipeline_logging_config
def setup_pipeline(
    pipeline_metadata: dict,
    data_scope_dir: str,
    _config: dict,
    *,
    input_data: dict = {},
    output_data: dict = {},
    params: dict = {},
) -> SetupOutput:
    """This pipeline step set-up the pipeline by writing hyperparameters and run info to pipeline output tables in S3
        and checking if data is already extracted by aws glue job in previous run and thus not need to be extracted again.
    Args:
        pipeline_metadata: a dict containing the following fields:
        - run_type: int representing the run type (ind, sim or rec)
        - version: Hash value created by sending the pipeline to the Kubeflow pipeline server
        - release: Release Number
        data_scope_dir: path of data_scope_param1/data_scope_param2/daterange
        config: dict from PipelineConfig object, holding the configuration for all the steps
    Named-only args:
        input_data: dictonary of which input_data should be overwritten from the default
        output_data: dictonary of which output_data should be overwritten from the default
        params: dictonary of which parameters should be overwritten from the default
        run_id: set a specific run_id for this run (named only)

    Returns: NamedTuple containing run_id, data_scope_param1, data_scope_param2, and configs for the next steps

    """
    config = config_from_dict(_config)
    common = config.setup_pipeline.common
    if "run_id" not in common:
        common["run_id"] = str(uuid.uuid4())
    if "kf_run_id" not in common:
        common["kf_run_id"] = "debug"

    # Update config with pipeline parameters
    update_config(config, input_data=input_data, output_data=output_data, params=params)
    timestamp = datetime.now().strftime("%Y_%m_%d-%H_%M")
    data_scope_param1, data_scope_param2, date_range = data_scope_dir.split("/")
    start_date, end_date = date_range.split("_")

    # blank output files for each execution
    result_files_loc = S3Path(f"s3://{config.setup_pipeline['bucket']}") / config.setup_pipeline["dir_pipeline_output"]
    copy_result_files(result_files_loc)

    # Write Hyperparameters & Run info to pipeline output tables in S3
    upload_runinfo_to_output_tables(
        timestamp,
        pipeline_metadata["run_type"],
        pipeline_metadata["version"],
        pipeline_metadata["release"],
        common["run_id"],
        common["kf_run_id"],
        config.setup_pipeline,
    )
    upload_hyperparam_to_output_tables(
        common["run_id"],
        data_scope_param1,
        start_date,
        end_date,
        data_scope_param2,
        config,
    )

    # serialize config to pass it to kubeflow
    cfg = config_to_dict(config)
    return SetupOutput(
        common["run_id"],
        data_scope_param1,
        data_scope_param2,
        cfg["preprocessing"],
        cfg["feature_engineering"],
        cfg["set_mining"],
        cfg["postprocessing"],
    )


# pass preprocessing function to kubeflow container operation
setup_pipeline_op = func_to_container_op(
    func=setup_pipeline,
    use_code_pickling=True,
    modules_to_capture=[
        "ml_pipeline.components.setup_pipeline.setup_pipeline",
        "ml_pipeline.components.setup_pipeline.steps",
        "ml_pipeline.util.util",
        "ml_pipeline.util.s3util",
        "data.result_files",
        "config.config",
        "config.util",
    ],
    base_image="python:3.8",
    packages_to_install=[
        "pandas",
        "boto3",
        "cloudpickle",
        "cloudpathlib",
    ],
)
