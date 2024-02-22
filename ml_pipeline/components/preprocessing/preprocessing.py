import logging
from typing import NamedTuple

from kfp.components import func_to_container_op
from ml_pipeline.components.preprocessing.steps import (
    load_input_data_preprocessing,
    convert_dtyps_input_data,
    upload_output_data_preprocessing,
    preprocessing_step_1,
)
from ml_pipeline.util.util import timed, pipeline_logging_config
from ml_pipeline.util.exceptions import NoDataToProcess

logger = logging.getLogger("set_mining")

PreprocessingOutput = NamedTuple(
    "PreprocessingOutput",
    [
        ("nb_unique_events_after_prepro", int),
        ("share_value_threshold", float),
    ],
)


@timed
@pipeline_logging_config
def preprocessing(run_id: str, abc: str, config: dict) -> PreprocessingOutput:
    """This function is a pipeline step and acts as a wrapper for preprocessing functions like noise reduction.

    Args:
        run_id: ID that is unique within a kubeflow run and identifies a run for a specific data scope
                (=iteration of a for loop)
        abc: Data scope parameter
        config: Dictionary containing all configurations for this pipeline step

    Returns: True, if pipeline step was executed successfully

    """
    # Start Pipeline
    logger.info("Start pipeline step: Preprocessing")

    # Load Input Data Pipeline Step
    dc_data_1, dc_data_2, dc_data_3, dc_data_3 = load_input_data_preprocessing(abc, config)

    if dc_data_1.data.shape[0] == 0:
        raise NoDataToProcess(
            config["common"]["kf_run_id"],
            run_id,
            "preprocessing",
            config["bucket"],
            config["error_logs"],
            "No event is available for the given data scope in preprocessing step",
        )

    # Type Conversion of incoming data
    dc_data_1, dc_data_2, dc_data_3 = convert_dtyps_input_data(dc_data_1, dc_data_2, dc_data_3)

    # Remove Duplicates due to data quality issues
    dc_data_1 = preprocessing_step_1(dc_data_1)

    # Store Output Pipeline Step
    upload_output_data_preprocessing(run_id, dc_data_1, dc_data_2, dc_data_3, config)

    return PreprocessingOutput(dc_data_1.kpis["nb_unique_events_after_prepro"][0], share_value_threshold)


# pass preprocessing function to kubeflow container operation
preprocessing_op = func_to_container_op(
    func=preprocessing,
    use_code_pickling=True,
    modules_to_capture=[
        "ml_pipeline.components.preprocessing.preprocessing",
        "ml_pipeline.components.preprocessing.steps",
        "ml_pipeline.util.util",
        "ml_pipeline.util.data_class",
        "ml_pipeline.util.exceptions",
    ],
    base_image="python:3.8",
    packages_to_install=[
        "pandas",
        "boto3",
        "cloudpickle",
    ],
)
