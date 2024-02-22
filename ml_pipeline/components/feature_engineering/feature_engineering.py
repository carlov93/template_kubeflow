import logging
from typing import List

from kfp.components import func_to_container_op
from ml_pipeline.components.feature_engineering.steps import (
    clustering,
    create_list_of_sequences,
    load_input_data_feature_engineering,
    upload_output_data_feature_engineering,
)
from ml_pipeline.util.util import timed, pipeline_logging_config
from ml_pipeline.util.exceptions import NoDataToProcess

logger = logging.getLogger("set_mining")


@timed
@pipeline_logging_config
def feature_engineering(run_id: str, config: dict) -> bool:
    """This function is a pipeline step and acts as a wrapper for feature engineering functions.

    Args:
        run_id: ID that is unique within a kubeflow run and identifies a run for a specific data scope
                (=iteration of a for loop)
        config: Dictionary containing all configurations

    Returns: True, if pipeline step was executed successfully

    """
    # Start Pipeline
    logger.info("Start pipeline step: Feature Engineering")

    # Load Input Data Pipeline Step
    dc_events_hist = load_input_data_feature_engineering(config)

    if dc_events_hist.data.shape[0] == 0:
        raise NoDataToProcess(
            config["common"]["kf_run_id"],
            run_id,
            "feature_engineering",
            config["bucket"],
            config["error_logs"],
            "No sequences of events are available for feature_engineering step",
        )

    # Assign each event to a sequence
    dc_events_hist = clustering(dc_events_hist, config)

    # Create lists of events for each sequence (=time window)
    dc_events_hist = create_list_of_sequences(dc_events_hist, config)

    # Store Output Pipeline Step
    upload_output_data_feature_engineering(run_id, dc_events_hist, config)

    return True


# pass feature engineering function to kubeflow container operation
feature_engineering_op = func_to_container_op(
    func=feature_engineering,
    use_code_pickling=True,
    modules_to_capture=[
        "ml_pipeline.components.feature_engineering.feature_engineering",
        "ml_pipeline.components.feature_engineering.steps",
        "ml_pipeline.util.util",
        "ml_pipeline.util.s3util",
        "ml_pipeline.util.data_class",
        "ml_pipeline.util.exceptions",
    ],
    base_image="python:3.8",
    packages_to_install=[
        "pandas==1.5.3",
        "boto3",
        "cloudpickle",
    ],
)
