import logging
from typing import List

from kfp.components import func_to_container_op
from ml_pipeline.components.set_mining.steps import (
    load_input_data_set_mining,
    upload_output_data_set_mining,
    apply_fpgrowth_set_mining,
    get_names_for_set,
)
from ml_pipeline.util.util import timed, pipeline_logging_config
from ml_pipeline.util.exceptions import NoDataToProcess

logger = logging.getLogger("set_mining")


@timed
@pipeline_logging_config
def set_mining(run_id: str, unique_event_count: int, config: dict) -> bool:
    """This function is a pipeline step and acts as a wrapper for set mining functions.

    Args:
        run_id: ID that is unique within a kubeflow run and identifies a run for a specific data scope
                (=iteration of a for loop)
        unique_event_count: Number of unique Events in the data set
        config: Dictionary containing all configurations
    Returns: True, if pipeline step was executed successfully

    """
    logger.info("Start pipeline step: Set Mining")

    # Load Input Data Pipeline Step
    dc_event_history, dc_event_id_meta = load_input_data_set_mining(config)

    if dc_event_history.sequences.shape[0] == 0:
        raise NoDataToProcess(
            config["common"]["kf_run_id"],
            run_id,
            "set_mining",
            config["bucket"],
            config["error_logs"],
            "No sequences are available for set mining",
        )

    # get most frequent sets out of all sequences by applying the fbgrowth set mining algorithm
    dc_most_frequent_sets = apply_fpgrowth_set_mining(dc_event_history, unique_event_count, config)

    # get event names for all events that are part of the 'most frequent itemsets'
    dc_most_frequent_sets = get_names_for_set(dc_most_frequent_sets, dc_event_id_meta)

    # Store Output Pipeline Step
    upload_output_data_set_mining(run_id, dc_most_frequent_sets, config)

    return True


# pass set mining function to kubeflow container operation
set_mining_op = func_to_container_op(
    func=set_mining,
    use_code_pickling=True,
    modules_to_capture=[
        "ml_pipeline.components.set_mining.set_mining",
        "ml_pipeline.components.set_mining.steps",
        "ml_pipeline.util.util",
        "ml_pipeline.util.data_class",
        "ml_pipeline.util.exceptions",
    ],
    base_image="python:3.8",
    packages_to_install=["pandas", "boto3", "cloudpickle", "mlxtend"],
)
