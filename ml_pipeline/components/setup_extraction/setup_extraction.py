from typing import NamedTuple
from kfp.components import func_to_container_op
from ml_pipeline.util.util import (
    get_cumulative_weeks,
    timed,
    pipeline_logging_config,
    upload_data_s3,
    get_last_week,
)

from cloudpathlib import S3Path

ExtractionOutput = NamedTuple("ExtractionOutput", [("should_extract_data", bool), ("data_scopes", list)])


@timed
@pipeline_logging_config
def setup_extraction(
    data_scope_param1: str,
    data_scope_param2: str,
    bucket: str,
    extraction_location: str,
    extraction_json_location: str,
    start_date: str,
    end_date: str,
    run_type: int,
) -> ExtractionOutput:
    """This pipeline step checks if data is already extracted by aws glue job in previous run and thus not
    need to be extracted again.
    Args:
        data_scope_param1: comma separated string of data_scope_param1
        data_scope_param2: comma separated string of data_scope_param2
        bucket: aws bucket
        extraction_location: location to check if data was already extracted (will extract here if not already extracted)
        extraction_json_location: where in s3 to save the should_extract_dict, used by extraction step
        start_date: start date of the extraction (at nearest monday)
        end_date: end_date of the extraction (at nearest monday)
        run_type: int indicating the type of run. 0: individual, 1: simulation, 2: recurring

    Returns: a flag indicating if the data extraction step should be done, and a dict containing the details
    """
    # Check if data is already extracted and stored in S3
    # true if should extract, false if already extracted
    should_extract_dict = {}
    basepath = S3Path(f"s3://{bucket}") / extraction_location

    # parameters for iteration
    data_scope_param1_list = [c.strip() for c in data_scope_param1.split(",")]
    data_scope_param2_list = [m.strip() for m in data_scope_param2.split(",")]

    dates = get_cumulative_weeks(start_date, end_date)
    init_date = dates[0].strftime("%Y-%m-%d")
    if run_type == 0:  # individual run
        end_dates = [dates[-1]]
    elif run_type == 1:  # simulation run
        end_dates = dates[1:]
    elif run_type == 2:  # recurring run
        init_date, end_dates = get_last_week()
        end_dates = [end_dates]
    else:
        raise NotImplementedError

    for end in end_dates:
        finish_date = end.strftime("%Y-%m-%d")
        for data_scope_param1 in data_scope_param1_list:
            for data_scope_param2 in data_scope_param2_list:
                data_scope_dir = f"{data_scope_param1}/{data_scope_param2}/{init_date}_{finish_date}"
                should_extract_dict[data_scope_dir] = not (basepath / data_scope_dir).exists()

    # flag to check if glue script should be called
    should_extract_data = any(should_extract_dict.values())

    # upload dict to s3 for glue script, only if should_extract_data
    if should_extract_data:
        upload_data_s3(should_extract_dict, bucket, extraction_json_location)

    return ExtractionOutput(
        should_extract_data,
        list(should_extract_dict.keys()),
    )


# pass preprocessing function to kubeflow container operation
setup_extraction_op = func_to_container_op(
    func=setup_extraction,
    use_code_pickling=True,
    modules_to_capture=[
        "ml_pipeline.components.setup_extraction.setup_extraction",
        "ml_pipeline.util.util",
    ],
    base_image="python:3.8",
    packages_to_install=[
        "pandas",
        "boto3",
        "cloudpickle",
        "cloudpathlib",
    ],
)
