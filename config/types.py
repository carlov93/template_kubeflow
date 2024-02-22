from typing import TypedDict, NamedTuple, List

S3Info = TypedDict(
    "S3Info",
    {
        "bucket": str,
        "dir_pipeline_input": str,
        "dir_pipeline_tmp": str,
        "dir_pipeline_output": str,
    },
)

Common = TypedDict(
    "Common",
    {
        "kf_run_id": str,
        "run_id": str,
        "debug": bool,
    },
    total=False,
)

DirPipeline = NamedTuple(
    "DirPipeline", [("dir_pipeline_input", str), ("dir_pipeline_tmp", str), ("dir_pipeline_output", str)]
)


### PIPELINE STEPS PARAMETERS
# SETUP_PIPELINE STEP
setup_pipeline_input = TypedDict(
    "setup_pipeline_input",
    {
        "location_1": str,
        "location_2": str,
        "location_3": str,
    },
)
setup_pipeline_output = TypedDict("setup_pipeline_output", {})
setup_pipeline_params = TypedDict("setup_pipeline_params", {})

# PREPROCESSING STEP
preprocessing_input = TypedDict()
preprocessing_output = TypedDict()
preprocessing_params = TypedDict()

# FEATURE_ENGINEERING STEP
feature_engineering_input = TypedDict()
feature_engineering_output = TypedDict()
feature_engineering_params = TypedDict()


# SET_MINING STEP
set_mining_input = TypedDict()
set_mining_output = TypedDict()
set_mining_params = TypedDict()


# ANALYSIS_ENV_COND STEP
analysis_env_cond_input = TypedDict()
analysis_env_cond_output = TypedDict()
analysis_env_cond_params = TypedDict()


# POSTPROCESSING STEP
postprocessing_input = TypedDict()
postprocessing_output = TypedDict()
postprocessing_params = TypedDict()
