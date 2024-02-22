from typing import Any, Dict, Optional
from config.config import (
    PipelineConfigTuple,
    SetupPipelineConfig,
    PreprocessingConfig,
    FeatureEngineeringConfig,
    SetMiningConfig,
)
from config.types import Common, DirPipeline, S3Info


def load_config(
    dir_pipeline: DirPipeline,
    data_scope_dir: str,
    common: Optional[Common] = None,
    bucket: str = "",
):
    if not common:
        common = {}
    s3_info: S3Info = {
        "bucket": bucket,
        "dir_pipeline_input": dir_pipeline.dir_pipeline_input,
        "dir_pipeline_tmp": dir_pipeline.dir_pipeline_tmp,
        "dir_pipeline_output": dir_pipeline.dir_pipeline_output,
    }

    # Load Config Pipeline Step
    config_setup_pipeline = SetupPipelineConfig(s3_info, common)
    config_preprocessing = PreprocessingConfig(s3_info, common)
    config_preprocessing.input_data.update({"data_scope_dir": data_scope_dir})
    config_feature_engineering = FeatureEngineeringConfig(s3_info, common)
    config_set_mining = SetMiningConfig(s3_info, common)

    return PipelineConfigTuple(
        config_setup_pipeline,
        config_preprocessing,
        config_feature_engineering,
        config_set_mining,
    )


def update_config(config: PipelineConfigTuple, *, input_data: dict = {}, output_data: dict = {}, params: dict = {}):
    for name, step_dicts in (("input_data", input_data), ("output_data", output_data), ("params", params)):
        for step, new_values in step_dicts.items():
            cfg = getattr(config, step)  # get specific step object. For example, config.set_mining
            cfg_dct = getattr(cfg, name)  # get dict to update. For example, set_mining.output_data
            keys = set(new_values.keys())
            # make sure to only update keys if they are preexisting
            assert all(
                k in cfg_dct for k in keys
            ), f"{step}.{name} does not accept the following keys: {keys - set(cfg_dct)}"
            cfg_dct.update(new_values)

