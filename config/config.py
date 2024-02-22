from __future__ import annotations
from os import getenv
from dataclasses import dataclass, field, asdict
from typing import Dict
from abc import ABC

from config.types import *


GLUE_OP_VERSION: str = "aws-glue-submit-glue-job-1.1.9"
pipeline_release: str = "1.0.0"


class Config(ABC):
    def _get_config(self: Config) -> Config:
        return self

    def get_config_as_dict(self: Config) -> Dict[str, str]:
        return asdict(self._get_config())


@dataclass
class GlueDefaultConfig(Config):
    glue_role: str = ""
    glue_version: str = "3.0"
    worker_type: str = "G.1X"
    max_concurrent_runs: int = 1
    python_version: str = "3"
    glue_job_type: str = "glueetl"
    number_of_workers: int = 50
    max_retries: int = 0
    timeout: int = 120
    region_name: str = "eu-west-1"
    script_path: str = f""
    job_arguments: Dict[str, str] = field(
        default_factory=lambda: (
            {
                "--RUN_TIMESTAMP": "default",
            }
        )
    )
    tags: Dict[str, str] = field(
        default_factory=lambda: (
            {
                "usecase": "",
                "APP-ID": "",
            }
        )
    )


@dataclass
class ExtractionConfig:
    prefix_already_extracted: str = ("")
    extraction_subfolder: str = "standard/"
    extraction_json_location: str = ""


@dataclass
class StepConfig:
    s3info: S3Info
    common: Common = field(default_factory=lambda: {})

    def __post_init__(self):
        self.bucket = self.s3info["bucket"]
        self.dir_pipeline_input = self.s3info["dir_pipeline_input"]
        self.dir_pipeline_tmp = self.s3info["dir_pipeline_tmp"]
        self.dir_pipeline_output = self.s3info["dir_pipeline_output"]
        self.error_logs = self.s3info["dir_pipeline_output"] + "error_logs.csv"

    def __getitem__(self, k):
        return getattr(self, k)


@dataclass
class SetupPipelineConfig(StepConfig):
    name = "setup_pipeline"
    input_data: setup_pipeline_input = field(
        default_factory=lambda: {
            "location_1": "",
            "location_2": "",
            "location_3": "",
        }
    )

    output_data: setup_pipeline_output = field(
        default_factory=lambda: {
            "location_1": "",
            "location_2": "",
        }
    )
    params: setup_pipeline_params = field(default_factory=lambda: {})


@dataclass
class PreprocessingConfig(StepConfig):
    name = "preprocessing"
    input_data: preprocessing_input = field(
        default_factory=lambda: {
        }
    )

    output_data: preprocessing_output = field(
        default_factory=lambda: {
        }
    )
    params: preprocessing_params = field(
        default_factory=lambda: {
        }
    )


@dataclass
class FeatureEngineeringConfig(StepConfig):
    name = "feature_engineering"
    input_data: feature_engineering_input = field(
        default_factory=lambda: {
        }
    )

    output_data: feature_engineering_output = field(
        default_factory=lambda: {
        }
    )
    params: feature_engineering_params = field(
        default_factory=lambda: {
        }
    )


@dataclass
class SetMiningConfig(StepConfig):
    name = "set_mining"
    input_data: set_mining_input = field(
        default_factory=lambda: {
        }
    )

    output_data: set_mining_output = field(
        default_factory=lambda: {
        }
    )
    params: set_mining_params = field(default_factory=lambda: {"min_support": 0.3})


PipelineConfigTuple = NamedTuple(
    "PipelineConfigTuple",
    [
        ("setup_pipeline", SetupPipelineConfig),
        ("preprocessing", PreprocessingConfig),
        ("feature_engineering", FeatureEngineeringConfig),
        ("set_mining", SetMiningConfig),
    ],
)
