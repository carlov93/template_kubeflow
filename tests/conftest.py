from cloudpathlib import S3Path
from config.types import DirPipeline, S3Info
import pytest
import pandas as pd
import os
from pathlib import Path
import boto3
import collections
from typing import NamedTuple, List, Any


from config.config import (
    SetupPipelineConfig,
    FeatureEngineeringConfig,
    PreprocessingConfig,
    SetMiningConfig,
    account,
)
from ml_pipeline.components.exit_handler.steps import gather_results
from ml_pipeline.components.setup_extraction.setup_extraction import ExtractionOutput
from ml_pipeline.components.setup_pipeline.setup_pipeline import SetupOutput, setup_pipeline
from ml_pipeline.components.preprocessing.preprocessing import PreprocessingOutput, preprocessing
from ml_pipeline.components.feature_engineering.feature_engineering import feature_engineering
from ml_pipeline.components.set_mining.set_mining import set_mining
from config.util import config_to_dict, load_config
from ml_pipeline.util.data_class import (
    EventHistory,
    KnownPattern,
    SetMiningResults,
    MetaData,
)


# Fixtures for Preprocessing Unit tests
@pytest.fixture(scope="module")
def known_event_patterns_raw():
    """This testdata contains known patterns that are already split in its parts (event_pattern, and_not_events, ...)"""
    filepath = os.path.join(Path(__file__).parent, "test_data/pipeline_input/static/test_known_patterns.csv")
    df_known_patterns = pd.read_csv(filepath, sep=",")
    df_known_patterns["event_pattern"] = df_known_patterns.event_pattern.astype(str)
    df_known_patterns["trigger"] = df_known_patterns.trigger.astype(str)
    df_known_patterns = df_known_patterns.fillna("")
    known_patterns = KnownPattern(data=df_known_patterns, kpis=collections.defaultdict(list))
    return known_patterns


@pytest.fixture
def event_history_after_trigger_test():
    """Fixture to provide a EventHistory instance for testing."""
    sample_data = {
        "snapshot_timestamp_calc": [
            "2022-01-07T21:30:50.720Z",
            "2022-01-07T21:32:10.720Z",
        ],
        "message_timestamp_trigger": [
            "2022-01-07T21:30:10.720Z",
            "2022-01-07T21:30:10.720Z",
        ],
    }
    return EventHistory(
        data=pd.DataFrame(sample_data),
        kpis={"filtered_events_after_trigger": []},
        occurrence_each_event=None,
        sequences=None,
    )


@pytest.fixture(scope="module")
def run_pipeline():
    run_id = "integration_test"
    # metadata
    pipeline_metadata = {
        "run_type": 0,
        "version": "test",
        "release": "test",
    }

    bucket = account.data_bucket_name
    data_scope_dir_prefix = ""
    pipeline_in = ""
    base_tmp = ""
    base_tmp_out = ""
    pipeline_out = ""

    # CLEAN UP & SETUP s3
    s3 = boto3.resource("s3")

    # delete possible tmp files not properly deleted
    bucket_s3 = s3.Bucket(account.data_bucket_name)
    [file.delete() for file in bucket_s3.objects.filter(Prefix=base_tmp_out)]
    [file.delete() for file in bucket_s3.objects.filter(Prefix=base_tmp)]

    # MAIN LOOP
    for data_scope_dir in setup_extraction_step.data_scopes:
        dir_pipeline = DirPipeline(
            pipeline_in,
            f"{base_tmp}{data_scope_dir}/",
            f"{base_tmp_out}{data_scope_dir}/",
        )
        config = load_config(
            dir_pipeline,
            data_scope_dir_prefix + str(data_scope_dir),
            common={"run_id": run_id},
        )

        setup_pipeline_step: SetupOutput = setup_pipeline(
            pipeline_metadata,
            data_scope_dir,
            config_to_dict(config),
            input_data=new_input_data,
            output_data=new_output_data,
            params=params,
        )
        run_id = setup_pipeline_step.run_id
        trigger = setup_pipeline_step.trigger_id
        config_preprocessing = setup_pipeline_step.config_preprocessing
        config_feature_eng = setup_pipeline_step.config_feature_engineering
        config_set_mining = setup_pipeline_step.config_set_mining

        # ACTION
        preprocessing_step: PreprocessingOutput = preprocessing(run_id, trigger, config_preprocessing)
        approaches = config.feature_engineering["params"]["approaches"]
        feature_engineering(run_id, approaches, config_feature_eng)
        set_mining(run_id, approaches, 300, config_set_mining)

    gather_results_step = gather_results(bucket, base_tmp, base_tmp_out, pipeline_out)
    return NamedTuple("RunPipeline", [("s3", Any), ("bucket", str), ("dir_pipeline_output", str)])(
        s3, bucket, pipeline_out
    )
