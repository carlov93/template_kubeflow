import logging
import io

from kfp.dsl import Condition, ExitHandler, ParallelFor
from helpers.helpers import hyphenated_lowercase, set_max_cache_staleness
from kfp.dsl import pipeline

from ml_pipeline.components.setup_extraction.setup_extraction import setup_extraction_op
from ml_pipeline.components.setup_pipeline.setup_pipeline import setup_pipeline_op
from ml_pipeline.components.preprocessing.preprocessing import preprocessing_op
from ml_pipeline.components.feature_engineering.feature_engineering import feature_engineering_op
from ml_pipeline.components.set_mining.set_mining import set_mining_op
from ml_pipeline.components.exit_handler.exit_handler import exit_handler_op
from config.config import GLUE_OP_VERSION, ExtractionConfig, GlueDefaultConfig, account
from config.util import load_config, config_to_dict
from config.config_data_extraction import EventHistoryExtraction
from config.config import pipeline_release
from config.types import DirPipeline


logger = logging.getLogger("set_mining")
# create console handler, set level and add to logger
ch = logging.StreamHandler()
logger.addHandler(ch)
log_stringio_obj = io.StringIO()
# create file handler, set level and add to logger
s3_handler = logging.StreamHandler(log_stringio_obj)
logger.addHandler(s3_handler)

# Pipeline Configuration
pipeline_title = "Template Pipeline"
pipeline_name = f"{account.profile}-{hyphenated_lowercase(pipeline_title)}"


@pipeline(name=pipeline_title)
def ml_pipeline(
    data_scope_param1: str,
    data_scope_param2: str,
    data_scope_param3: str,
    data_scope_param4: str,
    data_scope_param5: str = "5",
    data_scope_param6: str = "30",
    data_scope_param7: str = "789",
    data_scope_param8: float = 80,
    data_scope_param9: int = 1,
) -> bool:
    """
    Standardize pipeline for all run types
    run_type:
    - 0: Individual Run
    - 1: Simultion Run
    - 2: Recurring Run
    """

    ## Global parameters
    pipeline_metadata = {
        "run_type": data_scope_param9,
        "version": "1.0.0",
        "release": pipeline_release,
    }
    bucket = account.data_bucket_name
    new_params = {
        "preprocessing": {
            "data_scope_param5": data_scope_param5,
            "data_scope_param7": data_scope_param7,
            "data_scope_param8": data_scope_param8,
        },
        "feature_engineering": {"data_scope_param6": data_scope_param6},
        "postprocessing": {
            "data_scope_param7": data_scope_param7,
        },
    }

    # Data Extraction
    extract_config = ExtractionConfig()

    # Define S3 directories that are used during pipeline run
    base_tmp = ""
    base_tmp_out = ""
    pipeline_in = ""
    pipeline_out = ""

    with ExitHandler(
        exit_handler_op(
            workflow_status="{{workflow.status}}",
            kf_run_id="{{workflow.uid}}",
            bucket=bucket,
            base_tmp=base_tmp,
            base_tmp_out=base_tmp_out,
            pipeline_out=pipeline_out,
            clear_folders=[base_tmp, base_tmp_out],
        )
    ):
        setup_extraction_step = setup_extraction_op(
            data_scope_param2,
            data_scope_param1,
            bucket,
            extract_config.prefix_already_extracted + extract_config.extraction_subfolder,
            extract_config.extraction_json_location,
            data_scope_param3,
            data_scope_param4,
            data_scope_param9,
        )

        should_extract_data = setup_extraction_step.outputs["should_extract_data"]
        with Condition(should_extract_data == True, "Check-Extraction"):
            glue_script = f"{GlueDefaultConfig.script_path}sql_query.py"
            glue_config = EventHistoryExtraction(script_path=glue_script)
            glue_job_arguments = {
                "--data_scope_dir_prefix": extract_config.extraction_subfolder,
                "--should_extract_dict_location": extract_config.extraction_json_location,
            }
            glue_config.job_arguments.update(glue_job_arguments)

            # Load Glue Job Component
            components = GithubStore()
            glue_op = components.load_component_from_url(
                f"https://www.website.com/{GLUE_OP_VERSION}" f"/components/aws/glue/submit_glue_job/component.yaml"
            )
            data_extraction_step = glue_op(**glue_config.get_config_as_dict()).after(setup_extraction_step)
            data_extraction_step.set_display_name("Data Extraction by Glue Job")
            set_max_cache_staleness(data_extraction_step)
            logging.info("Data Extraction Step added to the pipeline")

        # MAIN LOOP
        with ParallelFor(loop_args=setup_extraction_step.outputs["data_scopes"]) as data_scope_dir:
            # each step writes to its own tmp folder and output folder
            dir_pipeline = DirPipeline(
                pipeline_in,
                f"{base_tmp}{data_scope_dir}/",
                f"{base_tmp_out}{data_scope_dir}/",
            )

            config = load_config(
                dir_pipeline,
                extract_config.extraction_subfolder + str(data_scope_dir),
                common={
                    "kf_run_id": "{{workflow.uid}}",
                    "debug": False,
                },
            )

            # Setup Pipeline
            setup_pipeline_step = setup_pipeline_op(
                pipeline_metadata,
                data_scope_dir,
                config_to_dict(config),
                params=new_params,
            )
            run_id = setup_pipeline_step.outputs["run_id"]
            param_2 = setup_pipeline_step.outputs["param_2"]
            config_preprocessing = setup_pipeline_step.outputs["config_preprocessing"]
            config_feature_eng = setup_pipeline_step.outputs["config_feature_engineering"]
            config_set_mining = setup_pipeline_step.outputs["config_set_mining"]
            setup_pipeline_step.set_display_name("Setup Pipeline")
            set_max_cache_staleness(setup_pipeline_step)
            logging.info("Setup Pipeline Step added to the pipeline")

            # Data Preprocessing
            preprocessing_step = preprocessing_op(run_id, param_2, config_preprocessing).after(data_extraction_step)
            param_3 = preprocessing_step.outputs["param_3"]
            preprocessing_step.set_display_name("Preprocessing")
            set_max_cache_staleness(preprocessing_step)
            logging.info("Data Preprocessing Step added to the pipeline")

            # Feature Engineering
            feature_engineering_step = feature_engineering_op(run_id, config_feature_eng).after(preprocessing_step)
            feature_engineering_step.set_display_name("Feature Engineering")
            set_max_cache_staleness(feature_engineering_step)
            logging.info("Feature Engineering Step added to the pipeline")

            # Set Mining
            set_mining_step = set_mining_op(run_id, param_3, config_set_mining).after(feature_engineering_step)
            set_mining_step.set_display_name("Set Mining")
            set_max_cache_staleness(set_mining_step)
            logging.info("Set Mining Step added to the pipeline")

        return True


if __name__ == "__main__":
    client = Client(account)
    submit_to_kubeflow(
        client=client,
        pipeline=ml_pipeline,
        pipeline_name=pipeline_name,
        experiment_name=pipeline_name,
    )
