import logging
import boto3

from kfp.components import func_to_container_op
from ml_pipeline.util.util import is_debug_mode
from ml_pipeline.components.exit_handler.steps import gather_results

logger = logging.getLogger("set_mining")


def exit_handler(
    workflow_status: str,
    kf_run_id: str,
    bucket: str,
    base_tmp: str,
    base_tmp_out: str,
    pipeline_out: str,
    clear_folders: list,
) -> bool:
    """This pipeline step specifies exit task which will run as a last pipeline step, even if one of the earlier
        pipeline steps failed. This is analogous to using a try: block followed by a finally: block in normal Python,
        where the exit pipeline step is in the finally: block.

    :param workflow_status:
    :param kf_run_id: Global ID of a specific Kubeflow Run
    :param bucket: S3 bucket that is used for storing data
    :param base_tmp: S3 path where all temp pipeline data are stored
    :param base_tmp_out: S3 path where result files of each Kubeflow for-loop are stored
    :param pipeline_out: S3 path where concatenated result files are stored
    :param clear_folders: List of folders within base_tmp that should be deleted
    :return:
    """

    # Collect all results from the different parallel for streams
    gather_results(bucket, base_tmp, base_tmp_out, pipeline_out)

    # Delete tmp data of pipeline step
    s3 = boto3.resource("s3")
    if not is_debug_mode():
        bucket_s3 = s3.Bucket(bucket)
        for path in clear_folders:
            [file.delete() for file in bucket_s3.objects.filter(Prefix=path)]

    # Log error message in case of a not successfully run
    if workflow_status not in ["Succeeded"]:
        logger.error(
            f"An error occured somewhere in the pipeline. Please check the logs in error_logs.csv and search for"
            f" the kf_run_id {kf_run_id}."
        )
    else:
        logger.info("Pipeline successfully ended.")

    return True


# pass function to kubeflow container operation
exit_handler_op = func_to_container_op(
    func=exit_handler,
    use_code_pickling=True,
    modules_to_capture=[
        "ml_pipeline.components.exit_handler.exit_handler",
        "ml_pipeline.components.exit_handler.steps",
        "ml_pipeline.util.util",
        "ml_pipeline.util.s3util",
        "ml_pipeline.util.data_class",
    ],
    base_image="python:3.8",
    packages_to_install=[
        "pandas==1.5.3",
        "boto3",
        "cloudpickle",
        "cloudpathlib",
    ],
)
