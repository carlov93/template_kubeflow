import sys
import logging
import pandas as pd
import boto3
from os import environ

from ml_pipeline.util.util import is_debug_mode, upload_data_s3, load_data_s3, check_columns

logger = logging.getLogger("set_mining")


class NoDataToProcess(Exception):
    """Exception raised if based on the given data scope no event is in place.

    Attributes:
        message -- explanation of the error
    """

    def __init__(
        self,
        kf_run_id: str,
        run_id: str,
        pipeline_step: str,
        bucket: str,
        path: str,
        message: str,
    ):
        if is_debug_mode():
            # return the exception instead of ending the program
            self.pipeline_step = pipeline_step
            self.message = message
            return

        # load current table
        df_error_logs = load_data_s3(boto3.resource("s3"), bucket, path)

        # Store the error message with the run id to temp S3 file
        data = {
            "kf_run_id": [kf_run_id],
            "run_id": [run_id],
            "pipeline_step": [pipeline_step],
            "error_msg": [message],
        }

        # Concat df_run_info with new run_info
        new_df = pd.DataFrame(data=data)
        check_columns(df_error_logs, new_df)
        df_error_logs = pd.concat([df_error_logs, new_df], ignore_index=True)

        # upload updated table
        upload_data_s3(df_error_logs, bucket, path)

        # Log the error to kubeflow console and exit the python programm
        logger.error(message)
        sys.exit()
