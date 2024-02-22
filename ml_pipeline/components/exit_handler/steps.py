import pandas as pd

from ml_pipeline.util.s3util import copy_result_files, read_s3_csv
from ml_pipeline.util.util import is_debug_mode, timed, pipeline_logging_config, upload_data_s3

from cloudpathlib import S3Path


@timed
@pipeline_logging_config
def gather_results(
    bucket: str,
    base_tmp: str,
    base_tmp_out: str,
    pipeline_out: str,
) -> bool:
    """This pipeline step checks if data is already extracted by aws glue job in previous run and thus
    not need to be extracted again.
    Args:
        bucket: aws bucket
        base_tmp: folder where temporary operations were performed
        base_tmp_out: folder where the temporary outputs were created
        pipeline_out: place where the pipeline output should go (not the temporary output)

    Returns: a flag indicating if the data extraction step should be done, and a dict containing the details
    """
    # Check if data is already extracted and stored in S3
    result_files = S3Path(f"s3://{bucket}") / pipeline_out
    tmp_results = S3Path(f"s3://{bucket}") / base_tmp_out
    tmp_folder = S3Path(f"s3://{bucket}") / base_tmp

    if not result_files.exists():
        copy_result_files(result_files)

    data_scopes = []
    for prefix_model in tmp_results.iterdir():
        for prefix in prefix_model.iterdir():
            for prefix_period in prefix.iterdir():
                data_scopes.append(prefix_period)

    for file in result_files.iterdir():
        if not file.is_file():
            continue

        df = read_s3_csv(file)
        oldlen = len(df)
        others = [read_s3_csv(data_scope / file.name) for data_scope in data_scopes]
        new_df = pd.concat([df] + others)
        print(f"adding {len(new_df) - oldlen} results to {file.name}")

        upload_data_s3(new_df, file.bucket, file.key)

    # delete temporary workspaces
    if not is_debug_mode():
        tmp_results.rmtree()
        tmp_folder.rmtree()

    return True
