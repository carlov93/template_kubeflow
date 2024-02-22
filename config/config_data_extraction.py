from config.config import GlueDefaultConfig, account
from dataclasses import dataclass


@dataclass
class EventHistoryExtraction(GlueDefaultConfig):

    glue_job_name: str = account.product_name + "-" + account.profile + "-" + ""
    description: str = "Extraction"
    script_path: str = f""

    def __post_init__(self):
        self.job_arguments.update(
            {
                "--enable-glue-datacatalog": "",
                "--db_name": "",
                "--table_name_1": "",
                "--table_name_2": '',
                "--bucket": "",
                "--destination_path_1": "",
                "--destination_path_2": "",
            }
        )
