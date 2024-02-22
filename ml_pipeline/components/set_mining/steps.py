from pathlib import Path
import pandas as pd
from typing import Tuple
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import fpgrowth
from typing import Dict
import collections
import boto3
import logging

from ml_pipeline.util.data_class import EventHistory, MetaData, SetMiningResults
from ml_pipeline.util.util import check_columns, load_data_s3, upload_data_s3
from ml_pipeline.util.util import timed


logger = logging.getLogger("set_mining")


@timed
def load_input_data_set_mining(approach: str, config: Dict) -> Tuple[EventHistory, MetaData]:
    """This function loads data from S3 that is necessary for this pipeline step.

    :param approach: Approach represented by string in order to get corresponding data
    :param config: Dictionary containing all configuration regarding e.g. data paths
    :return: Tuple of Data Classes
    """
    # Load Data From S3

    # Type conversion

    # Create Data Classes

    return dc_event_history, dc_event_id_meta


@timed
def upload_output_data_set_mining(run_id: str, dc_most_frequent_sets: SetMiningResults, config: Dict) -> bool:
    """This function collects kpis of all dataclasses and uploads all processed to S3 that is needed in
        the following steps.

    :param dc_most_frequent_sets: DataClass containing most frequent sets discovered by algorithm
    :param config: Dictionary containing all configuration regarding e.g. data paths
    :return: True, if upload was successfully
    """
    # Collect all KPIs
    df_kpis = pd.DataFrame(dict([(k, pd.Series(v)) for k, v in dc_most_frequent_sets.kpis.items()]))
    df_kpis["run_id"] = run_id

    # Upload Data for next pipeline step

    # Load current tables

    # Concat old data with new data

    # Upload Data

    return True


@timed
def apply_fpgrowth_set_mining(
    dc_event_history: EventHistory, unique_event_count: int, config: Dict
) -> SetMiningResults:
    """This function first transforms the data into a OneHotEncoding format, then executes the set mining algorithm
        and calculates for each 'most frequent sets' the amount of events (=length of pattern)

    Args:
        dc_event_history: DataClass containing the sets (=event sequences) that are mined by the algorithm
        unique_event_count: Amount of unique Events
        config: Dict of configurations

    Returns: DataClass containing the set mining results
    """
    # Get Data from DataClass and transform data type
    df_sequences = dc_event_history.sequences
    list_of_sequences = df_sequences["event_sequence"].tolist()

    # OneHotEncoding
    te = TransactionEncoder()
    te_ary = te.fit(list_of_sequences).transform(list_of_sequences)
    encoded_df = pd.DataFrame(te_ary, columns=te.columns_)

    # Set the min_support at .3 if we have more than 300 unique Events
    if unique_event_count > 300:
        config["params"]["min_support"] = 0.3
        print(
            f"Due to the fact that number of Events are {unique_event_count}, the min support value is changed to .3 to avoid high computational complexity"
        )

    # Apply Set Mining Algorithm
    frequent_itemsets = fpgrowth(
        encoded_df, min_support=config["params"]["min_support"], use_colnames=True
    ).sort_values(by=["support"], ascending=False)

    # Get the length of the most frequent sets
    frequent_itemsets["pattern_length"] = [len(itemset) for itemset in frequent_itemsets["itemsets"]]
    frequent_itemsets = frequent_itemsets.reset_index(drop=True)

    # Convert frozen sets into list and add info of current approach
    frequent_itemsets["itemsets"] = frequent_itemsets["itemsets"].apply(lambda x: list(x))

    # Update data from DataClass
    dc_most_frequent_sets = SetMiningResults(fpgrowth=frequent_itemsets.iloc[:100], kpis=collections.defaultdict(list))
    dc_most_frequent_sets.kpis["nb_freq_sets"].append(dc_most_frequent_sets.fpgrowth.shape[0])
    dc_most_frequent_sets.kpis["max_support_value"].append(round(dc_most_frequent_sets.fpgrowth.support.max(), 3))

    return dc_most_frequent_sets


@timed
def get_names_for_set(dc_most_frequent_sets: SetMiningResults, dc_meta_data: MetaData) -> SetMiningResults:
    """Each frequent set could be composed out of one to many events. To have a more user-friendly model output,
        this function extracts the event name for each event that is part of a frequent item-set.

    Args:
        dc_most_frequent_sets: DataClass containing most frequent item-sets discovered by the algorithm
        dc_meta_data: The german and english name for all events

    Returns: Frequent item sets enriched with the event names

    """
    # Get Data from DataClass
    df_freq_itemsets = dc_most_frequent_sets.fpgrowth
    df_event_names = dc_meta_data.data

    def get_event_name(set_events):
        ls_names = []
        for event in set_events:
            try:
                name = df_event_names.loc[df_event_names.event_id == event]["event_description_de"].values[0]
            except IndexError:
                name = None
            ls_names.append(name)
        return ls_names

    # get name of each event
    df_freq_itemsets["itemsets_desc"] = df_freq_itemsets["itemsets"].apply(get_event_name)

    # Update data from DataClass
    dc_most_frequent_sets.fpgrowth = df_freq_itemsets

    return dc_most_frequent_sets
