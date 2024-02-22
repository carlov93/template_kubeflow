from typing import Dict
import logging
import pandas as pd
import collections
import boto3

from ml_pipeline.util.data_class import EventHistory
from ml_pipeline.util.util import check_columns, load_data_s3, upload_data_s3, timed

logger = logging.getLogger("set_mining")


@timed
def load_input_data_feature_engineering(config: Dict) -> EventHistory:
    """This function loads data from S3 that is necessary for this pipeline step.

    :param config: Dictionary containing all configuration regarding e.g. data paths
    :return: DataClass containing the events
    """
    # Load Data From S3

    # Type conversion

    # Create Data Classes

    return dc_data


@timed
def upload_output_data_feature_engineering(run_id: str, dc_event_history: EventHistory, config: Dict) -> bool:
    """This function collects kpis of all dataclasses and uploads all processed data to S3 that is needed in
        the following steps.

    :param run_id: ID that is unique within a kubeflow run and identifies a run for a specific data scope
                (=iteration of a for loop)
    :param dc_event_history: DataClass containing preprocessed events
    :param config: Dictionary containing all configuration regarding e.g. data paths
    :return: True, if upload was successfully
    """
    # Upload Data for next pipeline step

    # Load current tables

    # Concat old data with new data

    # Upload Data

    return True


@timed
def clustering(dc_events: EventHistory, config: Dict) -> EventHistory:
    """In this function a 'Rolling Window' procedure is conducted. All events are grouped into a cluster,
        which appear within a time span (e.g. 60 sec) or KM-span (e.g. 0.05).

    Args:
        :param dc_events: DataClass containing preprocessed events
        :param config: Dict with all configurations

    Returns: DataClass that contains the events with additional information to which cluster each event belongs

    """
    # Get data from DataClass
    df = dc_events.data
    df["snapshot_timestamp_calc"] = pd.to_datetime(df["snapshot_timestamp_calc"])
    df["message_timestamp"] = pd.to_datetime(df["message_timestamp"])

    # Bring the events in the correct time order and calculate the difference to previous row
    df_events = df.sort_values(by=["object_a", "snapshot_systemtime_seconds"], ascending=True).reset_index(drop=True)

    # Check if clustering will done based on time or km
    if config["params"]["clustering_approach"] == "time":
        col_for_clustering = "snapshot_timestamp_calc"
        df_events["diff_previous_event"] = df_events[col_for_clustering].diff().dt.seconds
    else:
        col_for_clustering = "snapshot_mileage_km"
        df_events["diff_previous_event"] = df_events[col_for_clustering].diff()

    # Intercept the case where the previous row is another object_a
    df_events["prev_object_a"] = df_events["object_a"].shift(1)
    df_events.loc[df_events["object_a"] != df_events["prev_object_a"], "flag_prev_object_a"] = 1
    df_events["diff_previous_event"] = df_events["diff_previous_event"].fillna(0)
    df_events.loc[df_events["flag_prev_object_a"] == 1, "diff_previous_event"] = 0.0

    ls_cumsum = []
    ls_cluster_no = []
    for object_a in df_events["object_a"].unique():
        # select a object_a
        df_event_history = df_events.loc[df_events.object_a == object_a]

        # set everything to zero
        cumsum = 0
        lfd_cluster = 0

        # iterate over events
        for _, row in df_event_history.iterrows():
            if 0 <= cumsum + row["diff_previous_event"] <= int(config["params"]["window_length"]):
                cumsum += row["diff_previous_event"]
                ls_cluster_no.append(lfd_cluster)
            else:
                lfd_cluster += 1
                cumsum = 0
                ls_cluster_no.append(lfd_cluster)
            ls_cumsum.append(cumsum)

    # attach the lists to the data frame
    df_events["cumsum"] = ls_cumsum
    df_events["cluster"] = ls_cluster_no

    # Select subset of columns
    df_events = df_events[
        ["object_a", "readout_id", "event_id", "snapshot_timestamp_calc", "snapshot_mileage_km", "cluster"]
    ]

    # Update data from DataClass
    dc_events.data = df_events

    return dc_events


@timed
def create_list_of_sequences(dc_data: EventHistory, config: Dict) -> EventHistory:
    """This functions transforms the DataFrame into a list of lists of events based on the cluster information.

    Args:
        :param dc_data: DataClass containing events with cluster information
        :param config: Dictionary containing the configuration

    Returns: DataClass containing a Dataframe where each row represents a cluster
    """
    # Get data from DataClass
    df = dc_data.data

    # group events of the same cluster into a list
    df_clusters = df.groupby(["object_a", "cluster"], group_keys=False)["id"].apply(list).reset_index(name="sequence")

    df_clusters_info = df.groupby(["object_a", "cluster"], group_keys=False)["id"].size().reset_index(name="nb_items")

    # Merge data
    df_clusters_merged = df_clusters.merge(df_clusters_info, how="left", on=["object_a", "cluster"])

    # Filter out sequences/clusters with only one element
    if not config["params"]["process_seq_containing_only_one_event"]:
        sequences = df_clusters_merged.copy()
        sequences = sequences.loc[df_clusters_merged.nb_items > 1]
    else:
        sequences = df_clusters_merged

    # Type Conversion
    sequences["sequence"] = [",".join(map(str, l)) for l in sequences["sequence"]]

    # Update data from DataClass
    dc_data.sequences = sequences
    dc_data.kpis["nr_sequences"].append(len(sequences))
    dc_data.kpis["mean_nr_elements_in_sequences"].append(round(sequences.nb_items.mean(), 2))

    return dc_data
