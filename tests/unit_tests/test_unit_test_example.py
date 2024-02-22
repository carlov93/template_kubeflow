from typing import Dict
import numpy as np

from ml_pipeline.components.preprocessing.steps import (
    convert_dtyps_input_data,
    drop_duplicates,
)
from ml_pipeline.util.data_class import KnownPattern, EventHistory


def test_class_based_filter(event_history_class_filter_test):
    """Verify that the resulting EventHistory object doesn't contain classes 'B' and 'D'
    and verify that the KPIs are updated correctly"""
    # Given
    classes_to_be_filtered = "B, D"

    # Act
    filtered_event_history = class_based_filter(event_history_class_filter_test, classes_to_be_filtered)

    # Assert
    assert set(filtered_event_history.data["class_short"].unique()) == {"A", "C", "E"}
    assert filtered_event_history.kpis["nb_events_post_class_filter"] == [3]
