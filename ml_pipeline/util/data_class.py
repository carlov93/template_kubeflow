from dataclasses import dataclass
import pandas as pd
from typing import List, Union


@dataclass
class EventHistory:
    data: Union[pd.DataFrame, None]
    occurrence_each_event: Union[pd.DataFrame, None]
    sequences: Union[pd.DataFrame, None]
    kpis: dict


@dataclass
class KnownPattern:
    data: pd.DataFrame
    kpis: dict


@dataclass
class SetMiningResults:
    fpgrowth: Union[pd.DataFrame, None]
    kpis: dict


@dataclass
class MetaData:
    data: pd.DataFrame
