from abc import ABC, abstractmethod
import pandas as pd
from .constants import UserErrorMessages

class EdaModule(ABC):

    def __init__(self, params, data):
        if data is None and not isinstance(data, (pd.DataFrame, pd.Series)):
            raise ValueError(UserErrorMessages.DATA_NOT_PD_DATAFRAME.value)
        
        if params is None and not isinstance(params, dict):
            raise ValueError(UserErrorMessages.PARAMS_NOT_DICT.value)
        self._params = params
        self._data = data

        self._validate_params()
    
    @abstractmethod
    def analyze(self):
        pass

    @abstractmethod
    def act(self, actions):
        pass

    def _validate_params(self):
        """Validate that all columns are present in data."""
        for column in self._params.get("columns", {}).keys():
            if column not in getattr(self._data, "columns", []):
                raise ValueError(UserErrorMessages.PARAMS_DATA_COLUMN_MISMATCH.value.format(column=column))

__all__ = ["EdaModule"]