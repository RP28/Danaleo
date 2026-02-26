from .json_utils import load_danaleo_json
import re
import pandas as pd

_danaleo_config = load_danaleo_json()

def _detect_numeric_semantic_type(sample_values, semantic_types=_danaleo_config["eda"]["numeric_semantic_types"]):
    type_scores = {stype: 0 for stype in semantic_types}
    
    for val in sample_values:
        val = str(val).lower().strip()
        for stype, config in semantic_types.items():
            for pattern in config["patterns"]:
                if re.search(pattern, val):
                    type_scores[stype] += 1
                    break
    
    # if nothing matched return None
    max_score = max(type_scores.values()) if type_scores else 0
    if max_score == 0:
        return None
    return max(type_scores, key=type_scores.get)

def _parse_numeric_value(series: pd.Series, sem_type: str):
    """Convert a Series of strings using semantic configuration.

    This avoids Python loops by using pandas string methods and vectorized
    arithmetic.  The configuration may contain multiple patterns; we try each
    in order and fill results into the output.  Units are applied by matching
    substrings in descending length to prevent short keys ("m") shadowing
    longer ones ("mile").
    """

    config = _danaleo_config["eda"]["numeric_semantic_types"][sem_type]
    result = pd.Series(index=series.index, dtype="float64")

    for pattern in config["patterns"]:
        matches = series.str.extract(pattern, expand=True)
        if matches.empty:
            continue

        if "value_min" in matches.columns and "value_max" in matches.columns:
            num = (matches["value_min"].astype(float) + matches["value_max"].astype(float)) / 2
        else:
            num = matches.get("value", pd.Series(0, index=matches.index)).astype(float)

        # apply unit conversion
        for unit in sorted(config.get("unit_map", {}).keys(), key=len, reverse=True):
            factor = config["unit_map"][unit]
            mask = series.str.contains(re.escape(unit))
            num = num.where(~mask, num * factor)

        result = result.combine_first(num)

    return result


def parse_numeric_column(data_column, sample_values):
    """Detect semantic type then convert entire column vectorized.
    """
    sem_type = _detect_numeric_semantic_type(sample_values)

    result = None
    if sem_type is not None:
        result = _parse_numeric_value(data_column.astype(str), sem_type)
    numeric_coerce = pd.to_numeric(data_column, errors="coerce")
    if result is None:
        return numeric_coerce

    return result.fillna(numeric_coerce)

__all__ = ["parse_numeric_column"]