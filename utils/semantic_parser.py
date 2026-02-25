from .json_utils import load_danaleo_json
import re

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
    
    return max(type_scores, key=type_scores.get)

def _parse_numeric_value(value, sem_type, semantic_types=_danaleo_config["eda"]["numeric_semantic_types"]):
    value = str(value).lower().strip()
    config = semantic_types[sem_type]
    
    for pattern in config["patterns"]:
        match = re.search(pattern, value)
        if match:
            groups = match.groupdict()
            
            # Handle ranges
            if "value_min" in groups and "value_max" in groups:
                num = (float(groups["value_min"]) + float(groups["value_max"])) / 2
            else:
                num = float(groups.get("value") or 0)
            
            # Apply unit conversion
            for unit, factor in config.get("unit_map", {}).items():
                if unit in value:
                    num *= factor
                    break
            return num
    return None

def parse_column(data_column, sample_values):
    sem_type = _detect_numeric_semantic_type(sample_values)
    print(sem_type)
    
    parsed_values = data_column.apply(lambda x: _parse_numeric_value(x, sem_type))
    return parsed_values

__all__ = ["parse_column"]