import json

_danaleo_json = None

def load_danaleo_json():
    global _danaleo_json
    if _danaleo_json is None:
        with open("danaleo.json", "r") as f:
            _danaleo_json = json.load(f)
    return _danaleo_json