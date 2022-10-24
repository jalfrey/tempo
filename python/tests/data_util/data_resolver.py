import warnings
from collections import OrderedDict, namedtuple
from pathlib import Path

import jsonref

RESOURCES_PATH = Path(__file__).parent.parent.joinpath("unit_test_data")


def create_namedtuple_from_dict(obj):
    """Converts given list or dict to named tuples, generic alternative to dataclass"""
    if isinstance(obj, dict):
        fields = sorted(obj.keys())
        namedtuple_type = namedtuple(
            typename='TestData',
            field_names=fields,
            rename=True,
        )
        field_value_pairs = OrderedDict(
            (str(field), create_namedtuple_from_dict(obj[field]))
            for field in fields
        )
        try:
            return namedtuple_type(**field_value_pairs)
        except TypeError as e:
            raise Exception(
                "Cannot create namedtuple instance due to invalid attribute names."
            ) from e
    elif isinstance(obj, (list, set, tuple, frozenset)):
        return [create_namedtuple_from_dict(item) for item in obj]
    else:
        return obj


def inject_test_data(file: str) -> namedtuple:
    file = str(RESOURCES_PATH.joinpath(file))

    try:
        with open(file) as f:
            raw_dict = jsonref.load(f)
    except FileNotFoundError:
        warnings.warn(f"Could not load test data file {file}")
        raw_dict = {}

    return create_namedtuple_from_dict(raw_dict)
