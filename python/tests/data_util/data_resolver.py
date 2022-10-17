import inspect
import json
import warnings
from pathlib import Path

RESOURCES_PATH = Path(__file__).parent.parent.joinpath("unit_test_data")


def inject_test_data(test_data_file):
    """
        Read the content of the json file and convert it to named tuple,
        can be used for injecting test data set to tests, helps in separating test data from the tests
    """

    file = str(RESOURCES_PATH.joinpath(test_data_file))

    try:
        with open(file) as f:
            raw_dict = json.load(f)
    except FileNotFoundError:
        warnings.warn(f"Could not load test data file {file}")
        raw_dict = {}

    return raw_dict


test = inject_test_data("intervals_tests.json")
print(test["IntervalsDFTests"]["test_fromStackedMetrics_series_str"])


def current_func_name():
    print(inspect.currentframe().f_code.co_name)


current_func_name()
