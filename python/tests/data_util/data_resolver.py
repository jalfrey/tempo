import json
import uuid
from collections import OrderedDict, namedtuple
from pathlib import Path

RESOURCES_PATH = Path(__file__).parent.parent.joinpath("unit_test_data")

print(RESOURCES_PATH)


def inject_test_data(file):
    """
        Read the content of the json file and convert it to named tuple,
        can be used for injecting test data set to tests, helps in separating test data from the tests
    """

    file = str(RESOURCES_PATH.joinpath(file))
    with open(file) as f:
        raw_dict = json.load(f)
    return raw_dict


test = inject_test_data("intervals_tests.json")
print(test["__SharedData"]["init"])

