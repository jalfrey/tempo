import os
import re
import warnings
from pathlib import Path
from typing import Union

import delta.pip_utils
import jsonref
import pytest
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from tempo import TSDF
from tempo.intervals import IntervalsDF

from chispa import assert_df_equality


@pytest.fixture(scope="session", autouse=True)
def set_timezone() -> None:
    os.environ["TZ"] = "UTC"


@pytest.fixture(scope="session", autouse=True)
def get_spark():
    builder = (
        SparkSession.builder.appName("unit-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.driver.extraJavaOptions",
            "-Dio.netty.tryReflectionSetAccessible=true",
        )
        .config(
            "spark.executor.extraJavaOptions",
            "-Dio.netty.tryReflectionSetAccessible=true",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .master("local")
    )

    spark: SparkSession = delta.pip_utils.configure_spark_with_delta_pip(builder).getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", "1")
    spark.sparkContext.setLogLevel("ERROR")
    # filter out ResourceWarning messages
    warnings.filterwarnings("ignore", category=ResourceWarning)

    return spark


# @pytest.fixture(scope="module")
# def get_test_data() -> dict:
#     test_data = __load_test_data()
#     return test_data


@pytest.fixture(scope="module")
def get_module_name(request) -> str:
    module_name = str(request.module).split(" ")[1].split(".")[-1]
    return module_name


@pytest.fixture(scope="class", autouse=True)
def get_class_name(request) -> str:
    class_name = str(request.cls)
    return class_name


# @pytest.fixture(scope="class")
# def __load_test_data() -> dict:
#     """
#     This function reads our unit test data config json and returns the required metadata to create the correct
#     format of test data (Spark DataFrames, Pandas DataFrames and Tempo TSDFs)
#     :param test_case_path: string representation of the data path e.g. : "tsdf_tests.BasicTests.test_describe"
#     :type test_case_path: str
#     """
#     file_name = get_module_name()
#     class_name = get_class_name()
#     func_name = get_function_name()
#
#     # find our test data file
#     test_data_file = __get_test_data_file_path(file_name)
#     if not os.path.isfile(test_data_file):
#         warnings.warn(f"Could not load test data file {test_data_file}")
#         return {}
#
#     # proces the data file
#     with open(test_data_file, "r") as f:
#         data_metadata_from_json = jsonref.load(f)
#         # warn if data not present
#         if class_name not in data_metadata_from_json:
#             warnings.warn(f"Could not load test data for {file_name}.{class_name}")
#             return {}
#         if func_name not in data_metadata_from_json[class_name]:
#             warnings.warn(
#                 f"Could not load test data for {file_name}.{class_name}.{func_name}"
#             )
#             return {}
#         return data_metadata_from_json[class_name][func_name]


@pytest.fixture()
def get_function_name(request) -> str:
    function_name = str(request.function).split(" ")[1]
    return function_name


def buildTestDF(schema, data, ts_cols=None):
    """
    Constructs a Spark Dataframe from the given components
    :param schema: the schema to use for the Dataframe
    :param data: values to use for the Dataframe
    :param ts_cols: list of column names to be converted to Timestamp values
    :return: a Spark Dataframe, constructed from the given schema and values
    """
    if ts_cols is None:
        ts_cols = ["events_ts"]

    # build dataframe
    df = spark.createDataFrame(data, schema)

    # check if ts_col follows standard timestamp format, then check if timestamp has micro/nanoseconds
    for tsc in ts_cols:
        ts_value = str(df.select(ts_cols).limit(1).collect()[0][0])
        ts_pattern = r"^\d{4}-\d{2}-\d{2}| \d{2}:\d{2}:\d{2}\.\d*$"
        decimal_pattern = r"[.]\d+"
        if re.match(ts_pattern, str(ts_value)) is not None:
            if (
                    re.search(decimal_pattern, ts_value) is None
                    or len(re.search(decimal_pattern, ts_value)[0]) <= 4
            ):
                df = df.withColumn(tsc, f.to_timestamp(f.col(tsc)))
    return df


@pytest.fixture()
def get_data_as_tsdf(name: str = "input", convert_ts_col: bool = True):
    df = get_data_as_sdf(name, convert_ts_col)
    td = get_test_data[name]
    tsdf = TSDF(
        df,
        ts_col=td["ts_col"],
        partition_cols=td.get("partition_cols", None),
        sequence_col=td.get("sequence_col", None),
    )
    return tsdf


@pytest.fixture()
def get_data_as_idf(name: str, convert_ts_col: bool = True):
    df = get_data_as_sdf(name, convert_ts_col)
    td = get_test_data[name]
    idf = IntervalsDF(
        df,
        start_ts=td["start_ts"],
        end_ts=td["end_ts"],
        series_ids=td.get("series", None),
    )
    return idf


#
# Assertion Functions
#

@pytest.fixture()
def assertFieldsEqual(self, fieldA, fieldB):
    """
    Test that two fields are equivalent
    """
    self.assertEqual(
        fieldA.name.lower(),
        fieldB.name.lower(),
        msg=f"Field {fieldA} has different name from {fieldB}",
    )
    self.assertEqual(
        fieldA.dataType,
        fieldB.dataType,
        msg=f"Field {fieldA} has different type from {fieldB}",
    )
    # self.assertEqual(fieldA.nullable, fieldB.nullable)


@pytest.fixture()
def assertSchemaContainsField(self, schema, field):
    """
    Test that the given schema contains the given field
    """
    # the schema must contain a field with the right name
    lc_fieldNames = [fc.lower() for fc in schema.fieldNames()]
    self.assertTrue(field.name.lower() in lc_fieldNames)
    # the attributes of the fields must be equal
    self.assertFieldsEqual(field, schema[field.name])


@pytest.fixture()
def assertDataFrameEquality(
        df1: Union[IntervalsDF, TSDF, DataFrame],
        df2: Union[IntervalsDF, TSDF, DataFrame],
        from_tsdf: bool = False,
        from_idf: bool = False,
        ignore_row_order: bool = False,
        ignore_column_order: bool = True,
        ignore_nullable: bool = True,
):
    """
    Test that the two given Dataframes are equivalent.
    That is, they have equivalent schemas, and both contain the same values
    """

    if from_tsdf or from_idf:
        df1 = df1.df
        df2 = df2.df

    assert_df_equality(
        df1,
        df2,
        ignore_row_order=ignore_row_order,
        ignore_column_order=ignore_column_order,
        ignore_nullable=ignore_nullable,
    )
