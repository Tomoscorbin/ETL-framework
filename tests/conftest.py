import logging

import pytest
from pyspark.sql import SparkSession

# Names of fixture that require Spark to be available
_SPARK_FIXTURE_NAME = "spark_fixture"


def quiet_py4j() -> None:
    """Turn down Spark logging during the test context."""
    logging.getLogger("py4j").setLevel(logging.WARN)


@pytest.fixture(scope="session")
def spark_fixture():
    quiet_py4j()

    spark = (
        SparkSession.Builder()
        .appName("Integration Test PySpark")
        # Small amounts of data in tests so no need for more than 1 CPU
        # More CPUs would actually require more overhead in this instance.
        .master("local[1]")
        # fail faster if there's an issue with initial [local] conections
        .config("spark.network.timeout", "10000")
        .config("spark.executor.heartbeatInterval", "1000")
        # Locally, the driver shares the memory with the executors.
        # So best to constrain it somewhat!
        .config("spark.driver.memory", "2g")
        # Default partitions is 200 which is good for _big data_. The shuffle
        # overhead for our small, test data sets will be more expensive than the
        # computation.
        .config("spark.sql.shuffle.partitions", "1")
        # No need for any UI components, or keeping history
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.dagGraph.retainedRootRDDs", "1")
        .config("spark.ui.retainedJobs", "1")
        .config("spark.ui.retainedStages", "1")
        .config("spark.ui.retainedTasks", "1")
        .config("spark.sql.ui.retainedExecutions", "1")
        .config("spark.worker.ui.retainedExecutors", "1")
        .config("spark.worker.ui.retainedDrivers", "1")
        #
        .config("spark.default.parallelism", "1")
        # Disable compression (pointless on small datasets)
        .config("spark.rdd.compress", "false")
        .config("spark.shuffle.compress", "false")
        #
        .config("spark.dynamicAllocation.enabled", "false")
        # Control the executor resources
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .getOrCreate()
    )

    yield spark

    spark.stop()


def _mark_tests_using_spark_fixture(tests: list[pytest.Function]) -> None:
    """
    Adds the `requires_spark` marker to tests that are using the fixture that require a
    Spark instance.

    :param tests: list of tests collected by `pytest`
    """
    for test in tests:
        # If the test uses our Spark fixture, add a marker!
        if _SPARK_FIXTURE_NAME in test.fixturenames:
            test.add_marker(pytest.mark.requires_spark)


def _skip_spark_tests(test: pytest.Function) -> None:
    """
    Tell `pytest` to skip tests that require a SparkSession.

    If the config argument `--include-spark-tests` is present, this could shouldn't be
    invoked.

    :param test: test collected by `pytest`
    """

    requires_spark_markers = list(test.iter_markers(name="requires_spark"))

    if requires_spark_markers:
        pytest.skip("Skipped tests that require a SparkSession")


def pytest_addoption(parser: pytest.Parser):
    parser.addoption(
        "--include-spark-tests",
        action="store_true",
        default=False,
        help="Run Spark tests or nah?",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]):
    if not config.getoption("--include-spark-tests"):
        _mark_tests_using_spark_fixture(tests=items)


def pytest_runtest_setup(item: pytest.Item):
    if not item.config.getoption("--include-spark-tests"):
        _skip_spark_tests(test=item)
