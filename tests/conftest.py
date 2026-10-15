import os
import sys

import pytest


@pytest.fixture(scope="session", autouse=True)
def bootstrap_delta_classpath(request):
    """Load Delta before any module creates Spark's process-wide JVM gateway.

    Stopping a SparkSession does not stop the gateway. Adding Maven packages
    in a later module's builder cannot update that gateway's classpath.
    Individual modules still own their sessions and session configuration.
    """
    if not any("spark" in item.fixturenames for item in request.session.items):
        return

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    builder = (
        SparkSession.builder
        .appName("MarketGuard-TestBootstrap")
        .master("local[1]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.stop()
