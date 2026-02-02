import dagster as dg

from dagster_gcp_pandas import BigQueryPandasIOManager


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "io_manager": BigQueryPandasIOManager(
                project="delays-486122",
                dataset="delays",
                timeout=15.0
            )
        }
    )
