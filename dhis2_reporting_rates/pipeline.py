from openhexa.sdk import current_run, parameter, pipeline, workspace
from typing import List
import os
import polars as pl
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.periods import period_from_string
from itertools import product
from datetime import datetime, timedelta
import shutil


@pipeline("dhis2-reporting-rates", name="DHIS2 Reporting Rates")
@parameter(
    "datasets",
    name="Datasets",
    help="UIDs of datasets",
    type=str,
    multiple=True,
    required=True,
)
@parameter(
    "periods",
    name="Periods",
    help="DHIS2 periods",
    type=str,
    multiple=True,
    required=False,
)
@parameter(
    "start",
    name="Period (start)",
    help="Start of DHIS2 period range",
    type=str,
    required=False,
)
@parameter(
    "end",
    name="Period (end)",
    help="End of DHIS2 period range",
    type=str,
    required=False,
)
@parameter(
    "org_units",
    name="Organisation units",
    help="UIDs of organisation units",
    type=str,
    multiple=True,
    required=False,
)
@parameter(
    "org_unit_groups",
    name="Organisation unit groups",
    help="UIDs of organisation unit groups",
    type=str,
    multiple=True,
    required=False,
)
@parameter(
    "org_unit_levels",
    name="Organisation unit levels",
    help="Organisation unit levels",
    type=str,
    multiple=True,
    required=False,
)
@parameter(
    "output_dir",
    name="Output directory",
    help="Output directory where data extract will be saved",
    type=str,
    required=False,
)
@parameter(
    "data_format",
    name="Dataframe format",
    choices=["Wide", "Long"],
    default="Wide",
    type=str,
    required=True,
)
@parameter(
    "use_cache",
    name="Use cache",
    help="Use cache if possible (NB: data might be outdated)",
    type=bool,
    default=True,
    required=False,
)
def dhis2_reporting_rates(
    datasets: List[str],
    periods: List[str],
    start: str,
    end: str,
    org_units: List[str],
    org_unit_groups: List[str],
    org_unit_levels: List[str],
    output_dir: str,
    data_format: str,
    use_cache: bool,
):
    CONNECTION_ID = "cmr-snis"
    df = download(
        connection_id=CONNECTION_ID,
        datasets=datasets,
        periods=periods,
        start=start,
        end=end,
        org_units=org_units,
        org_unit_groups=org_unit_groups,
        org_unit_levels=org_unit_levels,
        use_cache=use_cache,
    )

    df = transform(
        connection_id=CONNECTION_ID, df=df, data_format=data_format, use_cache=use_cache
    )

    write(df=df, output_dir=output_dir)


@dhis2_reporting_rates.task
def download(
    connection_id: str,
    datasets: List[str],
    periods: List[str] = None,
    start: str = None,
    end: str = None,
    org_units: List[str] = None,
    org_unit_groups: List[str] = None,
    org_unit_levels: List[int] = None,
    use_cache: bool = True,
) -> pl.DataFrame:
    METRICS = [
        "REPORTING_RATE",
        "REPORTING_RATE_ON_TIME",
        "ACTUAL_REPORTS",
        "ACTUAL_REPORTS_ON_TIME",
        "EXPECTED_REPORTS",
    ]

    # if they exist, use start & end parameters to build a list of
    # dhis2 periods
    if start and end:
        p1 = period_from_string(start)
        p2 = period_from_string(end)
        periods = [str(pe) for pe in p1.get_range(p2)]
    if not periods:
        msg = "No temporal dimension provided"
        current_run.log_error(msg)
        raise ValueError(msg)

    # make sure org unit levels are numeric
    if org_unit_levels:
        org_unit_levels = [int(level) for level in org_unit_levels]

    # setup connection to dhis2 instance
    con = workspace.dhis2_connection(connection_id)
    if use_cache:
        cache_dir = os.path.join(workspace.files_path, ".cache")
    else:
        cache_dir = None
    dhis = DHIS2(con, cache_dir=cache_dir)
    current_run.log_info(f"Connected to {con.url}")

    # max elements per request
    dhis.analytics.MAX_DX = 50
    dhis.analytics.MAX_ORG_UNITS = 50
    dhis.analytics.MAX_PERIODS = 1

    data_values = dhis.analytics.get(
        data_elements=[f"{ds}.{metric}" for ds, metric in product(datasets, METRICS)],
        periods=periods,
        org_units=org_units,
        org_unit_groups=org_unit_groups,
        org_unit_levels=org_unit_levels,
        include_cocs=False,
    )
    current_run.log_info(f"Extracted {len(data_values)} data values")

    df = pl.DataFrame(
        data=data_values,
        schema={"dx": pl.Utf8, "ou": pl.Utf8, "pe": pl.Utf8, "value": pl.Utf8},
    )

    return df


@dhis2_reporting_rates.task
def transform(
    connection_id: str,
    df: pl.DataFrame,
    data_format: str = "Wide",
    use_cache: bool = True,
):
    METRICS = {
        "REPORTING_RATE": float,
        "REPORTING_RATE_ON_TIME": float,
        "ACTUAL_REPORTS": int,
        "ACTUAL_REPORTS_ON_TIME": int,
        "EXPECTED_REPORTS": int,
    }

    # connect to the dhis2 instance, we'll need it to join metadata
    # e.g. names instead of uid
    con = workspace.dhis2_connection(connection_id)
    if use_cache:
        cache_dir = os.path.join(workspace.files_path, ".cache")
    else:
        cache_dir = None
    dhis = DHIS2(con, cache_dir=cache_dir)

    # default dx values are in the format "<dataset_uid>.<reporting_rate_metric>"
    # e.g. gl7JRAdXkhR.REPORTING_RATE, gl7JRAdXkhR.ACTUAL_REPORTS, etc
    # we want to move dataset uid and metric into two new columns
    df = df.with_columns(
        [
            pl.col("dx").str.split(".").list.get(1).alias("metric"),
            pl.col("dx").str.split(".").list.get(0).alias("ds"),
        ]
    )

    # we don't need the dx column anymore
    df = df.select([column for column in df.columns if column != "dx"])

    if data_format == "Long":
        df = df.with_columns(pl.col("value").cast(float))

    elif data_format == "Wide":
        df = df.pivot(
            values=["value"],
            index=["ds", "ou", "pe"],
            columns=["metric"],
            aggregate_function="first",
        )

        df = df.with_columns(
            [
                pl.col(metric).cast(dtype)
                for metric, dtype in METRICS.items()
                if metric in df.columns
            ]
        )

    else:
        raise ValueError("Unrecognized dataframe format")

    # build a lookup index mapping dataset uids with dataset names
    datasets_meta = pl.DataFrame(dhis.meta.datasets())
    mapping = {}
    for uid, name in datasets_meta.select(["id", "name"]).unique().iter_rows():
        mapping[uid] = name

    # join metadata
    df = df.with_columns(pl.col("ds").map_dict(mapping).alias("ds_name"))
    df = dhis.meta.add_org_unit_name_column(df, "ou")
    df = dhis.meta.add_org_unit_parent_columns(df, "ou")

    # drop columns with only null values
    df = df[[s.name for s in df if not (s.null_count() == df.height)]]

    # reorder columns
    if data_format == "Wide":
        df = df.select(
            ["ds", "ds_name", "ou", "ou_name", "pe"]
            + [col for col in df.columns if col.startswith("parent_")]
            + [col for col in df.columns if col in METRICS]
        )

    # reorder columns
    elif data_format == "Long":
        df = df.select(
            ["ds", "ds_name", "ou", "ou_name", "pe"]
            + [col for col in df.columns if col.startswith("parent_")]
            + ["metric", "value"]
        )

    else:
        raise ValueError("Unrecognized dataframe format")

    current_run.log_info(
        f"Transformed data values into a dataframe of shape {df.shape}"
    )

    return df


@dhis2_reporting_rates.task
def write(df: pl.DataFrame, output_dir: str = None):
    # if no output dir have been provided by the user, use a default one
    # and remove previous default directories older than 1 month
    if output_dir:
        output_dir = os.path.join(workspace.files_path, output_dir)
        os.makedirs(output_dir, exist_ok=True)
    else:
        default_basedir = os.path.join(
            workspace.files_path,
            "pipelines",
            "dhis2-reporting-rates",
        )
        output_dir = os.path.join(
            default_basedir, datetime.now().strftime("%Y-%m-%d_%H:%M:%f")
        )
        os.makedirs(output_dir, exist_ok=True)
        clean_default_output_dir(default_basedir)

    fp = os.path.join(output_dir, "reporting_rates.csv")
    df.write_csv(fp)
    current_run.add_file_output(fp)
    current_run.log_info("Pipeline finished successfully")
    return


def clean_default_output_dir(output_dir: str):
    """Delete directories older than 1 month."""
    for d in os.listdir(output_dir):
        try:
            date = datetime.strptime(d, "%Y-%m-%d_%H:%M:%f")
        except ValueError:
            continue
        if datetime.now() - date > timedelta(days=31):
            shutil.rmtree(os.path.join(output_dir, d))


if __name__ == "__main__":
    dhis2_reporting_rates()
