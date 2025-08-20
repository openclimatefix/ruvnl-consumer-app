"""
Main consumer app entrypoint

Before running this app, set the following environment variables:

DB_URL: postgres DB connection string (required)
DATA_URL: of generation JSON data (optional)

"""

import datetime as dt
import logging
import os
import sys
import time
from zoneinfo import ZoneInfo

import click
import pandas as pd
import requests
import sentry_sdk
from pvsite_datamodel import DatabaseConnection, LocationSQL
from pvsite_datamodel.read import get_sites_by_country
from pvsite_datamodel.write import insert_generation_values
from sqlalchemy.orm import Session

from ruvnl_consumer_app import __version__

log = logging.getLogger(__name__)

sentry_sdk.init(
    dsn=os.getenv("SENTRY_DSN"), environment=os.getenv("ENVIRONMENT", "local"), traces_sample_rate=1
)
sentry_sdk.set_tag("app_name", "india_ruvnl_consumer")
sentry_sdk.set_tag("version", __version__)

DEFAULT_DATA_URL = "http://sldc.rajasthan.gov.in/rrvpnl/read-sftp?type=overview"


def get_sites(db_session: Session) -> list[LocationSQL]:
    """
    Gets 1 site for each asset type (pv and wind)

    Args:
            db_session: A SQLAlchemy session

    Returns:
            A list of LocationSQL objects
    """

    all_sites = get_sites_by_country(db_session, country="india")

    # This naively selects the 1st wind and 1st pv site in the array
    valid_sites = []
    for asset_type in ["pv", "wind"]:

        sites = [
            s for s in all_sites if (s.asset_type.name == asset_type) and (s.region == "ruvnl")
        ]
        if len(sites) == 0:
            log.warning(f"Could not find site for asset type: {asset_type}")
            assert len(sites) > 0, f"No sites found for ruvnl region for asset type: {asset_type}"

        valid_sites.append(sites[0])

    return valid_sites


def fetch_data(data_url: str, retry_interval: int = 30) -> pd.DataFrame:
    """
    Fetches the latest state-wide generation data for Rajasthan

    Args:
            data_url: The URL to query data from
            retry_interval: the amount of seconds to sleep between retying the api again.

    Returns:
            A pandas DataFrame of generation values for wind and PV

    Raises:
            RuntimeError: If max retries are reached without any response
    """
    print("Starting to get data")
    retries = 0
    max_retries = 5
    while retries < max_retries:
        try:
            r = requests.get(data_url, timeout=10)  # 10 second
            # Got a response (even if not 200), so break the retry loop
            break
        except requests.exceptions.Timeout as err:
            log.error("Timed out")
            retries += 1
            if retries == max_retries:
                error_msg = f"Failed to fetch data after {max_retries} attempts from {data_url}"
                log.error(error_msg)
                raise RuntimeError(error_msg) from err
            log.info(f"Retrying again in {retry_interval} seconds (retry count: {retries})")
            time.sleep(retry_interval)

    # Handle non-200 status codes by returning empty DataFrame
    if r.status_code != 200:
        log.warning(f"Failed to fetch data from {data_url}. Status code: {r.status_code}")
        return pd.DataFrame(columns=["asset_type", "start_utc", "power_kw"])

    raw_data = r.json()
    asset_map = {"WIND GEN": "wind", "SOLAR GEN": "pv"}
    data = []
    for k, v in asset_map.items():
        record = next((d["0"] for d in raw_data["data"] if d["0"]["scada_name"] == k), None)
        if record is not None:

            start_utc = dt.datetime.fromtimestamp(int(record["SourceTimeSec"]), tz=dt.UTC)
            power_kw = record["Average2"] * 1000  # source is in MW, convert to kW
            if power_kw < 0:
                log.warning(f"Ignoring negative power value: {power_kw} kW for asset type: {v}")
                continue
            if v == "wind":
                if start_utc < dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=1):
                    start_ist = start_utc.astimezone(ZoneInfo("Asia/Kolkata"))
                    start_ist = str(start_ist)
                    now = dt.datetime.now(ZoneInfo("Asia/Kolkata"))
                    now = str(now)
                    timestamp_after_raise = f"Timestamp Now: {now} Timestamp data: {start_ist}"
                    timestamp_fstring = f"{timestamp_after_raise}"
                    log.warning("Start time is at least 1 hour old. " + timestamp_fstring)

            data.append({"asset_type": v, "start_utc": start_utc, "power_kw": power_kw})
            log.info(
                f"Found generation data for asset type: {v}, " f"{power_kw} kW at {start_utc} UTC"
            )
        else:
            log.warning(f"No generation data for asset type: {v}")

    return pd.DataFrame(data)


def merge_generation_data_with_sites(data: pd.DataFrame, sites: list[LocationSQL]) -> pd.DataFrame:
    """
    Augments the input dataframe with corresponding site_uuid

    Args:
            data: A dataframe of generation data
            sites: a list of LocationSQL objects

    Returns:
            An augmented dataframe with the associated site uuids
    """

    # Associate correct site_uuid with each generation asset type
    sites_map = {s.asset_type.name: s.location_uuid for s in sites}
    data["site_uuid"] = data["asset_type"].apply(lambda d: sites_map[d] if d in sites_map else None)

    # Remove generation data for which we have no associated site
    data = data[data["site_uuid"].notnull()]

    return data


def save_generation_data(
    db_session: Session, generation_data: pd.DataFrame, write_to_db: bool
) -> None:
    """
    Saves generation data to DB (or prints to stdout)

    Args:
            db_session: A SQLAlchemy session
            generation_data: a pandas Dataframe of generation values for PV and wind
            write_to_db: If true, generation values are written to db, otherwise to stdout
    """

    for asset_type in ["pv", "wind"]:
        asset_data = generation_data[generation_data["asset_type"] == asset_type]

        if asset_data.empty:
            log.warning(f"No generation data for asset type: {asset_type}")
            continue

        # check if generation exceeds capacity and update if necessary
        if write_to_db:
            site_uuid = asset_data["site_uuid"].iloc[0]
            max_power = float(asset_data["power_kw"].max())

            site = (
                db_session.query(LocationSQL).filter(LocationSQL.location_uuid == site_uuid).first()
            )
            if site and max_power > site.capacity_kw:
                log.info(
                    f"Updating capacity for site {site_uuid} from {site.capacity_kw}kW "
                    f"to {max_power}kW"
                )
                site.capacity_kw = max_power
                db_session.commit()

        # Drop asset_type column
        asset_data = asset_data.drop("asset_type", axis=1)

        if write_to_db:
            insert_generation_values(db_session, asset_data)
            db_session.commit()
        else:
            log.info(f"Generation data: {asset_type}:\n{asset_data.to_string()}")


@click.command()
@click.option(
    "--write-to-db",
    is_flag=True,
    default=False,
    help="Set this flag to actually write the results to the database.",
)
@click.option(
    "--log-level",
    default="info",
    help="Set the python logging log level",
    show_default=True,
)
@click.option(
    "--retry-interval",
    default=30,
    help="Set the sleep time (seoncds) between retries for fetching data.",
    show_default=True,
)
def app(write_to_db: bool, log_level: str, retry_interval: int) -> None:
    """
    Main function for running data consumer
    """
    logging.basicConfig(stream=sys.stdout, level=getattr(logging, log_level.upper()))

    log.info(f"Running data consumer app (version: {__version__})")

    url = os.getenv("DB_URL", "sqlite:///test.db")
    data_url = os.getenv("DATA_URL", DEFAULT_DATA_URL)

    # 0. Initialise DB connection
    db_conn = DatabaseConnection(url, echo=False)

    with db_conn.get_session() as session:
        # 1. Get sites
        log.info("Getting sites...")
        sites = get_sites(session)
        log.info(f"Found {len(sites)} sites")

        # 2. Fetch latest generation data
        log.info(f"Fetching generation data from {data_url}...")
        data = fetch_data(data_url, retry_interval)

        # 3. Assign site to generation data
        data = merge_generation_data_with_sites(data, sites)

        # 3. Write generation data to DB or stdout
        if data.empty:
            log.warning("No generation data to write")
        else:
            log.info("Writing generation data...")
            save_generation_data(session, data, write_to_db)

        log.info("Done!")


if __name__ == "__main__":
    app()
