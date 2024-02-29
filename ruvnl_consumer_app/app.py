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

import click
import pandas as pd
import requests
from pvsite_datamodel import DatabaseConnection, GenerationSQL, SiteSQL
from pvsite_datamodel.read import get_sites_by_country
from pvsite_datamodel.write import insert_generation_values
from sqlalchemy.orm import Session

from ruvnl_consumer_app import __version__

log = logging.getLogger(__name__)

DEFAULT_DATA_URL = "http://sldc.rajasthan.gov.in/rrvpnl/read-sftp?type=overview"


def get_sites(db_session: Session) -> list[SiteSQL]:
    """
    Gets 1 site for each asset type (pv and wind)

    Args:
            db_session: A SQLAlchemy session

    Returns:
            A list of SiteSQL objects
    """

    sites = get_sites_by_country(db_session, country="india")

    # This naively selects the 1st wind and 1st pv site in the array
    valid_sites = []
    for asset_type in ["pv", "wind"]:

        site = next((s for s in sites if s.asset_type.name == asset_type), None)
        if site is not None:
            valid_sites.append(site)
        else:
            log.warning(f"Could not find site for asset type: {asset_type}")

    return valid_sites


def fetch_data(data_url: str) -> pd.DataFrame:
    """
    Fetches the latest state-wide generation data for Rajasthan

    Args:
            data_url: The URL ot query data from

    Returns:
            A pandas DataFrame of generation values for wind and PV
    """
    try:
        r = requests.get(data_url, timeout=10)  # 10 seconds
    except requests.exceptions.Timeout as e:
        log.error("Timed out")
        raise e

    # Raise error if response is 4XX or 5XX
    r.raise_for_status()

    raw_data = r.json()
    asset_map = {"WIND GEN": "wind", "SOLAR GEN": "pv"}
    data = []
    for k, v in asset_map.items():
        record = next((d["0"] for d in raw_data["data"] if d["0"]["scada_name"] == k), None)
        if record is not None:

            start_utc = dt.datetime.fromtimestamp(int(record["SourceTimeSec"]), tz=dt.UTC)
            power_kw = record["Average2"] * 1000  # source is in MW, convert to kW

            data.append({"asset_type": v, "start_utc": start_utc, "power_kw": power_kw})
            log.info(
                f"Found generation data for asset type: {v}, " f"{power_kw} kW at {start_utc} UTC"
            )
        else:
            log.warning(f"No generation data for asset type: {v}. Filling with Nans")
            data.append({
                "asset_type": v,
                "start_utc": dt.datetime.now(tz=dt.UTC),
                "power_kw": None
            })

    return pd.DataFrame(data)


def merge_generation_data_with_sites(
        db_session: Session,
        data: pd.DataFrame,
        sites: list[SiteSQL]
) -> pd.DataFrame:
    """
    Augments the input dataframe with corresponding site_uuid

    Args:
            db_session: A SQLAlchemy session
            data: A dataframe of generation data
            sites: a list of SiteSQL objects

    Returns:
            An augmented dataframe with the associated site uuids
    """

    # Associate correct site_uuid with each generation asset type
    sites_map = {s.asset_type.name: s.site_uuid for s in sites}
    data["site_uuid"] = data["asset_type"].apply(lambda d: sites_map[d] if d in sites_map else None)

    # Remove generation data for which we have no associated site
    data = data[data["site_uuid"].notnull()]

    # Get last generated timestamp for each site
    last_gen = (db_session.query(GenerationSQL.start_utc).distinct(GenerationSQL.site_uuid)
                .filter(GenerationSQL.site_uuid.in_([s.site_uuid for s in sites]))
                .order_by(GenerationSQL.site_uuid, GenerationSQL.start_utc.desc())
                .all())

    # TODO Fix the following code:
    # using the last generated timestamps above, check the data["start_utc"] is not
    # equal to last_gen. If it is, set data["start_utc"] to now() and data["power_kw"] to None

    data.sort_values(by="site_uuid", ascending=True, inplace=True)
    log.info(data["start_utc"])
    log.info(last_gen)
    mask = data["start_utc"] == last_gen
    log.info(mask)
    data.loc[mask, "start_utc"] = dt.datetime.now(dt.UTC)
    data.loc[mask, "power_kw"] = None

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

        # Drop asset_type column
        asset_data = asset_data.drop("asset_type", axis=1)

        if asset_data.empty:
            log.warning(f"No generation data for asset type: {asset_type}")
            continue

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
def app(write_to_db: bool, log_level: str) -> None:
    """
    Main function for running data consumer
    """
    logging.basicConfig(stream=sys.stdout, level=getattr(logging, log_level.upper()))

    log.info(f'Running data consumer app (version: {__version__})')

    url = os.environ["DB_URL"]
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
        data = fetch_data(data_url)

        # 3. Assign site to generation data
        data = merge_generation_data_with_sites(session, data, sites)

        # 3. Write generation data to DB or stdout
        if data.empty:
            log.warning("No generation data to write")
        else:
            log.info("Writing generation data...")
            save_generation_data(session, data, write_to_db)

        log.info("Done!")


if __name__ == "__main__":
    app()
