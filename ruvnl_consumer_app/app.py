"""
Main consumer app entrypoint
"""

import datetime as dt
import logging
import os
import sys

import click
import pandas as pd
import requests
from pvsite_datamodel import DatabaseConnection, SiteSQL
from pvsite_datamodel.read import get_sites_by_country
from pvsite_datamodel.write import insert_generation_values
from sqlalchemy.orm import Session

log = logging.getLogger(__name__)

DATA_URL = "http://sldc.rajasthan.gov.in/rrvpnl/read-sftp?type=overview"


def get_sites(db_session: Session) -> list[SiteSQL]:
    """
    Gets 1 site for each asset type (pv and wind)

    Args:
            db_session: A SQLAlchemy session

    Returns:
            A list of SiteSQL objects
    """

    sites = get_sites_by_country(db_session, country="india")

    # TODO don't assume there are only 2 sites in the DB - check!
    return sites


def fetch_data(data_url: str) -> pd.DataFrame:
    """
    Fetches the latest state-wide generation data for Rajasthan

    Args:
            data_url: The URL ot query data from

    Returns:
            A pandas DataFrame of generation values for wind and PV
    """
    r = requests.get(data_url)

    # Raise error if response is 4XX or 5XX
    r.raise_for_status()

    raw_data = r.json()
    asset_map = {"WIND GEN": "wind", "SOLAR GEN": "pv"}
    data = []
    for d in raw_data["data"]:
        record = d["0"]
        key = record["scada_name"]
        if key in asset_map.keys():
            data.append({
                "asset_type": asset_map[key],
                "start_utc": dt.datetime.fromtimestamp(int(record["SourceTimeSec"]), tz=dt.UTC),
                "power_kw": record["Average2"] * 1000  # source is in MW, convert to kW
            })

    return pd.DataFrame(data)


def save_generation_data(
        db_session: Session,
        generation_data: pd.DataFrame,
        write_to_db: bool
) -> None:
    """
    Saves generation data to DB (or prints to stdout)

    Args:
            db_session: A SQLAlchemy session
            generation_data: a pandas Dataframe of generation values for PV and wind
            write_to_db: If true, generation values are written to db, otherwise to stdout
    """
    if write_to_db:
        insert_generation_values(db_session, generation_data)
    else:
        log.info(f"\n{generation_data}")


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

    # 0. Initialise DB connection
    url = os.environ["DB_URL"]
    db_conn = DatabaseConnection(url, echo=False)

    with db_conn.get_session() as session:
        # 1. Get sites
        log.info("Getting sites...")
        # TODO Gets sites (1 for PV, 1 for wind)

        # 2. Fetch latest generation data
        log.info(f"Fetching generation data from {DATA_URL}...")
        data = fetch_data(DATA_URL)

        # 3. Assign site to generation data
        # TODO Assign site to generation data

        # 3. Write generation data to DB or stdout
        log.info("Writing generation data...")
        save_generation_data(session, data, write_to_db)

        log.info("Done!")


if __name__ == "__main__":
    app()
