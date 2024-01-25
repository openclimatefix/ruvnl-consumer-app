"""
Main consumer app entrypoint
"""

import datetime as dt
import logging
import os
import sys

import click
from pvsite_datamodel import DatabaseConnection


log = logging.getLogger(__name__)


@click.command()
@click.option(
    "--date",
    "-d",
    "timestamp",
    type=click.DateTime(formats=["%Y-%m-%d-%H-%M"]),
    default=None,
    help='Date-time (UTC) at which we make the prediction. \
Format should be YYYY-MM-DD-HH-mm. Defaults to "now".',
)
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
def app(timestamp: dt.datetime | None, write_to_db: bool, log_level: str):
    """
    Main function for running data consumer
    """
    logging.basicConfig(stream=sys.stdout, level=getattr(logging, log_level.upper()))

    if timestamp is None:
        timestamp = dt.datetime.now(tz=dt.UTC)
        log.info('Timestamp omitted - will generate forecasts for "now"')
    else:
        # Ensure timestamp is UTC
        timestamp.replace(tzinfo=dt.UTC)
        
    # 0. Initialise DB connection
    url = os.environ["DB_URL"]

    db_conn = DatabaseConnection(url, echo=False)
    
    with db_conn.get_session() as session:
        pass


if __name__ == "__main__":
    app()
