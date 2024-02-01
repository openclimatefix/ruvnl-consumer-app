"""
Fixtures for testing
"""

import datetime as dt
import os

import pandas as pd
import pytest
from pvsite_datamodel.sqlmodels import Base, SiteSQL
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="session")
def engine():
    """Database engine fixture."""

    with PostgresContainer("postgres:14.5") as postgres:
        url = postgres.get_connection_url()
        os.environ["DB_URL"] = url
        engine = create_engine(url)
        Base.metadata.create_all(engine)

        yield engine


@pytest.fixture()
def db_session(engine):
    """Return a sqlalchemy session, which tears down everything properly post-test."""

    connection = engine.connect()
    # begin the nested transaction
    transaction = connection.begin()
    # use the connection with the already started transaction

    with Session(bind=connection) as session:
        yield session

        session.close()
        # roll back the broader transaction
        transaction.rollback()
        # put back the connection to the connection pool
        connection.close()
        session.flush()

    engine.dispose()


@pytest.fixture(scope="session", autouse=True)
def db_data(engine):
    """Seed some initial data into DB."""

    with engine.connect() as connection:
        with Session(bind=connection) as session:
            # PV site
            site = SiteSQL(
                client_site_id=1,
                latitude=20.59,
                longitude=78.96,
                capacity_kw=4,
                ml_id=1,
                asset_type="pv",
                country="india"
            )
            session.add(site)

            # Wind site
            site = SiteSQL(
                client_site_id=2,
                latitude=20.59,
                longitude=78.96,
                capacity_kw=4,
                ml_id=2,
                asset_type="wind",
                country="india"
            )
            session.add(site)

            session.commit()


@pytest.fixture()
def unassociated_generation_data(db_session):
    """
    A valid generation dataframe not associated with site uuids

    Instead this dataframe provides the associated asset types
    """

    sites = db_session.query(SiteSQL).all()
    data = [(
        "pv" if i == 0 else "wind",
        dt.datetime.now(tz=dt.UTC),
        i+1
    ) for i, s in enumerate(sites)]

    return pd.DataFrame(data, columns=["asset_type", "start_utc", "power_kw"])


@pytest.fixture()
def associated_generation_data(db_session):
    """A valid generation dataframe with associated site uuids"""

    sites = db_session.query(SiteSQL).all()
    data = [(s.site_uuid, dt.datetime.now(tz=dt.UTC), i+1) for i, s in enumerate(sites)]

    return pd.DataFrame(data, columns=["site_uuid", "start_utc", "power_kw"])