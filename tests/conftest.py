"""
Fixtures for testing
"""

import os

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
            n_sites = 3

            # Sites
            for i in range(n_sites):
                site = SiteSQL(
                    client_site_id=i + 1,
                    latitude=51,
                    longitude=3,
                    capacity_kw=4,
                    ml_id=i,
                    country="india"
                )
                session.add(site)

            session.commit()
