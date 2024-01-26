"""
Tests for functions in app.py
"""

import pytest

from ruvnl_consumer_app.app import app

from ._utils import run_click_script


def test_get_sites(db_session):
    """Test for getting correct sites"""
    pass


def test_save_generation_data(db_session):
    """Test for saving generation data"""
    pass


def test_fetch_data():
    """Test for correctly fetching data"""
    pass




@pytest.mark.parametrize("write_to_db", [True, False])
def test_app(write_to_db, db_session):
    """Test for running app from command line"""

    pass
    # init_n_forecasts = db_session.query(ForecastSQL).count()
    # init_n_forecast_values = db_session.query(ForecastValueSQL).count()

    args = []
    if write_to_db:
        args.append('--write-to-db')

    result = run_click_script(app, args)
    assert result.exit_code == 0

    # if write_to_db:
    #     assert db_session.query(ForecastSQL).count() == init_n_forecasts + 3
    #     assert db_session.query(ForecastValueSQL).count() == init_n_forecast_values + (3 * 192)
    # else:
    #     assert db_session.query(ForecastSQL).count() == init_n_forecasts
    #     assert db_session.query(ForecastValueSQL).count() == init_n_forecast_values
