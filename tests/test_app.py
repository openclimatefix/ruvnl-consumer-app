"""
Tests for functions in app.py
"""
import logging
import uuid

import pandas as pd
import pytest
import requests
from freezegun import freeze_time
from pvsite_datamodel import GenerationSQL, SiteSQL

from ruvnl_consumer_app.app import (
    DEFAULT_DATA_URL,
    app,
    fetch_data,
    get_sites,
    merge_generation_data_with_sites,
    save_generation_data,
)

from ._utils import load_mock_response, run_click_script

retry_interval = 0


class TestGetSites:
    """
    Test Suite for getting sites
    """

    def test_get_sites(self, db_session):
        """Test for getting correct sites"""

        sites = get_sites(db_session)
        sites = sorted(sites, key=lambda s: s.asset_type.name)

        assert len(sites) == 2
        for site in sites:
            assert isinstance(site.site_uuid, uuid.UUID)

        assert sites[0].asset_type.name == "pv"
        assert sites[1].asset_type.name == "wind"


class TestFetchData:
    """
    Test suite for fetching data from RUVNL
    """

    @freeze_time("2021-01-31T10:01:00Z")
    def test_fetch_data(self, requests_mock):
        """Test for correctly fetching data"""

        requests_mock.get(
            DEFAULT_DATA_URL,
            text=load_mock_response("tests/mock/responses/ruvnl-valid-response.json"),
        )
        result = fetch_data(DEFAULT_DATA_URL, retry_interval=retry_interval)

        assert isinstance(result, pd.DataFrame)

        # Assert correct num rows/cols and column names
        assert result.shape == (2, 3)
        for col in ["asset_type", "start_utc", "power_kw"]:
            assert col in result.columns

        # Ensure 1 pv and wind value
        result.sort_values(by="asset_type", inplace=True)
        assert result.iloc[0]["asset_type"] == "pv"
        assert result.iloc[1]["asset_type"] == "wind"

        for vals in result[["start_utc", "power_kw"]]:
            assert not pd.isna(vals)

    @freeze_time("2021-01-31T10:01:00Z")
    def test_fetch_data_with_missing_asset(self, requests_mock, caplog):
        """Test for fetching data with missing asset type"""

        requests_mock.get(
            DEFAULT_DATA_URL,
            text=load_mock_response("tests/mock/responses/ruvnl-valid-response-missing-pv.json"),
        )
        result = fetch_data(DEFAULT_DATA_URL, retry_interval=retry_interval)

        assert result.shape[0] == 1
        assert result.iloc[0]["asset_type"] == "wind"
        assert "No generation data for asset type: pv" in caplog.text

    def test_catch_bad_response_code(self, requests_mock):
        """Test for catching bad response code"""

        requests_mock.get(DEFAULT_DATA_URL, status_code=404, reason="Not Found")
        # just a warning now
        fetch_data(DEFAULT_DATA_URL, retry_interval=retry_interval)

    def test_old_fetch_data(self, requests_mock):
        """Test for correctly fetching data"""

        requests_mock.get(
            DEFAULT_DATA_URL,
            text=load_mock_response("tests/mock/responses/ruvnl-valid-response.json"),
        )

        # we now just get a warning
        fetch_data(DEFAULT_DATA_URL, retry_interval=retry_interval)

    def test_catch_bad_response_json(self, requests_mock):
        """Test for catching invalid response JSON"""

        requests_mock.get(
            DEFAULT_DATA_URL,
            text=load_mock_response("tests/mock/responses/ruvnl-invalid-response.json"),
        )
        with pytest.raises(requests.exceptions.JSONDecodeError):
            fetch_data(DEFAULT_DATA_URL, retry_interval=retry_interval)

    def test_call_bad_url(self, requests_mock):
        """Test to check timeout doesnt cause error"""

        requests_mock.get(DEFAULT_DATA_URL, exc=requests.exceptions.ConnectTimeout)

        data = fetch_data(DEFAULT_DATA_URL, retry_interval=retry_interval)
        assert len(data) == 0


class TestMergeGenerationDataWithSite:
    """Test suite for merging generation data with site ids"""

    def test_merge_data_with_sites(self, db_session, unassociated_generation_data):
        """Test for successful merge of generation data with sites"""

        sites = db_session.query(SiteSQL).all()
        result = merge_generation_data_with_sites(unassociated_generation_data, sites)

        assert isinstance(result, pd.DataFrame)

        # Assert correct num rows/cols and column names
        assert result.shape == (2, 4)
        for col in ["site_uuid", "start_utc", "power_kw"]:
            assert col in result.columns

        for site_uuid in result["site_uuid"]:
            assert pd.notnull(site_uuid)

    def test_merge_data_with_sites_with_missing_pv_data(
        self, db_session, unassociated_generation_data
    ):
        """Test for merge of generation data without pv with sites"""

        sites = db_session.query(SiteSQL).all()
        df = unassociated_generation_data
        data_without_pv = df.drop(df[df["asset_type"] == "pv"].index)

        assert data_without_pv.shape[0] == 1
        assert data_without_pv.iloc[0]["asset_type"] == "wind"

        result = merge_generation_data_with_sites(data_without_pv, sites)

        assert result.shape[0] == 1
        assert result.iloc[0]["site_uuid"] == next(
            s.site_uuid for s in sites if s.asset_type.name == "wind"
        )

    def test_merge_data_with_sites_with_missing_pv_site(
        self, db_session, unassociated_generation_data
    ):
        """Test for merge of generation data with missing pv site"""

        sites = db_session.query(SiteSQL).filter(SiteSQL.asset_type == "wind")

        result = merge_generation_data_with_sites(unassociated_generation_data, sites)

        assert result.shape[0] == 1
        assert result.iloc[0]["site_uuid"] == next(
            s.site_uuid for s in sites if s.asset_type.name == "wind"
        )


@pytest.mark.parametrize("write_to_db", [True, False])
def test_save_generation_data(write_to_db, db_session, caplog, associated_generation_data):
    """Test for saving generation data and capacity updates"""

    caplog.set_level(logging.INFO)
    
    # initial site capacity
    site_uuid = associated_generation_data["site_uuid"].iloc[0]
    site = db_session.query(SiteSQL).filter(SiteSQL.site_uuid == site_uuid).first()
    initial_capacity = site.capacity_kw
    
    # generation data to exceed capacity
    associated_generation_data.loc[0, "power_kw"] = initial_capacity * 2
    
    save_generation_data(db_session, associated_generation_data, write_to_db)

    if write_to_db:
        assert db_session.query(GenerationSQL).count() == 2
        # Check capacity was updated
        site = db_session.query(SiteSQL).filter(SiteSQL.site_uuid == site_uuid).first()
        assert site.capacity_kw == initial_capacity * 2
    else:
        assert "Generation data:" in caplog.text
        # Verify capacity wasn't updated when not writing to db
        site = db_session.query(SiteSQL).filter(SiteSQL.site_uuid == site_uuid).first()
        assert site.capacity_kw == initial_capacity


@freeze_time("2021-01-31T10:01:00Z")
@pytest.mark.parametrize("write_to_db", [True, False])
def test_app(write_to_db, requests_mock, db_session, caplog):
    """Test for running app from command line"""

    caplog.set_level(logging.INFO)
    requests_mock.get(
        DEFAULT_DATA_URL, text=load_mock_response("tests/mock/responses/ruvnl-valid-response.json")
    )
    init_n_generation_data = db_session.query(GenerationSQL).count()

    args = [f"--retry-interval={retry_interval}"]
    if write_to_db:
        args.append("--write-to-db")

    result = run_click_script(app, args)
    assert result.exit_code == 0

    if write_to_db:
        assert db_session.query(GenerationSQL).count() == init_n_generation_data + 2
    else:
        assert db_session.query(GenerationSQL).count() == init_n_generation_data

