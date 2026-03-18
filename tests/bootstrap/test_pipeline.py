from pathlib import Path

import pytest

from nyc_taxi_data_service.bootstrap.pipeline import run_bootstrap


def test_run_bootstrap_calls_builder_and_downloader(monkeypatch):
    """
    Verify that run_bootstrap calls run_manifest_builder and run_downloader
    in order, and returns their output paths.
    """

    fake_manifest_path = Path("manifest.json")
    fake_log_path = Path("log.json")

    def mock_run_manifest_builder(sources_config, resolved_runtime_config):
        return fake_manifest_path

    def mock_run_downloader(manifest_path):
        assert manifest_path == fake_manifest_path
        return fake_log_path

    monkeypatch.setattr(
        "nyc_taxi_data_service.bootstrap.pipeline.run_manifest_builder",
        mock_run_manifest_builder,
    )

    monkeypatch.setattr(
        "nyc_taxi_data_service.bootstrap.pipeline.run_downloader",
        mock_run_downloader,
    )

    result = run_bootstrap(
        sources_config={"a": 1},
        resolved_runtime_config={"b": 2},
    )

    assert result == {
        "manifest_path": fake_manifest_path,
        "log_path": fake_log_path,
    }


def test_run_bootstrap_allows_none_log_path(monkeypatch):
    """
    Verify that run_bootstrap returns log_path=None if downloader
    returns None.
    """

    fake_manifest_path = Path("manifest.json")

    monkeypatch.setattr(
        "nyc_taxi_data_service.bootstrap.pipeline.run_manifest_builder",
        lambda *args, **kwargs: fake_manifest_path,
    )

    monkeypatch.setattr(
        "nyc_taxi_data_service.bootstrap.pipeline.run_downloader",
        lambda manifest_path: None,
    )

    result = run_bootstrap({}, {})

    assert result["manifest_path"] == fake_manifest_path
    assert result["log_path"] is None


def test_run_bootstrap_propagates_builder_error(monkeypatch):
    """
    Verify that run_bootstrap does not swallow exceptions from
    run_manifest_builder.
    """

    def mock_builder(*args, **kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(
        "nyc_taxi_data_service.bootstrap.pipeline.run_manifest_builder",
        mock_builder,
    )

    with pytest.raises(RuntimeError, match="boom"):
        run_bootstrap({}, {})
