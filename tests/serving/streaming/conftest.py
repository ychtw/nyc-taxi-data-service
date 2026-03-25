import types
from datetime import datetime
from pathlib import Path

import pytest


@pytest.fixture
def resolved_runtime_config(tmp_path: Path) -> dict:
    # build a minimal but realistic resolved runtime config containing only
    # the settings required by KafkaReplayProducer.__init__()
    return {
        "serving": {
            "service_event_time_column_by_vehicle_type": {
                "yellow": "tpep_dropoff_datetime",
                "green": "lpep_dropoff_datetime",
                "fhv": "dropOff_datetime",
                "fhvhv": "dropoff_datetime",
            },
            "streaming": {
                "buffer": {
                    "db_base_path": str(tmp_path / "stream_buffer"),
                    "db_table_name": "buffered_data",
                },
                "kafka": {
                    "topic": {
                        "prefix": "nyc_taxi",
                    },
                    "client": {
                        "bootstrap_servers": "localhost:9092",
                        "client_id": "nyc-taxi-data-service",
                    },
                    "producer": {
                        "acks": "all",
                        "linger_ms": 0,
                        "batch_size": 65536,
                        "compression_type": "snappy",
                    },
                },
            },
        }
    }


@pytest.fixture
def mimic_service_start_time() -> datetime:
    return datetime(2025, 3, 1, 0, 0, 0)


@pytest.fixture
def kafka_producer_module() -> types.ModuleType:
    import nyc_taxi_data_service.serving.streaming.kafka_producer as module

    return module


@pytest.fixture
def dummy_admin_config() -> dict:
    """Return a representative admin client config for Kafka admin tests."""
    return {
        "bootstrap.servers": "localhost:9092",
        "client.id": "nyc-taxi-data-service",
    }
