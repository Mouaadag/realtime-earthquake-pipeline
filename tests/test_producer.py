"""
Tests unitaires pour earthquake_producer.py.

Aucune connexion réseau, Kafka ou PostgreSQL — tout est mocké.
"""

import sys
import types
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
#  Mock des dépendances externes avant l'import du module
# ---------------------------------------------------------------------------

# kafka-python n'est pas forcément installé localement
kafka_mock = types.ModuleType("kafka")
kafka_mock.KafkaProducer = MagicMock
sys.modules.setdefault("kafka", kafka_mock)

# psycopg2 idem
psycopg2_mock = types.ModuleType("psycopg2")
psycopg2_mock.connect = MagicMock
psycopg2_mock.Error = Exception
sys.modules.setdefault("psycopg2", psycopg2_mock)

import producer.earthquake_producer as ep  # noqa: E402


# ---------------------------------------------------------------------------
#  _parse_feature
# ---------------------------------------------------------------------------

SAMPLE_FEATURE = {
    "id": "us7000abc1",
    "properties": {
        "code":    "7000abc1",
        "mag":     6.2,
        "magType": "mww",
        "place":   "50km NW of Tokyo, Japan",
        "time":    1700000000000,   # ms depuis epoch
        "sig":     650,
        "tsunami": 0,
        "status":  "reviewed",
        "type":    "earthquake",
        "title":   "M 6.2 - 50km NW of Tokyo, Japan",
    },
    "geometry": {
        "coordinates": [139.5, 35.8, 12.0],   # [lon, lat, depth]
    },
}


@pytest.fixture
def parsed():
    return ep._parse_feature(SAMPLE_FEATURE)


def test_parse_event_id(parsed):
    assert parsed["event_id"] == "7000abc1"


def test_parse_magnitude(parsed):
    assert parsed["magnitude"] == 6.2


def test_parse_coordinates(parsed):
    assert parsed["latitude"]  == 35.8
    assert parsed["longitude"] == 139.5
    assert parsed["depth_km"]  == 12.0


def test_parse_event_time_is_iso(parsed):
    """event_time doit être une chaîne ISO 8601 avec timezone UTC."""
    assert parsed["event_time"].endswith("+00:00")


def test_parse_significance(parsed):
    assert parsed["significance"] == 650


def test_parse_tsunami_flag(parsed):
    assert parsed["tsunami"] == 0


def test_parse_ingested_at_present(parsed):
    assert parsed["ingested_at"] is not None


def test_parse_missing_coords():
    """Feature sans géométrie — ne doit pas lever d'exception."""
    feature = {
        "id": "xx001",
        "properties": {"code": "xx001", "mag": 3.0, "time": None},
        "geometry": {"coordinates": [None, None, None]},
    }
    result = ep._parse_feature(feature)
    assert result["latitude"]  is None
    assert result["longitude"] is None
    assert result["depth_km"]  is None


def test_parse_missing_time():
    """Timestamp None → event_time doit être None sans crash."""
    feature = {**SAMPLE_FEATURE, "properties": {**SAMPLE_FEATURE["properties"], "time": None}}
    result = ep._parse_feature(feature)
    assert result["event_time"] is None


def test_parse_uses_code_over_id():
    """event_id doit utiliser 'code' en priorité sur 'id'."""
    feature = {**SAMPLE_FEATURE}
    feature["properties"] = {**SAMPLE_FEATURE["properties"], "code": "mycode"}
    assert ep._parse_feature(feature)["event_id"] == "mycode"


def test_parse_fallback_to_id_when_no_code():
    """Sans 'code', event_id doit utiliser 'id'."""
    feature = {**SAMPLE_FEATURE, "id": "fallback-id"}
    props = {k: v for k, v in SAMPLE_FEATURE["properties"].items() if k != "code"}
    feature["properties"] = props
    assert ep._parse_feature(feature)["event_id"] == "fallback-id"


# ---------------------------------------------------------------------------
#  _wait_for_kafka
# ---------------------------------------------------------------------------

def test_wait_for_kafka_succeeds_immediately():
    """Si Kafka répond au premier essai, la fonction doit retourner sans erreur."""
    with patch("producer.earthquake_producer.KafkaProducer") as MockProducer:
        MockProducer.return_value.close = MagicMock()
        ep._wait_for_kafka(max_retries=3, delay=0)
        MockProducer.assert_called_once()


def test_wait_for_kafka_raises_after_max_retries():
    """Si Kafka ne répond jamais, RuntimeError doit être levé."""
    with patch("producer.earthquake_producer.KafkaProducer", side_effect=Exception("refused")):
        with pytest.raises(RuntimeError, match="inaccessible"):
            ep._wait_for_kafka(max_retries=2, delay=0)


# ---------------------------------------------------------------------------
#  _wait_for_postgres
# ---------------------------------------------------------------------------

def test_wait_for_postgres_succeeds_immediately():
    with patch("producer.earthquake_producer._get_pg_conn") as mock_conn:
        mock_conn.return_value.close = MagicMock()
        ep._wait_for_postgres(max_retries=3, delay=0)
        mock_conn.assert_called_once()


def test_wait_for_postgres_raises_after_max_retries():
    with patch("producer.earthquake_producer._get_pg_conn", side_effect=Exception("refused")):
        with pytest.raises(RuntimeError, match="inaccessible"):
            ep._wait_for_postgres(max_retries=2, delay=0)
