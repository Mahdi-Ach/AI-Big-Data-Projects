# spark/tests/test_transformations.py
from datetime import datetime


def compute_taux_remplissage(num_bikes: int, capacity: int):
    """Fonction pure reproduisant la logique du job Silver."""
    if capacity is None or capacity <= 0:
        return None
    if num_bikes is None or num_bikes < 0:
        return None
    return num_bikes / capacity


def is_valid_coordinates(lat: float, lon: float) -> bool:
    """Filtre géographique utilisé dans le job Silver (Paris intra-muros)."""
    if lat is None or lon is None:
        return False
    return 48.7 <= lat <= 49.0 and 2.2 <= lon <= 2.6


def parse_ingested_at(value: str):
    """Parse un timestamp ISO 8601 (utilisé côté Bronze)."""
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class TestTauxRemplissage:
    def test_cas_nominal(self):
        assert compute_taux_remplissage(10, 20) == 0.5

    def test_station_pleine(self):
        assert compute_taux_remplissage(20, 20) == 1.0

    def test_station_vide(self):
        assert compute_taux_remplissage(0, 20) == 0.0

    def test_capacity_zero(self):
        assert compute_taux_remplissage(5, 0) is None

    def test_capacity_negative(self):
        assert compute_taux_remplissage(5, -10) is None

    def test_bikes_negatif(self):
        assert compute_taux_remplissage(-1, 20) is None

    def test_capacity_none(self):
        assert compute_taux_remplissage(5, None) is None


class TestCoordonnees:
    def test_paris_valide(self):
        assert is_valid_coordinates(48.8566, 2.3522) is True

    def test_hors_paris(self):
        assert is_valid_coordinates(43.2965, 5.3698) is False

    def test_none(self):
        assert is_valid_coordinates(None, 2.35) is False


class TestParseIngestedAt:
    def test_iso_utc(self):
        dt = parse_ingested_at("2026-10-29T15:22:57.662213+00:00")
        assert dt is not None
        assert dt.year == 2026
        assert dt.hour == 15

    def test_z_suffix(self):
        dt = parse_ingested_at("2026-10-29T15:22:57Z")
        assert dt is not None
        assert dt.tzinfo is not None

    def test_vide(self):
        assert parse_ingested_at("") is None