"""Tests for the adql_to_lsdb module, specifically parse_adql_entities."""

import pathlib

import pytest

from adql_to_lsdb import parse_adql_entities


class TestSampleQueries:
    """Test that sample ADQL files parse correctly."""

    @pytest.fixture
    def samples_dir(self):
        """Get the path to the samples directory."""
        return pathlib.Path(__file__).parent.parent.parent / "src" / "samples"

    def test_sample1_parses(self, samples_dir):
        """Test sample1.adql parses correctly with expected entities."""
        adql = (samples_dir / "sample1.adql").read_text()
        result = parse_adql_entities(adql)

        assert result["tables"] == ["gaia_dr3.gaia"]
        assert result["columns"] == ["source_id", "ra", "dec", "phot_g_mean_mag", "phot_variable_flag"]
        assert result["spatial_search"] == {
            "type": "ConeSearch",
            "ra": 270.0,
            "dec": 23.0,
            "radius": 0.25,
        }
        assert result["limits"] == 15
        assert ("phot_g_mean_mag", "<", 16) in result["conditions"]
        assert ("phot_variable_flag", "==", "VARIABLE") in result["conditions"]

    def test_sample2_parses(self, samples_dir):
        """Test sample2.adql parses correctly with expected entities."""
        adql = (samples_dir / "sample2.adql").read_text()
        result = parse_adql_entities(adql)

        assert result["tables"] == ["ztf_dr22"]
        assert result["columns"] == ["objectid", "objra", "objdec", "nepochs", "mag", "magerr"]
        assert result["spatial_search"] == {
            "type": "ConeSearch",
            "ra": 280.0,
            "dec": 0.0,
            "radius": 0.1,
        }
        assert result["limits"] == 15

    def test_sample3_polygon_parses(self, samples_dir):
        """Test sample3.adql with POLYGON parses correctly."""
        adql = (samples_dir / "sample3.adql").read_text()
        result = parse_adql_entities(adql)

        assert result["tables"] == ["ztf_dr22"]
        assert result["columns"] == ["objectid", "objra", "objdec"]
        assert result["spatial_search"]["type"] == "PolygonSearch"
        assert result["spatial_search"]["coordinates"] == [
            (280.0, 30.0),
            (281.0, 30.0),
            (281.0, 29.0),
            (279.0, 27.0),
        ]
        assert result["limits"] == 10

    def test_sample4_scientific_notation_parses(self, samples_dir):
        """Test sample4.adql with scientific notation parses correctly."""
        adql = (samples_dir / "sample4.adql").read_text()
        result = parse_adql_entities(adql)

        assert result["tables"] == ["ztf_dr22"]
        assert result["spatial_search"]["type"] == "PolygonSearch"
        # Verify scientific notation parsing: 1.2e-3 = 0.0012
        coords = result["spatial_search"]["coordinates"]
        assert coords[0] == (280.0, pytest.approx(0.0012))

    def test_sample5_order_by_parses(self, samples_dir):
        """Test sample5.adql with ORDER BY parses correctly."""
        adql = (samples_dir / "sample5.adql").read_text()
        result = parse_adql_entities(adql)

        assert result["tables"] == ["ztf_dr22"]
        assert result["order_by"] == [("ra", True), ("dec", False)]


class TestMultipleContainsValidation:
    """Test that multiple CONTAINS clauses raise ValueError."""

    def test_multiple_contains_raises_value_error(self):
        """Test that a query with two CONTAINS clauses raises ValueError."""
        adql = """
        SELECT ra, dec
        FROM gaiadr3.gaia
        WHERE 1 = CONTAINS(
            POINT('ICRS', ra, dec),
            CIRCLE('ICRS', 270.0, 23.0, 0.25)
        )
        AND 1 = CONTAINS(
            POINT('ICRS', ra, dec),
            CIRCLE('ICRS', 280.0, 0.0, 0.1)
        )
        """
        with pytest.raises(ValueError, match="Multiple CONTAINS"):
            parse_adql_entities(adql)

    def test_single_contains_succeeds(self):
        """Test that a query with one CONTAINS clause works."""
        adql = """
        SELECT ra, dec
        FROM gaiadr3.gaia
        WHERE 1 = CONTAINS(
            POINT('ICRS', ra, dec),
            CIRCLE('ICRS', 270.0, 23.0, 0.25)
        )
        """
        result = parse_adql_entities(adql)
        assert result["spatial_search"]["type"] == "ConeSearch"


class TestPointCirclePolygonOnlyInContains:
    """Test that POINT, CIRCLE, and POLYGON can only appear within a CONTAINS clause."""

    def test_point_outside_contains_raises_error(self):
        """Test that POINT outside of CONTAINS raises ValueError."""
        adql = """
        SELECT ra, dec, POINT('ICRS', ra, dec) AS pos
        FROM gaiadr3.gaia
        """
        with pytest.raises(ValueError, match="POINT outside of CONTAINS"):
            parse_adql_entities(adql)

    def test_circle_outside_contains_raises_error(self):
        """Test that CIRCLE outside of CONTAINS raises ValueError."""
        adql = """
        SELECT ra, dec
        FROM gaiadr3.gaia
        WHERE ra = CIRCLE('ICRS', 270.0, 23.0, 0.25)
        """
        with pytest.raises(ValueError, match="CIRCLE outside of CONTAINS"):
            parse_adql_entities(adql)

    def test_polygon_outside_contains_raises_error(self):
        """Test that POLYGON outside of CONTAINS raises ValueError."""
        adql = """
        SELECT ra, dec
        FROM gaiadr3.gaia
        WHERE ra = POLYGON('ICRS', 280.0, 30.0, 281.0, 30.0, 281.0, 29.0)
        """
        with pytest.raises(ValueError, match="POLYGON outside of CONTAINS"):
            parse_adql_entities(adql)


class TestCoordinateSystemValidation:
    """Test that ICRS is the only allowed coordinate system."""

    def test_icrs_coordinate_system_succeeds(self):
        """Test that ICRS coordinate system works."""
        adql = """
        SELECT ra, dec
        FROM gaiadr3.gaia
        WHERE 1 = CONTAINS(
            POINT('ICRS', ra, dec),
            CIRCLE('ICRS', 270.0, 23.0, 0.25)
        )
        """
        result = parse_adql_entities(adql)
        assert result["spatial_search"]["type"] == "ConeSearch"

    def test_non_icrs_in_point_raises_error(self):
        """Test that non-ICRS coordinate system in POINT raises NotImplementedError."""
        adql = """
        SELECT ra, dec
        FROM gaiadr3.gaia
        WHERE 1 = CONTAINS(
            POINT('FK5', ra, dec),
            CIRCLE('ICRS', 270.0, 23.0, 0.25)
        )
        """
        with pytest.raises((NotImplementedError, ValueError)):
            parse_adql_entities(adql)

    def test_non_icrs_in_circle_raises_error(self):
        """Test that non-ICRS coordinate system in CIRCLE raises NotImplementedError."""
        adql = """
        SELECT ra, dec
        FROM gaiadr3.gaia
        WHERE 1 = CONTAINS(
            POINT('ICRS', ra, dec),
            CIRCLE('GALACTIC', 270.0, 23.0, 0.25)
        )
        """
        with pytest.raises((NotImplementedError, ValueError)):
            parse_adql_entities(adql)

    def test_non_icrs_in_polygon_raises_error(self):
        """Test that non-ICRS coordinate system in POLYGON raises NotImplementedError."""
        adql = """
        SELECT objra, objdec
        FROM ztf_dr22
        WHERE CONTAINS(
            POINT('ICRS', objra, objdec),
            POLYGON('FK4', 280.0, 30.0, 281.0, 30.0, 281.0, 29.0)) = 1
        """
        with pytest.raises((NotImplementedError, ValueError)):
            parse_adql_entities(adql)


class TestRaDecInSelectValidation:
    """Test that RA and DEC variables in POINT must be in the SELECT clause."""

    def test_ra_dec_in_select_succeeds(self):
        """Test that when RA/DEC columns are in SELECT, parsing succeeds."""
        adql = """
        SELECT ra, dec
        FROM gaiadr3.gaia
        WHERE 1 = CONTAINS(
            POINT('ICRS', ra, dec),
            CIRCLE('ICRS', 270.0, 23.0, 0.25)
        )
        """
        result = parse_adql_entities(adql)
        assert "ra" in result["columns"]
        assert "dec" in result["columns"]

    def test_ra_not_in_select_raises_error(self):
        """Test that RA column not in SELECT raises ValueError."""
        adql = """
        SELECT source_id, dec
        FROM gaiadr3.gaia
        WHERE 1 = CONTAINS(
            POINT('ICRS', ra, dec),
            CIRCLE('ICRS', 270.0, 23.0, 0.25)
        )
        """
        with pytest.raises(ValueError, match="RA column 'ra' not in SELECT"):
            parse_adql_entities(adql)

    def test_dec_not_in_select_raises_error(self):
        """Test that DEC column not in SELECT raises ValueError."""
        adql = """
        SELECT source_id, ra
        FROM gaiadr3.gaia
        WHERE 1 = CONTAINS(
            POINT('ICRS', ra, dec),
            CIRCLE('ICRS', 270.0, 23.0, 0.25)
        )
        """
        with pytest.raises(ValueError, match="DEC column 'dec' not in SELECT"):
            parse_adql_entities(adql)

    def test_custom_column_names_work(self):
        """Test that custom column names like objra/objdec work when in SELECT."""
        adql = """
        SELECT objectid, objra, objdec
        FROM ztf_dr22
        WHERE CONTAINS(
            POINT('ICRS', objra, objdec),
            CIRCLE('ICRS', 280.0, 0.0, 0.1)) = 1
        """
        result = parse_adql_entities(adql)
        assert "objra" in result["columns"]
        assert "objdec" in result["columns"]


class TestEntityDictionary:
    """Test the structure and contents of the dictionary returned by parse_adql_entities."""

    def test_entities_has_required_keys(self):
        """Test that the returned dictionary has all required keys."""
        adql = """
        SELECT ra, dec
        FROM gaiadr3.gaia
        WHERE 1 = CONTAINS(
            POINT('ICRS', ra, dec),
            CIRCLE('ICRS', 270.0, 23.0, 0.25)
        )
        """
        result = parse_adql_entities(adql)

        assert "tables" in result
        assert "columns" in result
        assert "spatial_search" in result
        assert "conditions" in result
        assert "limits" in result
        assert "order_by" in result

    def test_cone_search_structure(self):
        """Test that ConeSearch has correct structure."""
        adql = """
        SELECT ra, dec
        FROM gaiadr3.gaia
        WHERE 1 = CONTAINS(
            POINT('ICRS', ra, dec),
            CIRCLE('ICRS', 270.0, 23.0, 0.25)
        )
        """
        result = parse_adql_entities(adql)
        spatial = result["spatial_search"]

        assert spatial["type"] == "ConeSearch"
        assert "ra" in spatial
        assert "dec" in spatial
        assert "radius" in spatial
        assert isinstance(spatial["ra"], (int, float))
        assert isinstance(spatial["dec"], (int, float))
        assert isinstance(spatial["radius"], (int, float))

    def test_polygon_search_structure(self):
        """Test that PolygonSearch has correct structure."""
        adql = """
        SELECT objra, objdec
        FROM ztf_dr22
        WHERE CONTAINS(
            POINT('ICRS', objra, objdec),
            POLYGON('ICRS', 280.0, 30.0, 281.0, 30.0, 281.0, 29.0)) = 1
        """
        result = parse_adql_entities(adql)
        spatial = result["spatial_search"]

        assert spatial["type"] == "PolygonSearch"
        assert "coordinates" in spatial
        assert isinstance(spatial["coordinates"], list)
        assert len(spatial["coordinates"]) >= 3
        for coord in spatial["coordinates"]:
            assert isinstance(coord, tuple)
            assert len(coord) == 2

    def test_conditions_structure(self):
        """Test that conditions have correct structure."""
        adql = """
        SELECT ra, dec, phot_g_mean_mag
        FROM gaiadr3.gaia
        WHERE 1 = CONTAINS(
            POINT('ICRS', ra, dec),
            CIRCLE('ICRS', 270.0, 23.0, 0.25)
        )
        AND phot_g_mean_mag < 16
        """
        result = parse_adql_entities(adql)
        conditions = result["conditions"]

        assert isinstance(conditions, list)
        for cond in conditions:
            assert isinstance(cond, tuple)
            assert len(cond) == 3  # (column, operator, value)

    def test_order_by_structure(self):
        """Test that order_by has correct structure."""
        adql = """
        SELECT objra, objdec
        FROM ztf_dr22
        WHERE CONTAINS(
            POINT('ICRS', objra, objdec),
            POLYGON('ICRS', 280.0, 30.0, 281.0, 30.0, 281.0, 29.0)) = 1
        ORDER BY objra ASC, objdec DESC
        """
        result = parse_adql_entities(adql)
        order_by = result["order_by"]

        assert isinstance(order_by, list)
        assert len(order_by) == 2
        assert order_by[0] == ("objra", True)  # ASC = True
        assert order_by[1] == ("objdec", False)  # DESC = False
