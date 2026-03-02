"""Two sample benchmarks to compute runtime and memory usage.

For more information on writing benchmarks:
https://asv.readthedocs.io/en/stable/writing_benchmarks.html."""

import pathlib

from hats_tap.adql_to_lsdb import parse_adql_entities


def time_sample1_parse():
    """Time computations are prefixed with 'time'."""
    samples_dir = pathlib.Path(__file__).parent.parent / "src" / "hats_tap" / "samples"

    adql = (samples_dir / "sample1.adql").read_text()
    result = parse_adql_entities(adql)

    assert result["tables"] == ["gaia_dr3.gaia"]
    assert result["columns"] == [
        "source_id",
        "ra",
        "dec",
        "phot_g_mean_mag",
        "phot_variable_flag",
    ]
    assert result["spatial_search"] == {
        "type": "ConeSearch",
        "ra": 270.0,
        "dec": 23.0,
        "radius": 0.25,
    }
    assert result["limits"] == 15
