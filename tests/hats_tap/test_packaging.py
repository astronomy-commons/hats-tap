import hats_tap


def test_version():
    """Check to see that we can get the package version"""
    assert hats_tap.__version__ is not None
