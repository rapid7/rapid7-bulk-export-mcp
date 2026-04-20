"""
Test to verify the project setup is working correctly.
"""


def test_imports():
    """Verify all required dependencies can be imported."""
    import duckdb
    import fastmcp
    import pyarrow
    import requests

    assert requests is not None
    assert pyarrow is not None
    assert duckdb is not None
    assert fastmcp is not None


def test_pytest_markers():
    """Verify pytest markers are configured correctly."""
    # This test itself verifies that pytest is working
    assert True
