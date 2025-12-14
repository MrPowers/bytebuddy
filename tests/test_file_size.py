import pytest
import bytebuddy

def test_parquet_file_size_vehicle():
    uncomp, comp = bytebuddy.parquet_bytes("tests/vehicle.parquet")
    assert uncomp == 5214
    assert comp == 2903


def test_parquet_file_size_customer():
    uncomp, comp = bytebuddy.parquet_bytes("tests/customer.parquet")
    assert uncomp == 2599669
    assert comp == 1740149


def test_parquet_file_size_many():
    uncomp, comp = bytebuddy.parquet_bytes(["tests/vehicle.parquet", "tests/customer.parquet"])
    assert uncomp == 2604883
    assert comp == 1743052

