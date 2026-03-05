"""공통 테스트 fixture"""

import pytest


@pytest.fixture
def sample_records() -> list[dict]:
    """테스트용 샘플 레코드"""
    return [
        {"id": 1, "name": "Alice", "amount": 100.5, "active": True},
        {"id": 2, "name": "Bob", "amount": 200.0, "active": False},
        {"id": 3, "name": "Charlie", "amount": None, "active": True},
    ]


@pytest.fixture
def sample_bytes() -> bytes:
    """테스트용 바이트 데이터"""
    return b'{"id": 1, "name": "test"}' * 100
