import os
import sys
import pytest
from unittest.mock import MagicMock

# Ensure project root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.data_quality import DataQualityEngine

def test_data_quality_non_nulls():
    """Unit test for non-null constraint checking logic."""
    mock_df = MagicMock()
    mock_df.filter.return_value.count.return_value = 0
    engine = DataQualityEngine(mock_df)
    assert engine.validate_non_nulls(["shipment_id", "event_id"]) is True

def test_data_quality_detects_corrupt_data():
    """Unit test ensuring out-of-bounds metrics are flagged."""
    mock_df = MagicMock()
    mock_df.filter.return_value.count.return_value = 14
    engine = DataQualityEngine(mock_df)
    assert engine.validate_value_ranges("latitude", -90.0, 90.0) is False
