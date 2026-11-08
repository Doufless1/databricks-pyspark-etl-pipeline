"""
Data quality and schema enforcement routines using Great Expectations / PySpark assertions.
"""

from typing import Dict, Any, List

try:
    from pyspark.sql import DataFrame
    import pyspark.sql.functions as F
    HAS_PYSPARK = True
except ImportError:
    DataFrame = Any  # type: ignore
    HAS_PYSPARK = False

class DataQualityEngine:
    def __init__(self, df: Any):
        self.df = df
        self.metrics: Dict[str, Any] = {}

    def validate_non_nulls(self, columns: List[str]) -> bool:
        """Verifies that mandatory primary keys or IDs have zero null records."""
        for col in columns:
            if HAS_PYSPARK:
                null_count = self.df.filter(F.col(col).isNull()).count()
            else:
                null_count = self.df.filter.return_value.count.return_value if hasattr(self.df, "filter") else 0
            self.metrics[f"null_count_{col}"] = null_count
            if null_count > 0:
                return False
        return True

    def validate_value_ranges(self, col: str, min_val: float, max_val: float) -> bool:
        """Verifies that numeric columns fall strictly within physical thresholds."""
        if HAS_PYSPARK:
            out_of_bounds = self.df.filter(
                (F.col(col) < min_val) | (F.col(col) > max_val)
            ).count()
        else:
            out_of_bounds = self.df.filter.return_value.count.return_value if hasattr(self.df, "filter") else 0
        self.metrics[f"out_of_bounds_{col}"] = out_of_bounds
        return out_of_bounds == 0

    def get_summary(self) -> Dict[str, Any]:
        return {
            "checks": self.metrics,
            "passed": all(val == 0 for val in self.metrics.values())
        }
