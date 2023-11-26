import polars as pl
from main import main 
import pytest

def test_main():
    result = """shape: (6, 3)
┌────────────┬───────────────┬────────┐
│ started_at ┆ rides_per_day ┆ Сhange │
│ ---        ┆ ---           ┆ ---    │
│ date       ┆ u32           ┆ i64    │
╞════════════╪═══════════════╪════════╡
│ 2023-06-02 ┆ 1             ┆ null   │
│ 2023-06-09 ┆ 1             ┆ 0      │
│ 2023-06-16 ┆ 2             ┆ 1      │
│ 2023-06-17 ┆ 1             ┆ null   │
│ 2023-06-23 ┆ 3             ┆ 1      │
│ 2023-06-30 ┆ 2             ┆ -1     │
└────────────┴───────────────┴────────┘"""
    result_df = main()

    assert result == str(result_df)


if __name__ == "__main__":
    pytest.main()