import polars as pl
from datetime import timedelta


def main():
    # filepath = r'./data/202306-divvy-tripdata.csv'
    filepath = r'./testdata/test.csv'
    df = pl.read_csv(filepath, ignore_errors=True, try_parse_dates=True)
    q1 = (df
        .lazy()
        .sort("started_at")
        .with_columns(pl.col("started_at")
        .cast(pl.Date))
        .group_by("started_at")
        .agg(pl.col("started_at")
        .count()
        .alias('rides_per_day')))
    lazy_df = q1.collect()
    print(q1.collect())
    q2 = (df
        .lazy()
        .sort("started_at")
        .group_by_dynamic("started_at", every="1w")
        .agg(pl.col("started_at")
        .count()
        .alias('average_per_week')))
    print(q2.collect().mean(), q2.collect().max(), q2.collect().min())
    date_range = pl.date_range(lazy_df["started_at"].min(), lazy_df["started_at"].max(), timedelta(days=1), eager=True)
    df_dates= pl.DataFrame({
        "started_at": date_range,
    })
    df = df_dates.join(lazy_df, on="started_at", how="left").with_columns(Сhange=pl.col('rides_per_day').diff(n=7)).filter(
        pl.col("started_at").is_in(lazy_df["started_at"])
    )
    print(df)
    return df


if __name__ == "__main__":
    main()
