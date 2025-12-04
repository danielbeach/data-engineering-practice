import polars as pl


def cast_data_types(df):
    df = df.with_columns(
        pl.col("ride_id").cast(pl.String),
        pl.col("rideable_type").cast(pl.String),
        pl.col("started_at").str.to_datetime(),
        pl.col("ended_at").str.to_datetime(),
        pl.col("start_station_name").cast(pl.String),
        pl.col("start_station_id").cast(pl.String),
        pl.col("end_station_name").cast(pl.String),
        pl.col("end_station_id").cast(pl.String),
        pl.col("start_lat").cast(pl.Float64),
        pl.col("start_lng").cast(pl.Float64),
        pl.col("end_lat").cast(pl.Float64),
        pl.col("end_lng").cast(pl.Float64),
        pl.col("member_casual").cast(pl.String)
    )
    print(df.schema)
    return df

def metrics_rides_per_week(df):
    df_result = (
        df.with_columns(
            pl.col("started_at").dt.week().alias ("week_number")
        )
        .group_by("week_number")
        .agg(
            pl.col("ride_id").count().alias("total_rides_in_week")
        )
        .with_columns(
            pl.col("total_rides_in_week").mean().alias("avg_rides_per_week"),
            pl.col("total_rides_in_week").min().alias("min_rides_per_week"),
            pl.col("total_rides_in_week").max().alias("max_rides_per_week")
        )
        .sort("week_number")
    )
    print("========================= Average, max, and minimum number of rides per week of the datase =========================")
    print(df_result)

def week_over_week(df):

    df = df.with_columns(
        pl.col("started_at").cast(pl.Date).alias("ride_date")
    )
    #counts rides per day
    daily= (
        df.group_by("ride_date")
          .agg(pl.count().cast(pl.Int32).alias("rides_today"))
          .sort("ride_date")
    )


    daily = daily.with_columns(
        (pl.col("ride_date") - pl.duration(days=7)).alias("last_week_date")
    )

    #self-join
    joined = daily.join(
        daily,
        left_on="last_week_date",
        right_on = "ride_date",
        how="left",
        suffix="_last_week"
    )

    result = joined.with_columns(
        (pl.col("rides_today") - pl.col("rides_today_last_week")).cast(pl.Int32).alias("wow_diff")
    ).select([
        "ride_date",
        "rides_today",
        "rides_today_last_week",
        "wow_diff"
    ])
    print("========================= Week over week metrics =========================")
    print(result)



def main():
    
    #Read csv file
    df = pl.read_csv("./data/202306-divvy-tripdata.csv", infer_schema=False)
    print(df.schema)

    #cast data types
    df = cast_data_types(df)

    #calculates avg, min, max per week
    metrics_rides_per_week(df)

    #calculates week over week diff
    week_over_week(df)
    



if __name__ == "__main__":
    main()
