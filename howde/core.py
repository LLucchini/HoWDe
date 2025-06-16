from datetime import datetime, timezone, timedelta
from dateutil import tz

import numpy as np
import pandas as pd
import os
import time
import operator
from typing import List
from collections import Counter

import itertools
from tqdm import tqdm

import pyspark.sql.functions as F
from pyspark.sql.functions import (
    lag,
    col,
    countDistinct,
    to_timestamp,
    lit,
    from_unixtime,
    udf,
    pandas_udf,
    PandasUDFType,
)
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql import SparkSession


# -------------------------------------------------------------------
# Stop Preprocessing and Hourly Trajectory Construction
# -------------------------------------------------------------------
# --- Pre-processing functions ---
def pre_process_stops(stops=None, config={}):
    """
    Preprocess raw stop-level location data for home/work detection.

    This function performs the following steps:
        1. Filters out invalid stops (e.g., Infostop's "-1" locations).
        2. Converts timestamps to local time if needed.
        3. Splits stop events that span multiple days into separate records.
        4. Computes additional time-based features (e.g., hour, weekday, duration).

    Parameters
    ----------
    stops : pyspark.sql.DataFrame
        Input stop-level data. Must contain at least the following columns:
        - 'useruuid' : unique user identifier
        - 'loc'      : stop location ID
        - 'start'    : Unix timestamp (start of stop)
        - 'end'      : Unix timestamp (end of stop)
        - Optionally: 'tz_hour_start', 'tz_minute_start' if times are in UTC
        - Optionally: 'country' (if not provided, a dummy "GL0B" value is added)

    config : dict
        Configuration dictionary. Expected keys include:
        - 'is_time_local' : bool, whether 'start'/'end' are already in local time
        - 'min_stop_t'    : minimum stop duration in seconds

    Returns
    -------
    pyspark.sql.DataFrame
        Cleaned and formatted stop-level data, with additional columns:
        - 'start_ts', 'end_ts' : timestamp versions of start/end
        - temporal features: 's_date', 's_hour', 's_min', 's_weekend', etc.
        - stop duration and split info for multi-day events
    """

    def format_stop_data(df=stops, config=config):
        """
        Step 1: Format timestamps, cast location types, remove invalid stops.

        Notes:
        - Drops all stops with loc == "-1" (Infostop convention).
        - Adjusts start/end times to local time if `is_time_local` is False.
        - Creates 'start_ts' and 'end_ts' timestamp columns.
        """
        is_time_local = config["is_time_local"]

        df = df.withColumn("loc", F.col("loc").cast(StringType())).filter(
            F.col("loc") != "-1"
        )  # Drop invalid Infostop locations

        if not is_time_local:
            df = (
                df.withColumn(
                    "start",
                    (
                        F.col("start")
                        + F.col("tz_hour_start") * 3600
                        + F.col("tz_minute_start") * 60
                    ).cast(LongType()),
                )
                .withColumn(
                    "end",
                    (
                        F.col("end")
                        + F.col("tz_hour_start") * 3600
                        + F.col("tz_minute_start") * 60
                    ).cast(LongType()),
                )
                .withColumn("start_ts", F.to_timestamp("start"))
                .withColumn("end_ts", F.to_timestamp("end"))
                .drop("tz_hour_start", "tz_minute_start")
            )
        else:
            df = (
                df.withColumn("start", F.col("start").cast(LongType()))
                .withColumn("end", F.col("end").cast(LongType()))
                .withColumn("start_ts", F.to_timestamp("start"))
                .withColumn("end_ts", F.to_timestamp("end"))
            )

        return df.select(
            "useruuid", "loc", "start", "end", "start_ts", "end_ts", "country"
        )

    def format_stops_within_day(stops=stops):
        """
        Step 2: Split stops that span multiple days into daily segments.

        Adds:
        - 'split_index' : unique identifier for the original stop
        - 'start_ts', 'end_ts' : updated timestamps for each daily segment
        - 'split_start' : flag indicating if this is a split (non-midnight) start
        """
        stops = (
            stops.withColumn(
                "splitted",
                F.size(
                    F.sequence(
                        F.date_trunc("day", F.col("start_ts")),
                        F.date_trunc("day", F.col("end_ts")),
                    )
                )
                > 1,
            )
            .withColumn(
                "split_index",
                F.concat(
                    F.col("loc").cast(StringType()),
                    F.lit("-"),
                    F.col("start_ts").cast(StringType()),
                ),
            )
            .withColumn(
                "date",
                F.explode(
                    F.sequence(
                        F.date_trunc("day", F.col("start_ts")),
                        F.date_trunc("day", F.col("end_ts")),
                    )
                ),
            )
            .withColumn(
                "s",
                F.when(F.col("date") < F.col("start_ts"), F.col("start_ts")).otherwise(
                    F.col("date")
                ),
            )
            .withColumn(
                "e",
                F.when(
                    F.date_sub("date", -1) > F.col("end_ts"), F.col("end_ts")
                ).otherwise(
                    F.col("date") + F.expr("INTERVAL 23 HOURS 59 minutes 59 seconds")
                ),
            )
            .drop("start", "end", "end_ts", "start_ts", "date")
            .withColumnRenamed("s", "start_ts")
            .withColumnRenamed("e", "end_ts")
            .withColumn("start", F.col("start_ts").cast(LongType()))
            .withColumn("end", F.col("end_ts").cast(LongType()))
        )

        # Flag if this segment ends at 23:59:59 and did not start at midnight
        stops = (
            stops.withColumn(
                "end_is_23",
                F.col("end_ts")
                == (
                    F.date_trunc("day", F.col("end_ts"))
                    + F.expr("INTERVAL 23 HOURS 59 minutes 59 seconds")
                ),
            )
            .withColumn(
                "start_not_00",
                F.col("start_ts") != F.date_trunc("day", F.col("start_ts")),
            )
            .withColumn(
                "split_start",
                F.col("end_is_23") & F.col("start_not_00") & F.col("splitted"),
            )
        )
        return stops

    def clean_stop_data(df=stops, config=config):
        """
        Step 3: Clean data, filter short stops, and extract temporal features.

        Adds:
        - Duration, start/end hour/minute, weekend flag, etc.
        """
        min_stop_t = config["min_stop_t"]

        df = (
            df.withColumn("stop_duration", F.col("end") - F.col("start"))
            .filter(F.col("stop_duration") > min_stop_t)
            .withColumn("s_date", F.date_trunc("day", F.col("start_ts")))
            .withColumn("s_yymm", F.date_format("s_date", "yyyy-MM"))
            .withColumn("s_hour", F.hour("start_ts"))
            .withColumn("s_min", F.minute("start_ts"))
            .withColumn(
                "s_weekend",
                (F.dayofweek("start_ts") == 1) | (F.dayofweek("start_ts") == 7),
            )
            .withColumn("e_date", F.date_trunc("day", F.col("end_ts")))
            .withColumn("e_hour", F.hour("end_ts"))
            .withColumn("e_min", F.minute("end_ts"))
            .select(
                "useruuid",
                "loc",
                "start_ts",
                "end_ts",
                "start",
                "end",
                "s_date",
                "s_yymm",
                "s_hour",
                "s_min",
                "s_weekend",
                "e_date",
                "e_hour",
                "e_min",
                "stop_duration",
                "country",
                "split_index",
                "split_start",
            )
        )
        return df

    # Run all steps in sequence
    if "country" not in stops.columns:
        stops = stops.withColumn("country", F.lit("GL0B"))
    stops = format_stop_data(stops, config)
    stops = format_stops_within_day(stops)
    stops = clean_stop_data(stops, config)

    return stops


# --- Trajectory Construction ---
def get_hourly_trajectories(df=None, config={}):
    """
    Convert preprocessed stop-level data into hourly location trajectories.

    For each user and each day:
        - Splits the day into 24 hourly slots (0 to 23).
        - Handles overlapping stops by keeping the one with the longest duration for each hour.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Input stop-level DataFrame, preprocessed via `pre_process_stops()`.
        Must include:
            - 'useruuid', 'loc', 's_date', 's_hour', 'e_hour', 'stop_duration', 's_yymm', 's_weekend', 'country'

    config : dict
        Currently unused, included for compatibility with other pipeline steps.

    Returns
    -------
    pyspark.sql.DataFrame
        One row per (user, day), with columns:
            - 'useruuid', 'country', 's_yymm', 's_date', 's_weekend'
            - Columns "0" through "23", each representing the dominant location at that hour.
    """
    # Define daily window per user to find longest stop
    day_window = (
        Window.partitionBy("useruuid", "s_date")
        .orderBy(F.desc("stop_duration"))
        .rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    # Fill hourly slots 0 to 23 with location labels
    for hour in range(24):
        hour_col = str(hour)
        within_hour = (F.col("s_hour") <= hour) & (F.col("e_hour") >= hour)

        df = df.withColumn(
            hour_col, F.when(within_hour, F.col("loc")).otherwise(F.lit(None))
        )

        # If multiple stops overlap the hour, keep the one with longest duration
        df = df.withColumn(
            hour_col, F.first(F.col(hour_col), ignorenulls=True).over(day_window)
        )

    # Select only final hourly trajectory columns
    cols = ["useruuid", "country", "s_yymm", "s_date", "s_weekend"] + [
        str(i) for i in range(24)
    ]

    return df.select(cols).dropDuplicates().orderBy(["useruuid", "s_date"])


# -------------------------------------------------------------------
# UDFs for Home/Work detection
# -------------------------------------------------------------------


# --- UDFs for Home and Work detection ---
@F.udf(MapType(StringType(), IntegerType()))
def dict_loc_visits_daily(*v):
    """
    Count how many times each location appears across hourly slots in a day.
    Ignores None values.
    Returns a dict: {location_id: visit_count}
    """
    cnt = dict(Counter(x for x in v if x is not None))
    return cnt if cnt else None


@F.udf(IntegerType())
def cnt_hours_none(*v):
    """
    Count how many hours (in a day) have no location data.
    Returns an integer (0 if all hours are covered).
    """
    cnt = [x for x in v if x is None]
    return len(cnt) if cnt else None


@F.udf(MapType(StringType(), FloatType()))
def dict_loc_frac_daily(dic_f, nan_cnt, bnd_nan, hour_range):
    """
    For a day, compute the fraction of time spent at each location,
    normalized by the number of hours with data.

    Returns None if there are too many missing hours (based on bnd_nan).
    """
    if dic_f is None:
        return None
    if nan_cnt is None:
        nan_cnt = 0
    if hour_range - nan_cnt >= bnd_nan:
        return {k: round(v / (hour_range - nan_cnt), 3) for k, v in dic_f.items()}
    else:
        return None


@F.udf(MapType(StringType(), FloatType()))
def sw_combdic_frac_daily_F(L, Nn, bnd_none):
    """
    Combine location frequency dictionaries over a sliding window.

    Returns average frequency per location only if the fraction of days
    with missing data is below `bnd_none`.
    """
    dict_comb = {}
    for d in L:
        for k, v in d.items():
            dict_comb[k] = dict_comb.get(k, 0) + v
    frac_nn = sum(Nn) / len(Nn) if Nn else 0
    return (
        {k: v / len(L) for k, v in dict_comb.items()}
        if (frac_nn < bnd_none) and L
        else None
    )


@F.udf(MapType(StringType(), FloatType()))
def sw_reddic(d_sw, bnd_freq):
    """
    Remove locations with frequency below `bnd_freq`.
    Returns a filtered dictionary or None.
    """
    try:
        dic = {k: v for k, v in d_sw.items() if v >= bnd_freq}
        return dic if dic else None
    except:
        return None


# --- UDFs for Home detection ---
@F.udf(StringType())
def sw_top_loc(d_sw):
    """
    Select the location with the highest frequency from a dict.
    Returns location ID or None.
    """
    try:
        return max(d_sw.items(), key=operator.itemgetter(1))[0]
    except:
        return None


# --- UDFs for Work detection ---
@F.udf(MapType(StringType(), FloatType()))
def dict_notH_daily(dic_f: dict, Hloc: list) -> dict:
    """
    Filter out home locations from the daily frequency dictionary.

    Parameters
    ----------
    dic_f : dict
        Daily location frequency dictionary {loc_id: freq}.
    Hloc : list
        List of known home locations for the user.

    Returns
    -------
    dict or None
        Dictionary of non-home locations with non-zero frequency,
        or None if empty.
    """
    if dic_f is None:
        return None
    dic = {k: v for k, v in dic_f.items() if (v > 0) and (k not in Hloc)}
    return dic if dic else None


@F.udf(MapType(StringType(), FloatType()))
def sw_combdic_frac_inWindow_F(L: list, Nn: list, bnd_none: float) -> dict:
    """
    Aggregate location visit counts across a sliding window and compute
    the ratio of days each location was visited.

    Parameters
    ----------
    L : list of dict
        List of daily frequency dicts across the window.
    Nn : list of int
        List of flags indicating whether the day is missing (1 if missing).
    bnd_none : float
        Maximum allowed fraction of missing days in the window.

    Returns
    -------
    dict or None
        {loc_id: fraction_of_days_visited}, or None if too many missing days.
    """
    dict_comb = {}
    for d in L:
        for k, v in d.items():
            dict_comb[k] = dict_comb.get(k, 0) + 1
    frac_missing = sum(Nn) / len(Nn) if len(Nn) > 0 else 0
    return (
        {k: v / len(L) for k, v in dict_comb.items()}
        if frac_missing < bnd_none and L
        else None
    )


@F.udf(StringType())
def sw_top_loc_DH(dic_d: dict, dic_h: dict) -> str:
    """
    Return the top location from two sliding window dictionaries.

    Prioritizes:
    1. Locations that appear consistently across days (routine)
    2. Locations with high frequency (backup if first dict is missing)

    Parameters
    ----------
    dic_d : dict
        Ratio-of-days dict: how often a location is visited (temporal regularity).
    dic_h : dict
        Frequency-based dict: average fraction of hours spent at location.

    Returns
    -------
    str or None
        The top location ID from available sources, or None if neither is valid.
    """
    top_locs = [max(d, key=d.get) for d in [dic_d, dic_h] if d]
    return top_locs[0] if top_locs else None


# -------------------------------------------------------------------
# Home Location Detection
# -------------------------------------------------------------------


def find_home(df_th, config):
    """
    Detect home location for each user using sliding windows of daily visit data.

    For each user:
        1. Extract hourly visits during 'home hours' (outside working hours).
        2. Aggregate visits per day into location-frequency dictionaries.
        3. Use a sliding window to smooth and filter daily patterns.
        4. Select the location with highest average frequency as home.

    Parameters
    ----------
    df_th : pyspark.sql.DataFrame
        Hourly trajectory data produced by `get_hourly_trajectories()`.
        Must include hourly columns ("0", ..., "23").

    config : dict
        Configuration dictionary with:
            - range_window_home : int, size of sliding window (in days)
            - start_hour_day / end_hour_day : int, defines "home hours"
            - data_for_predict : bool, if True only uses past data in the window
            - dhn : float, max number of missing hours per day
            - dn_H : float, max fraction of nulls in the window
            - bnd_freq_home : float, min average frequency to be considered home

    Returns
    -------
    pyspark.sql.DataFrame
        Original DataFrame with new column:
            - 'HomPot_loc': detected home location per day (can be None if no home found)
        Drops users with no home detected in the entire window (tracked via 'noHome_42').
    """
    range_window = config["range_window_home"]
    start_hour_day = config["start_hour_day"]
    end_hour_day = config["end_hour_day"]
    data_for_predict = config["data_for_predict"]
    bnd_nan = config["dhn"]
    bnd_none = config["dn_H"]
    bnd_freq = config["hf_H"]

    # Define sliding window range
    w_u = Window.partitionBy("useruuid")
    days = lambda i: i * 86400

    if data_for_predict:
        w_sw = (
            Window.partitionBy("useruuid")
            .orderBy(F.col("s_date").cast("timestamp").cast("long"))
            .rangeBetween(-days(int(range_window)), 0)
        )
    else:
        half = int(range_window / 2)
        w_sw = (
            Window.partitionBy("useruuid")
            .orderBy(F.col("s_date").cast("timestamp").cast("long"))
            .rangeBetween(-days(half), days(half))
        )

    # Select home hours: early morning and late evening
    home_range = [str(i) for i in range(0, start_hour_day + 1)] + [
        str(i) for i in range(end_hour_day, 24)
    ]
    tot_hours = F.lit(len(home_range))

    df_th = (
        df_th.withColumn("dicDct", dict_loc_visits_daily(*home_range))
        .withColumn("NaNct", cnt_hours_none(*home_range))
        .withColumn(
            "ResPot_dicD",
            dict_loc_frac_daily(F.col("dicDct"), F.col("NaNct"), bnd_nan, tot_hours),
        )
        .withColumn(
            "NnFlag",
            F.when(F.col("ResPot_dicD").isNull(), F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn(
            "ResAgg_dicSW",
            sw_combdic_frac_daily_F(
                F.collect_list(F.col("ResPot_dicD")).over(w_sw),
                F.collect_list(F.col("NnFlag")).over(w_sw),
                bnd_none,
            ),
        )
        .withColumn("ResAgg_dicredSW", sw_reddic(F.col("ResAgg_dicSW"), bnd_freq))
        .withColumn("HomPot_loc", sw_top_loc(F.col("ResAgg_dicredSW")))
    )

    return (
        df_th.withColumn(
            "noHome_W",
            F.when(F.count(F.col("HomPot_loc")).over(w_u) == 0, F.lit(1)).otherwise(
                F.lit(0)
            ),
        )
        .filter(F.col("noHome_W") == 0)
        .drop(
            "dicDct",
            "NaNct",
            "ResPot_dicD",
            "NnFlag",
            "ResAgg_dicSW",
            "ResAgg_dicredSW",
            "noHome_W",
        )
    )


# -------------------------------------------------------------------
# Work Location Detection
# -------------------------------------------------------------------


def find_work(df_tH, config):
    """
    Detect work location for each user using weekday behavior during working hours.

    This function uses a sliding window approach over weekdays to:
        1. Identify likely work locations based on hourly presence (excluding home).
        2. Aggregate visit patterns using two strategies:
            - Average hourly fraction (temporal stability).
            - Ratio of days with visits (temporal regularity).
        3. Select the most consistent and frequently visited non-home location as work.

    Parameters
    ----------
    df_tH : pyspark.sql.DataFrame
        Output from `find_home()` with daily hourly trajectories and home labels.

    config : dict
        Configuration with the following keys:
            - range_window_work : int, size of sliding window
            - start_hour_work / end_hour_work : int, define work hours
            - data_for_predict : bool, use past-only or symmetric window
            - dhn : float, max missing hours per day
            - dn_W : float, max missing days in window
            - hf_W : float, min avg hourly fraction
            - df_W : float, min ratio of days for routine presence

    Returns
    -------
    pyspark.sql.DataFrame
        Input DataFrame with new column 'EmpPot_loc' (detected work location),
        with intermediate and hourly trajectory columns dropped.
    """
    range_window = config["range_window_work"]
    start_hour_work = config["start_hour_work"]
    end_hour_work = config["end_hour_work"]
    data_for_predict = config["data_for_predict"]
    bnd_nan = config["dhn"]
    bnd_none = config["dn_W"]
    bnd_freq_h = config["hf_W"]
    bnd_freq_dVis = config["df_W"]

    # Define work hours
    work_range = [str(i) for i in range(start_hour_work, end_hour_work + 1)]
    tot_hours = F.lit(len(work_range))

    # Define sliding window
    days = lambda i: i * 86400
    w_u = Window.partitionBy("useruuid")
    if data_for_predict:
        w_sw = (
            Window.partitionBy("useruuid")
            .orderBy(F.col("s_date").cast("timestamp").cast("long"))
            .rangeBetween(-days(int(range_window)), 0)
        )
    else:
        half = int(range_window / 2)
        w_sw = (
            Window.partitionBy("useruuid")
            .orderBy(F.col("s_date").cast("timestamp").cast("long"))
            .rangeBetween(-days(half), days(half))
        )

    # Step 1: Get work-hour location fractions for weekdays only
    df_tH = (
        df_tH.withColumn(
            "dicDct",
            F.when(
                F.col("s_weekend") == False, dict_loc_visits_daily(*work_range)
            ).otherwise(None),
        )
        .withColumn("NaNct", cnt_hours_none(*work_range))
        .withColumn(
            "EmpPot_dicD",
            F.when(
                F.col("s_weekend") == False,
                dict_loc_frac_daily(
                    F.col("dicDct"), F.col("NaNct"), bnd_nan, tot_hours
                ),
            ).otherwise(None),
        )
        .withColumn(
            "EmpPot_dicD_nH",
            F.when(
                F.col("s_weekend") == False,
                dict_notH_daily(
                    F.col("EmpPot_dicD"), F.collect_set("HomPot_loc").over(w_u)
                ),
            ).otherwise(None),
        )
    )

    # Step 2a: Sliding window on average hourly fraction
    df_tH = (
        df_tH.withColumn(
            "NnFlag",
            F.when(
                (F.col("s_weekend") == False) & (F.col("EmpPot_dicD_nH").isNull()),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "EmpPot_dicSW_h",
            sw_combdic_frac_daily_F(
                F.collect_list(F.col("EmpPot_dicD_nH")).over(w_sw),
                F.collect_list(F.col("NnFlag")).over(w_sw),
                bnd_none,
            ),
        )
        .withColumn("EmpPot_dicredSW_h", sw_reddic(F.col("EmpPot_dicSW_h"), bnd_freq_h))
    )

    # Step 2b: Sliding window on ratio of days visited
    df_tH = df_tH.withColumn(
        "EmpPot_dicSW_dVis",
        sw_combdic_frac_inWindow_F(
            F.collect_list("EmpPot_dicD_nH").over(w_sw),
            F.collect_list("NnFlag").over(w_sw),
            bnd_none,
        ),
    ).withColumn(
        "EmpPot_dicredSW_dVis",
        sw_reddic(F.col("EmpPot_dicSW_dVis"), bnd_freq_dVis),
    )

    # Step 3: Pick top location prioritizing temporal regularity
    df_tH = df_tH.withColumn(
        "EmpPot_loc",
        sw_top_loc_DH(F.col("EmpPot_dicredSW_dVis"), F.col("EmpPot_dicredSW_h")),
    )

    # Drop intermediate and hourly columns
    drop_cols = [
        "dicDct",
        "NaNct",
        "EmpPot_dicD",
        "EmpPot_dicD_nH",
        "NnFlag",
        "EmpPot_dicSW_h",
        "EmpPot_dicredSW_h",
        "EmpPot_dicSW_dVis",
        "EmpPot_dicredSW_dVis",
    ]
    traj_cols = [str(i) for i in range(0, 24)]

    return df_tH.drop(*drop_cols).drop(*traj_cols)


# -------------------------------------------------------------------
# Output Formatting
# -------------------------------------------------------------------


# --- Stop-Level Output ---
def get_stop_level(df_stops, df_traj):
    """
    Attach detected home/work locations to stop-level data.

    Parameters
    ----------
    df_stops : pyspark.sql.DataFrame
        Stop-level input with one row per stop event.
    df_traj : pyspark.sql.DataFrame
        Daily user-level trajectory data with 'HomPot_loc' and 'EmpPot_loc'.

    Returns
    -------
    pyspark.sql.DataFrame
        One row per stop, with added columns:
        - 'detect_H_loc': Detected home location
        - 'detect_W_loc': Detected work location
        - 'location_type': 'H', 'W', or 'O' (other)
    """
    cols = [
        "useruuid",
        "country",
        "loc",
        "date",
        "start",
        "end",
        "stop_duration",
        "location_type",
        "HomPot_loc",
        "EmpPot_loc",
    ]

    # Merge in detected H/W locations by date and user
    hw_s = df_traj.select(
        ["useruuid", "s_date", "HomPot_loc", "EmpPot_loc"]
    ).dropDuplicates()

    stops_hw = df_stops.join(hw_s, on=["useruuid", "s_date"], how="left")

    # Assign stop as home (H), work (W), or other (O)
    stops_hw = (
        stops_hw.withColumn(
            "location_type",
            F.when(F.col("loc") == F.col("HomPot_loc"), "H").otherwise(
                F.when(F.col("loc") == F.col("EmpPot_loc"), "W").otherwise("O")
            ),
        )
        .withColumnRenamed("s_date", "date")
        .select(cols)
        .withColumnRenamed("HomPot_loc", "detect_H_loc")
        .withColumnRenamed("EmpPot_loc", "detect_W_loc")
    )

    return stops_hw


# === Change-Level Output ===
def get_change_level(df):
    """
    Reduce output to only days when home or work location changes.

    This function compares each user's home/work location to the previous day,
    flags changes, groups consecutive unchanged days into 'blocks', and reports
    start/end dates and locations for each block.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Daily trajectory data with 'HomPot_loc' and 'EmpPot_loc'.

    Returns
    -------
    pyspark.sql.DataFrame
        One row per change event, with:
        - useruuid
        - start_date (unix)
        - end_date (unix)
        - detect_H_loc, detect_W_loc
    """

    def replace_null(column):
        # Replace nulls with dummy string to enable comparison (null == null → False)
        return F.when(column.isNull(), F.lit("XXX")).otherwise(column)

    def flag_change(column, w_u):
        # Compare with previous row to flag changes
        return F.when((F.lag(column, -1).over(w_u) == column), F.lit(0)).otherwise(
            F.lit(1)
        )

    # Windows for comparisons and grouping
    w_u = Window.partitionBy("useruuid").orderBy(F.desc("s_date"))
    w_cum_before = (
        Window.partitionBy("useruuid")
        .orderBy("s_date")
        .rangeBetween(Window.unboundedPreceding, 0)
    )
    w_bk = (
        Window.partitionBy("useruuid", "chg_block")
        .orderBy("s_date")
        .rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    df = (
        df
        # Step 1: Filter out days with no home detected
        .select(["useruuid", "s_date", "HomPot_loc", "EmpPot_loc"])
        .filter(F.col("HomPot_loc").isNotNull())
        .orderBy(["useruuid", "s_date"])
        # Step 2: Flag changes in home/work using lag() comparison
        .withColumn("temp_HLoc", replace_null(F.col("HomPot_loc")))
        .withColumn("temp_ELoc", replace_null(F.col("EmpPot_loc")))
        .withColumn("flag_HLoc", flag_change(F.col("temp_HLoc"), w_u))
        .withColumn("flag_ELoc", flag_change(F.col("temp_ELoc"), w_u))
        .withColumn(
            "chg_flag",
            F.when(
                (F.col("flag_HLoc") == 0) & (F.col("flag_ELoc") == 0), F.lit(0)
            ).otherwise(F.lit(1)),
        )
        # Step 3: Assign change blocks based on consecutive H/W detected
        .withColumn("chg_block", F.sum("chg_flag").over(w_cum_before))
        .withColumn("start_date", F.first("s_date").over(w_bk))
        .withColumn("end_date", F.last("s_date").over(w_bk))
        # Step 4: Convert date to unix format
        .withColumn("start_date", F.unix_timestamp("start_date"))
        .withColumn("end_date", F.unix_timestamp("end_date"))
        # Step 5: Return change-block-level output
        .select("useruuid", "start_date", "end_date", "HomPot_loc", "EmpPot_loc")
        .withColumnRenamed("HomPot_loc", "detect_H_loc")
        .withColumnRenamed("EmpPot_loc", "detect_W_loc")
        .dropDuplicates()
    )

    return df
