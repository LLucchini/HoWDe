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

import warnings

warnings.filterwarnings("ignore")


#######################################
###### SETUP and PRE-PROCESSING  ######
#######################################
def check_and_convert(vars):
    vars_new = []
    for variable in vars:
        # Check if the variable is a list
        if isinstance(variable, list):
            pass
        # Check if the variable is a number (int or float)
        elif isinstance(variable, (int, float)):
            # Convert the number to a list with a single element
            variable = [variable]
        else:
            raise Exception("The variable is neither a list nor a number.")
        vars_new.append(variable)
    return vars_new


def pre_process_stops(spark=None, stops=None, config={}):
    ### GET CORRECT STOPS DATA FORMAT ###
    def format_stop_data(df=stops, config=config):
        """
        INPUT: Stops as pyspark dataframe
            In case start and end times are already in local timestamp set "is_time_local" to "True"
        OUTPUT: formatted stops as pyspark dataframe

        WARNING: following the "Infostop" notation, this function additionally drops all stops labelled as "-1"
            This notation should be avoided to label meaningful stop locations.
        """
        is_time_local = config["is_time_local"]

        df = (
            df.withColumn("loc", F.col("loc").cast(StringType()))
            # remove stops shorter than infostop tmin
            .filter(F.col("loc") != "-1")
        )

        if not is_time_local:
            # >> time updated to local
            df = (
                df
                # transform to local time
                .withColumn(
                    "start",
                    (
                        F.col("start")
                        + F.col("tz_hour_start") * 60 * 60
                        + F.col("tz_minute_start") * 60
                    ).cast(LongType()),
                )
                .withColumn(
                    "end",
                    (
                        F.col("end")
                        + F.col("tz_hour_start") * 60 * 60
                        + F.col("tz_minute_start") * 60
                    ).cast(LongType()),
                )
                .withColumn("start_ts", F.to_timestamp(F.col("start")))
                .withColumn("end_ts", F.to_timestamp(F.col("end")))
                .drop(*["tz_hour_start", "tz_minute_start"])
            )
        else:
            df = (
                df.withColumn("start", F.col("start").cast(LongType()))
                .withColumn("end", F.col("end").cast(LongType()))
                .withColumn("start_ts", F.to_timestamp("start"))
                .withColumn("end_ts", F.to_timestamp("end"))
            )

        return df.select(
            [
                "useruuid",
                "loc",
                "start",
                "end",
                "start_ts",
                "end_ts",
                "country",
            ]
        )

    ### EXPLODING STOPS: splitting stop_locations based on date ###
    def format_stops_within_day(stops=stops):
        ### After this processing every stop location will be contained within a single date
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

    ### CLEAN AND REFORMAT DATES ###
    def clean_stop_data(df=stops, config=config):
        min_stop_t = config["min_stop_t"]
        df = (
            df.withColumn(
                "stop_duration", (F.col("end") - F.col("start"))
            )  # computes stop duration in seconds
            .filter((F.col("stop_duration") > min_stop_t))
            # transform to date-time to correct TZ (unix)
            .withColumn("s_date", F.date_trunc("day", F.col("start_ts")))
            .withColumn("s_yymm", F.date_format(F.col("s_date"), "yyyy-MM"))
            .withColumn("s_hour", F.hour(F.col("start_ts")))
            .withColumn("s_min", F.minute(F.col("start_ts")))
            .withColumn(
                "s_weekend",
                (F.dayofweek(F.col("start_ts")) == 1)
                | (F.dayofweek(F.col("start_ts")) == 7),
            )
            .withColumn("e_date", F.date_trunc("day", F.col("end_ts")))
            .withColumn("e_hour", F.hour(F.col("end_ts")))
            .withColumn("e_min", F.minute(F.col("end_ts")))
            .select(
                [
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
                ]
            )
        )
        return df

    ### RUN FUNCTIONS: Clean and prep data
    if not "country" in stops.columns:
        stops = stops.withColumn("country", F.lit("GL0B"))
    stops = format_stop_data(stops, config)
    stops = format_stops_within_day(stops)
    stops = clean_stop_data(stops, config)
    return stops


############################################################################
################## HOME WORK DETECTION FUNCTIONS ###########################
############################################################################


############################################################################
#################### DYNAMIC HOME-WORK DETECTION - DTU #####################
############################################################################
###############################
#### GET HOURLY TRAJECTORIES ##
###############################
def get_hourly_trajectories(df=None, config={}):
    # Transform df in Hourly traject
    day_window = (
        Window.partitionBy("useruuid", "s_date")
        .orderBy(F.desc("stop_duration"))
        .rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    for elem in list(range(0, 24)):
        tag = str(elem)
        cond1 = F.col("s_hour") <= elem
        cond2 = F.col("e_hour") >= elem
        # For each stop, fill the hours span with the loc
        df = df.withColumn(
            tag, F.when(cond1 & cond2, F.col("loc")).otherwise(F.lit(None))
        )
        # Aggregate at day level (Keeping stops with highest duration when multiple stops happen in an hour, dropping stops <1hour)
        df = df.withColumn(tag, F.first(F.col(tag), ignorenulls=True).over(day_window))

    cols = ["useruuid", "country", "s_yymm", "s_date", "s_weekend"] + list(
        str(i) for i in range(0, 24)
    )
    return df.select(cols).dropDuplicates().orderBy(["useruuid", "s_date"])


###############################
### USEFUL UDF FUNCTIONS H/W ##
###############################
# Get dict of potential locations within hour range * v and visit counts for a day:
@F.udf(MapType(StringType(), IntegerType()))
def dict_loc_visits_daily(*v):
    cnt = dict(Counter(x for x in v if x is not None))
    return cnt if len(cnt) > 0 else None


# Count hours without data per day in range *v
@F.udf(IntegerType())
def cnt_hours_none(*v) -> int:
    cnt = [x for x in v if x is None]
    return len(cnt) if len(cnt) > 0 else None


# Get frequencies dict of potential locations within hour range *v per day
# > Filter at least (hours of home range - bnd_nan) hours of data (o: col_frac)
@F.udf(MapType(StringType(), FloatType()))
def dict_loc_frac_daily(
    dic_f: dict, nan_cnt: int, bnd_nan: float, hour_range: int
) -> dict:
    if dic_f is None:
        return None
    else:
        if nan_cnt is None:
            nan_cnt = 0
        return (
            {k: round(v / (hour_range - nan_cnt), 3) for k, v in dic_f.items()}
            if hour_range - nan_cnt >= bnd_nan
            else None
        )


# Aggregate Daily frequencies with Sliding Windows and retrive combined dict with filter on none ratio (bnd_none)
@F.udf(MapType(StringType(), FloatType()))
def sw_combdic_frac_daily_F(L: List[dict], Nn: List[int], bnd_none: float) -> dict:
    dict_comb = {}
    for d in L:  # collect_list does not pick-up none
        for k, v in d.items():
            dict_comb[k] = dict_comb.get(k, 0) + v
    # daily mean of the freq in the SW, None when no dict picked in the Window or too many None inside the window
    frac_nn = sum(Nn) / len(Nn) if (len(Nn) > 0) else 0
    return (
        {k: (v / len(L)) for k, v in dict_comb.items()}
        if (frac_nn < bnd_none) and (len(L) > 0)
        else None
    )


# Filter-out Homes with visit frequencies bellow threshold (bnd_freq)
@F.udf(MapType(StringType(), FloatType()))
def sw_reddic(d_sw: dict, bnd_freq: float) -> dict:
    try:  # adding try, since d_sw can have None values
        dic = {k: v for k, v in d_sw.items() if v >= bnd_freq}
        return dic if dic else None
    except:
        return None


##########################
## DETECT HOME LOCATION ##
##########################
def find_home(df_th, config):
    range_window = config["range_window"]
    start_hour_day = config["start_hour_day"]
    end_hour_day = config["end_hour_day"]
    data_for_predict = config["data_for_predict"]
    bnd_nan = config["bnd_nan"]
    bnd_none = config["bnd_none_home"]
    bnd_freq = config["bnd_freq_home"]

    # Get potential home location with highest freq.
    @F.udf(StringType())
    def sw_top_loc(d_sw: dict) -> str:
        try:
            return max(d_sw.items(), key=operator.itemgetter(1))[0]
        except:
            return None

    # Define Sliding Windows
    w_u = Window.partitionBy("useruuid")
    days = lambda i: i * 86400
    if data_for_predict:
        w_sw = (
            Window.partitionBy("useruuid")
            .orderBy(F.col("s_date").cast("timestamp").cast("long"))
            .rangeBetween(-days(int(range_window)), 0)
        )
    else:
        w_sw = (
            Window.partitionBy("useruuid")
            .orderBy(F.col("s_date").cast("timestamp").cast("long"))
            .rangeBetween(-days(int(range_window / 2)), days(int(range_window / 2)))
        )

    ## Find Potential Home at Day level -> dict potential loc and their fraction of time
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
    )

    ## Apply Sliding Window  -  aggregating frequencies with daily mean, discard loc with low freq
    df_th = (
        df_th.withColumn(
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
        # Each day has a selected home location or None
        .withColumn("HomPot_loc", sw_top_loc(F.col("ResAgg_dicredSW")))
    )

    ## Drop users with no home detected
    cols = [
        "dicDct",
        "NaNct",
        "ResPot_dicD",
        "NnFlag",
        "ResAgg_dicSW",
        "ResAgg_dicredSW",
        "noHome_42",
    ]
    return (
        df_th.withColumn(
            "noHome_42",
            F.when(F.count(F.col("HomPot_loc")).over(w_u) == 0, F.lit(1)).otherwise(
                F.lit(0)
            ),
        )
        .filter(F.col("noHome_42") == 0)
        .drop(*cols)
    )


##########################
## DETECT WORK LOCATION ##
##########################
def find_work(df_tH, config):
    range_window = config["range_window"]
    start_hour_work = config["start_hour_work"]
    end_hour_work = config["end_hour_work"]
    data_for_predict = config["data_for_predict"]
    bnd_nan = config["bnd_nan"]
    bnd_none = config["bnd_none_work"]
    bnd_freq_h = config["bnd_freqH_work"]
    bnd_freq_dVis = config["bnd_freqD_work"]

    ## Find Potential Work Loc at day level, not home
    @F.udf(MapType(StringType(), FloatType()))
    def dict_notH_daily(dic_f: dict, Hloc: list) -> dict:
        if dic_f is None:
            return None
        else:
            dic = {k: v for k, v in dic_f.items() if (v > 0) and (k not in Hloc)}
            return dic if dic else None

    # Aggregate with the ratio of days a loc was visisted within Sliding Windows, retrive combined dict with filter on none ratio (bnd_none)
    @F.udf(MapType(StringType(), FloatType()))
    def sw_combdic_frac_inWindow_F(
        L: List[dict], Nn: List[int], bnd_none: float
    ) -> dict:
        dict_comb = {}
        for d in L:  # collect_list does not pick-up none
            for k, v in d.items():
                dict_comb[k] = dict_comb.get(k, 0) + 1
        # ratio of days a loc was visisted, None when no dict picked in the Window or too many None inside the window
        return (
            {k: (v / len(L)) for k, v in dict_comb.items()}
            if ((sum(Nn) / len(Nn)) < bnd_none) and (len(L) > 0)
            else None
        )

    # Get top freq. loc from both dicts
    @F.udf(StringType())
    def sw_top_loc_DH(dic_d: dict, dic_h: dict) -> str:
        L = [dic_d, dic_h]
        top_locs = [max(d_sw, key=d_sw.get) for d_sw in L if d_sw]
        # prioritizes ratio of days a loc was visisted (routine) freq location
        return top_locs[0] if top_locs else None

    # Define Sliding Windows
    w_u = Window.partitionBy("useruuid")
    sWD = str(range_window)
    days = lambda i: i * 86400
    if data_for_predict:
        w_sw = (
            Window.partitionBy("useruuid")
            .orderBy(F.col("s_date").cast("timestamp").cast("long"))
            .rangeBetween(-days(int(range_window)), 0)
        )
    else:
        w_sw = (
            Window.partitionBy("useruuid")
            .orderBy(F.col("s_date").cast("timestamp").cast("long"))
            .rangeBetween(-days(int(range_window / 2)), days(int(range_window / 2)))
        )

    ## Find Potential Work at Day level in work-week, excluding home -> dict potential loc and their fraction of time
    work_range = [str(i) for i in range(start_hour_work, end_hour_work + 1)]
    tot_hours = F.lit(len(work_range))
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

    ## Apply Sliding Window -  aggregating hourly frequencies with daily mean, discard loc with low freq (bnd_freq_h)
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

    ## Apply Sliding Window - aggregating locs with ratio of days a loc was visisted within Sliding Windows, discard loc with low freq (bnd_freq_dV)
    df_tH = df_tH.withColumn(
        "EmpPot_dicSW_dVis",
        sw_combdic_frac_inWindow_F(
            F.collect_list(F.col("EmpPot_dicD_nH")).over(w_sw),
            F.collect_list(F.col("NnFlag")).over(w_sw),
            bnd_none,
        ),
    ).withColumn(
        "EmpPot_dicredSW_dVis", sw_reddic(F.col("EmpPot_dicSW_dVis"), bnd_freq_dVis)
    )

    # Get most visisted location for both time-scales: each day has a selected employment location
    df_tH = df_tH.withColumn(
        "EmpPot_loc",
        sw_top_loc_DH(F.col("EmpPot_dicredSW_dVis"), F.col("EmpPot_dicredSW_h")),
    )

    cols = [
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
    return df_tH.drop(*cols).drop(*traj_cols)


##############################
####### GET OUTPUT FORMAT ####
##############################
## KEEP STOP LEVEL ##
def get_stop_level(df_stops, df_traj):
    w_u = Window.partitionBy("useruuid").orderBy(F.desc("s_date"))
    w_u_unbounded = w_u.rowsBetween(
        Window.unboundedPreceding, Window.unboundedFollowing
    )

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

    hw_s = df_traj.select(
        ["useruuid", "s_date", "HomPot_loc", "EmpPot_loc"]
    ).dropDuplicates()
    stops_hw = df_stops.join(hw_s, on=["useruuid", "s_date"], how="left")
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


## REDUCE DATAFRAME TO CHANGE LEVEL ##
def get_change_level(df):
    def replace_null(column):
        return F.when(column.isNull(), F.lit("XXX")).otherwise(column)

    def flag_change(column, w_u):
        return F.when((F.lag(column, -1).over(w_u) == column), F.lit(0)).otherwise(
            F.lit(1)
        )

    w_u = Window.partitionBy("useruuid").orderBy(F.desc("s_date"))
    w_cum_before = (
        Window.partitionBy(["useruuid"])
        .orderBy("s_date")
        .rangeBetween(Window.unboundedPreceding, 0)
    )
    w_bk = (
        Window.partitionBy(["useruuid", "chg_block"])
        .orderBy("s_date")
        .rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    df = (
        df
        ## Remove all dates where no Home was detected
        .select(["useruuid", "s_date", "HomPot_loc", "EmpPot_loc"])
        .filter(df.HomPot_loc.isNotNull())
        .orderBy(["useruuid", "s_date"])
        ## Flag change of status:
        # Replace null to avoid equality errors (null == null is False)
        .withColumn("temp_HLoc", replace_null(F.col("HomPot_loc")))
        .withColumn("temp_ELoc", replace_null(F.col("EmpPot_loc")))
        # Flag changes based on prev.row
        .withColumn("flag_HLoc", flag_change(F.col("temp_HLoc"), w_u))
        .withColumn("flag_ELoc", flag_change(F.col("temp_ELoc"), w_u))
        .withColumn(
            "chg_flag",
            F.when(
                (F.col("flag_HLoc") == F.lit(0)) & (F.col("flag_ELoc") == F.lit(0)),
                F.lit(0),
            ).otherwise(F.lit(1)),
        )
        ## Define Change blocks and start/end of block
        .withColumn("chg_block", F.sum("chg_flag").over(w_cum_before))
        .withColumn("start_date", F.first(F.col("s_date")).over(w_bk))
        .withColumn("end_date", F.last(F.col("s_date")).over(w_bk))
        ## Transform dates to unix
        .withColumn("start_date", F.unix_timestamp(F.col("start_date")))
        .withColumn("end_date", F.unix_timestamp(F.col("end_date")))
        ## Keep only change level
        .select(["useruuid", "start_date", "end_date", "HomPot_loc", "EmpPot_loc"])
        .withColumnRenamed("HomPot_loc", "detect_H_loc")
        .withColumnRenamed("EmpPot_loc", "detect_W_loc")
        .dropDuplicates()
    )
    return df
