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


from .HoWDe_utils import *

import warnings

warnings.filterwarnings("ignore")


# #####################################################
# ################# HOME WORK PIPELINE ################
# #####################################################
def HoWDe_compute(df_stops=None, config={}, stops_output=True, verbose=False):
    ### Cleaning and preparing data
    if verbose:
        print(" >>> stops pre-processed")
    ### Computing key quantities
    df_traj = get_hourly_trajectories(df_stops, config=config)
    df_traj = find_home(df_traj, config=config)
    df_f = find_work(df_traj, config=config)
    if verbose:
        print(" >>> home/works detected")

    ## Select output style: stops or change level
    if stops_output:
        df_f = get_stop_level(df_stops, df_f)#.drop(*["HomPot_loc", "EmpPot_loc"])
    else:
        df_f = get_change_level(df_f)

    if verbose:
        print(f" >>> output formatted as stops: {stops_output}")
    return df_f


# #####################################################


# #####################################################
# ############## MAIN HoWDe Handling ##################
# #####################################################
def HoWDe_labelling(
    input_data = None,
    spark = None,
    HW_PATH = "./",
    SAVE_PATH = None,
    SAVE_NAME = "",
    save_multiple = False,
    edit_config_default = None,
    range_window = 42,
    dhn = 3,    # bnd_none_day
    dn_H = 0.7, # bnd_none_home
    dn_W = 0.5, # bnd_none_work
    hf_H = 0.7, # range_freq_home
    hf_W = 0.4, # range_freq_work_h
    df_W = 0.6, # range_freq_work_d
    stops_output = True,
    verbose = False,
    driver_memory = 250,
):
    """
    Perform Home and Work Detection (HoWDe)
        Outlook:
            1. This function first load (or take as input) the stop location dataset you wish to label
            2. It checks if all mandatory fields are present (or need renaming):
                - "useruuid"        : unique identifier of each user (if "country" not present otherwise useruuid + country should be unique)
                - "loc"             : stop location unique identifier (unique by useruuid) - WARNING: avoid using "-1" loc labels to indentify relevant location information. following the "Infostop" notation system, those stops are automatically dropped
                - "start"           : stop location start time / Unix Timestamp (will be treated as LongType)
                - "end"             : stop location end time / Unix Timestamp (will be treated as LongType)
                - "tz_hour_start"   : local hourly timezone offset (mandatory only if "start" / "end" Timestamp is not already in local time)
                - "tz_minute_start" : local minute timezone offset (mandatory only if "start" / "end" Timestamp is not already in local time)
                - ("country")       : not mandatory, if not present a dummy column F.lit("GLOB") will be added
            3. Run the labelling procedure (if lists of HoWDe parameters are provided all combinations of parameters will be explored)
               (access saving options through the "save_multiple" option)
       Output:
           Returns a pyspark.sql.dataframe.DataFrame with the additional column:
               1. "location_type"   : reporting the output location type as detected by the labelling algorithm; locations are only labelled as "H" or "W" if within the window the passed all the thresholds.

    Parameters
    ----------
    input_data : pyspark.sql.dataframe.DataFrame, default=None
        Preloaded data containing all mandatory fields in "spark" pyspark.sql.session.SparkSession.
        If not provided the data will be loaded directly using the "HW_PATH" directory
    spark : pyspark.sql.session.SparkSession, default=None
        pyspark.sql.session.SparkSession used to load the input_data.
        If input data is provided this parameter is mandatory.
    HW_PATH : str, default='./'
        Path to the stop location data. Input data are expected in .parquet format.
        Only .parquet files contained in the provided directory will be loaded. Data will be loaded as a pyspark.sql.dataframe.DataFrame.
        The loaded pyspark.sql.dataframe.DataFrame must have all mandatory fields to avoid errors.
    SAVE_PATH : str, default=None
        Path were the labelled results should be saved. If default, the function will return the labelled pyspark.sql.dataframe.DataFrame.
    SAVE_NAME : str, default=''
        Name of the output file to use. If "save_multiple==True" this will be used as suffix for the differet configuration output files.
    save_multiple : bool, default:False
        If "True" automatically append suffixes and save multiple output files, one for each of the possible combinations of the HoWDe parameters you are willing to explore.
        In this case the parameter "SAVE_NAME" becomes mandatory!
        If "False" the function will only return or save the latest computed results.
    edit_config_default : dict, default=None
        Should contain a dictionary with keys the parameters you are willing to change and values you want to set.
        Handles core default settings:
            - 'is_time_local':True, # wether sop_location time is in local (True) or utc (False) # DTU should read use "False", WB should set this at "True"
            - 'min_stop_t': 60*1, # min stop duration of 5 min (in sec) # no need if this was a requirement set in infostop (WB stops were)
            - 'data_for_predict':False, # whether to only look backward in time (True) or to focus on half window before ad half after time t (False)
            ############## home/work windows and heuristics#############
            - 'start_hour_day': 6,
            - 'end_hour_day': 24,
            - 'start_hour_work': 9,
            - 'end_hour_work': 17,
            ############################################################

    ########### HoWDe explorable parameters ("soft") #################
    # For additional details on how these parameters affect the labelling flow see the PAPER.
    range_window : float, default=42
        Size of the window used to detect home and work locations.
        If a list is provided with multiple values, all provided values will be explored and labels will be computed for all the possible parameters' combinations.
    dhn : float, default=6 (same as "bnd_none_day" in config)
        Day level, at least (9 - bnd_nan) hours of data in hourly range.
        If a list is provided with multiple values, all provided values will be explored and labels will be computed for all the possible parameters' combinations.
    dn_H : float, default=0.7 (same as "bnd_none_day" in config)
        Sliding window: min ratio of none in window range for home location detection.
        If a list is provided with multiple values, all provided values will be explored and labels will be computed for all the possible parameters' combinations.
    dn_W : float, default=0.5 (same as "bnd_none_day" in config)
        Sliding window: min ratio of none in window range for work locationd etection.
        (consider that this value is expected to be higher than the one for home to account for non-weekend workers)
        If a list is provided with multiple values, all provided values will be explored and labels will be computed for all the possible parameters' combinations.
    hf_H : float, default=0.7 (same as "bnd_freq_home" in config)
        Sliding window: min frequency of visits within window for a stop location to be considered home.
        If a list is provided with multiple values, all provided values will be explored and labels will be computed for all the possible parameters' combinations.
    hf_W : flaot, default=0.4 (same as "bnd_freqH_work" in config)
        Sliding window: min frequency of visits within window for a stop location to be considered work (hourly level, at least bnd_freq_h ratio of the work range has to be at loc).
        If a list is provided with multiple values, all provided values will be explored and labels will be computed for all the possible parameters' combinations.
    hf_W : float, default=0.6 (same as "bnd_freqD_work" in config)
        Sliding window: min fraction of days with visits within window for a stop location to be considered work (day level, at least bnd_freq_d ratio of days in window locA has to appear).
        If a list is provided with multiple values, all provided values will be explored and labels will be computed for all the possible parameters' combinations.

    driver_memory : float, default=250
        pyspark.sql.session.SparkSession parameter to handle drivers memory (see PySpark documentation)
        This is only uses when "spark=None" to initialise a fresh pyspark.sql.session.SparkSession.
    stops_output : bool, default=True
        If "True", it outputs results in the form of a pyspark.sql.dataframe.DataFrame with stops splitted within day limits and the additional "location_type" column. If "False", it ouptuts results in the form of a condensed pyspark.sql.dataframe.DataFrame, where only changes in detected home and work locations are reported.
    verbose : bool, default=False
        If "True" reports processing steps.
    """
    ############################################################
    ### EDIT DEFAUKLT CONFIG ###
    config = {
        "is_time_local": True,  # wether sop_location time is in local (True) or utc (False) # DTU should read use "False", WB should set this at "True"
        # # min stop duration of 5 min (in sec)
        "min_stop_t": 60
        * 1,  # no need if this was a requirement set in infostop (WB stops were)
        ############## home/work windows and heuristics#############
        "start_hour_day": 6,
        "end_hour_day": 24,
        "start_hour_work": 9,
        "end_hour_work": 17,
        # whether to only look backward in time (True) or to focus on half window before ad half after time t (False)
        "data_for_predict": False,
    }
    if not edit_config_default is None:
        for k, v in edit_config_default.items():
            config[k] = v

    ###########################################################
    ### Convert all HoWDe explorable parameters to lists ###
    # convert all algorithm parameters to lists
    (
        dhn,
        dn_H,
        dn_W,
        range_window,
        hf_H,
        hf_W,
        df_W,
    ) = check_and_convert(
        [
            dhn,
            dn_H,
            dn_W,
            range_window,
            hf_H,
            hf_W,
            df_W,
        ]
    )

    ###########################################################
    ### LOAD STOP LOCATION DATA  ###
    ## STOPS LABELLED
    if (spark is None) & (not input_data is None):
        raise Exception(
            'You should either provide pyspark.sql.session.SparkSession alongside with data, or leave "input_data=None" and provide the path to load the data ("HW_PATH")'
        )

    if input_data is None:
        ### Set up "spark" Session
        packages = "data/work/shared/tools/spark-avro_2.12-3.0.0.jar"
        os.environ["PYSPARK_SUBMIT_ARGS"] = "--jars {0} pyspark-shell ".format(packages)

        spark = (
            SparkSession.builder.master("local[50]")
            .config("spark.sql.files.ignoreCorruptFiles", "true")
            .config("spark.driver.memory", f"{driver_memory}g")
            .config("spark.executor.memory", "250g")
            .getOrCreate()
        )
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        spark.sparkContext.setLogLevel("ERROR")

        input_data = spark.read.format("parquet").load(
            HW_PATH, pathGlobFilter="*.parquet"
        )

    # Check for mandatory fields
    # mandatory_fields = ['useruuid','loc','start','end']
    mandatory_fields = ["useruuid", "loc", "start", "end"]
    if not config["is_time_local"]:
        mandatory_fields + ["tz_hour_start", "tz_minute_start"]
    if not all(column in input_data.columns for column in mandatory_fields):
        missing = [col for col in mandatory_fields if not col in input_data.columns]
        raise Exception(
            f"Column missing ({missing})! Read documentation for mandatory fields: https://github.com/LLucchini/HoWDe/README.md"
        )
    # Reshape input data
    stops_labelled = pre_process_stops(spark=spark, stops=input_data, config=config)
    stops_labelled = stops_labelled.cache()

    ###########################################################
    ### HoWDe computations across parameter configurations  ###
    # check saving parameters
    if (SAVE_PATH is None) & (save_multiple):
        raise Exception("Incompatible saving options!")
    # start computations
    print("HoWDe Labelling: computing LABs ...")
    iters = list(
        itertools.product(
            range_window,
            dhn,
            dn_H,
            hf_H,
            dn_W,
            hf_W,
            df_W,
        )
    )
    for rW, noneD, noneH, freqH, noneW, freqWh, freqWd in tqdm(iters):
        ## APPLY BY CONFIG TYPE
        config_ = config.copy()
        config_["range_window"] = rW
        config_["bnd_nan"] = F.lit(noneD)
        config_["bnd_none_home"] = F.lit(noneH)
        config_["bnd_freq_home"] = F.lit(freqH)
        config_["bnd_none_work"] = F.lit(noneW)
        config_["bnd_freqH_work"] = F.lit(freqWh)
        config_["bnd_freqD_work"] = F.lit(freqWd)

        ### Saving labels
        if (not SAVE_PATH is None) & (save_multiple):
            fname = f"{SAVE_NAME}-rW{str(rW)}-bN{noneD}-nH{str(noneH)}-fH{str(freqH)}-nW{str(noneW)}-fWh{str(freqWh)}-fWdv{str(freqWd)}"
            stops_hw_lab = HoWDe_compute(
                stops_labelled, config_, stops_output=stops_output, verbose=verbose
            )
            stops_hw_lab.write.format("parquet").mode("overwrite").save(
                SAVE_PATH + fname
            )
            saved = True
        else:
            fname = f"{SAVE_NAME}"
            stops_hw_lab = HoWDe_compute(
                stops_labelled, config_, stops_output=stops_output, verbose=verbose
            )
            saved = False
    # Check is saving successfull or Returning results
    if (not saved) & (not SAVE_PATH is None):
        stops_hw_lab.write.format("parquet").mode("overwrite").save(SAVE_PATH + fname)
        print("HoWDe Labelling: computations completed and saved!")
    else:
        print("HoWDe Labelling: computations completed!")
        return stops_hw_lab
