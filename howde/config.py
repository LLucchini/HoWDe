def default_config():
    return {
        "is_time_local": True,  # If True, timestamps in input are already in local time
        "min_stop_t": 60,  # Minimum duration of a stop in seconds
        "start_hour_day": 6,  # Start of the 'home hours' interval
        "end_hour_day": 24,  # End of the 'home hours' interval
        "start_hour_work": 9,  # Start of the 'work hours' interval
        "end_hour_work": 17,  # End of the 'work hours' interval
        "data_for_predict": False,  # If True, uses past-only data in sliding windows (causal mode)
    }


REQUIRED_COLUMNS = ["useruuid", "loc", "start", "end"]
REQUIRED_COLUMNS_WITH_TZ = REQUIRED_COLUMNS + ["tz_hour_start", "tz_minute_start"]
