def default_config():
    return {
        "is_time_local": True,  # If True, timestamps in input are already in local time
        "min_stop_t": 60,  # Minimum duration of a stop in seconds
        "start_hour_day": 6,  # Start hours of day, used to define the end of the 'home hours' interval
        "end_hour_day": 24,  # End hours of day, used to define the start of the 'home hours' interval
        "start_hour_work": 9,  # Start of the 'work hours' interval
        "end_hour_work": 17,  # End of the 'work hours' interval
        "data_for_predict": False,  # If True, uses past-only data in sliding windows
    }


def thresholds_config():
    return {
        "C_hours": [0.1, 1],
        "C_days_H": [0, 1],
        "C_days_W": [0, 1],
        "range_window_home": [1, 365],
        "range_window_work": [1, 365],
        "f_hours_H": [0.3, 1],
        "f_hours_W": [0.1, 1],
        "f_days_W": [0.1, 1],
    }


REQUIRED_COLUMNS = ["useruuid", "loc", "start", "end"]
REQUIRED_COLUMNS_WITH_TZ = REQUIRED_COLUMNS + ["tz_hour_start", "tz_minute_start"]
