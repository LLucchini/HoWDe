import itertools
import pyspark.sql.functions as F

from .utils import (
    validate_input_columns,
    check_and_convert,
)

from .core import (
    get_hourly_trajectories,
    find_home,
    find_work,
    get_stop_level,
    get_change_level,
    pre_process_stops,
)

from .config import default_config, REQUIRED_COLUMNS, REQUIRED_COLUMNS_WITH_TZ


def HoWDe_compute(df_stops, config, output_format="stop"):
    """
    Core pipeline: Detect home and work locations and format output.

    Parameters
    ----------
    df_stops : pyspark.sql.DataFrame
        Preprocessed stop-level input data.
    config : dict
        Configuration with detection parameters.
    output_format : str, default="stop"
        Can be "stop" (detailed, same as stops) or "change" (compact, one row per day with home/work loc change).

    Returns
    -------
    pyspark.sql.DataFrame
        Labelled stops with home/work assignments.
    """
    df_traj = get_hourly_trajectories(df_stops, config)
    df_traj = find_home(df_traj, config)
    df_labeled = find_work(df_traj, config)

    if output_format == "stop":
        return get_stop_level(df_stops, df_labeled)
    elif output_format == "change":
        return get_change_level(df_labeled)
    else:
        raise ValueError(
            f"Unknown output_format: {output_format} (only stop or change allowed)"
        )


def HoWDe_labelling(
    input_data,
    edit_config_default=None,
    range_window_home=28,
    range_window_work=42,
    dhn=3,
    dn_H=0.7,
    dn_W=0.5,
    hf_H=0.7,
    hf_W=0.4,
    df_W=0.6,
    output_format="stop",
    verbose=False,
):
    """
    Run the full HoWDe labelling pipeline over one or multiple parameter configurations.

    Parameters
    ----------
    input_data : pyspark.sql.DataFrame
        Raw stop data.
    edit_config_default : dict, optional
        Overrides to default configuration.
    output_format : str
        "stop" or "change" to determine the output format.
    verbose : bool
        Print status info.

    Returns
    -------
    pyspark.sql.DataFrame or list of dict
        If only one configuration is used, returns a PySpark DataFrame directly.
        If multiple configurations are tested, returns a list of dicts with:
            - 'configs': the parameter settings used
            - 'res': the resulting labeled DataFrame

    """

    # 1. Load config
    config = default_config()
    if edit_config_default is not None:
        config.update(edit_config_default)

    # 2. Validate input data
    REQUIRED_COLUMNS = ["useruuid", "loc", "start", "end"]
    REQUIRED_COLUMNS_WITH_TZ = REQUIRED_COLUMNS + ["tz_hour_start", "tz_minute_start"]
    required_cols = (
        REQUIRED_COLUMNS_WITH_TZ if not config["is_time_local"] else REQUIRED_COLUMNS
    )
    validate_input_columns(input_data, required_cols, label="stop data")

    # 3. Validate selected output format
    if output_format not in {"stop", "change"}:
        raise ValueError(
            f"Invalid output_format: {output_format}. Must be 'stop' or 'change'."
        )

    # 4. Convert parameters to lists
    (
        dhn,
        dn_H,
        dn_W,
        range_window_home,
        range_window_work,
        hf_H,
        hf_W,
        df_W,
    ) = check_and_convert(
        [
            dhn,
            dn_H,
            dn_W,
            range_window_home,
            range_window_work,
            hf_H,
            hf_W,
            df_W,
        ]
    )

    # 5. Pre-process stops
    df_stops = pre_process_stops(input_data, config).cache()
    if verbose:
        print("[HowDe] Stops pre-processed")

    # 6. Loop over parameter combinations
    output = []
    param_grid = itertools.product(
        range_window_home,
        range_window_work,
        dhn,
        dn_H,
        hf_H,
        dn_W,
        hf_W,
        df_W,
    )

    for rW_H, rW_W, noneD, noneH, freqH, noneW, freqWh, freqWd in param_grid:
        config_ = config.copy()
        config_.update(
            {
                "range_window_home": rW_H,
                "range_window_work": rW_W,
                "dhn": F.lit(noneD),
                "dn_H": F.lit(noneH),
                "hf_H": F.lit(freqH),
                "dn_W": F.lit(noneW),
                "hf_W": F.lit(freqWh),
                "df_W": F.lit(freqWd),
            }
        )

        if verbose:
            print(
                f"[HoWDe] Running config: "
                f"rw_H={rW_H}, rw_W={rW_W}, dn_H={noneH}, hf_H={freqH}, "
                f"dn_W={noneW}, hf_W={freqWh}, df_W={freqWd}"
            )

        df_labeled = HoWDe_compute(df_stops, config_, output_format=output_format)
        output.append({"configs": config_, "res": df_labeled})

    if verbose:
        print("[HoWDe] Computations completed")

    if len(output) == 1:
        return output[0]["res"]

    return output
