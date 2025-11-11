# HoWDe

**HoWDe** (Home and Work Detection) is a Python package designed to identify home and work locations from individual timestamped sequences of stop locations. It processes stop location data to label each location as 'Home', 'Work', or 'None' based on user-defined parameters and heuristics.

A complete description of the algorithm can be found in our [pre-print](https://arxiv.org/abs/2506.20679v1)
<!-- Add reference to paper -->

## Features

- Processes stop location datasets to detect home and work locations. 
- Allows customization through various parameters to fine-tune detection heuristics.
- Supports batch processing with multiple parameter configurations.
- Outputs results as a PySpark DataFrame for seamless integration with big data workflows.

## Installation

HoWDe requires **Python 3.6 or later** and a functional **PySpark** environment.

**1. Install PySpark**

Before installing HoWDe, ensure PySpark and Java are properly configured.  
For detailed setup instructions, please refer to the official PySpark documentation:  
- [PySpark Installation Guidelines](https://spark.apache.org/docs/latest/api/python/getting_started/install.html#manually-downloading)  
- [Debugging PySpark and Py4JJavaError](https://spark.apache.org/docs/latest/api/python/development/debugging.html)

> **Note for Windows users:**  
> PySpark may raise `Py4JJavaError` if Java or Spark are not properly configured.
> 
**2. Install HoWDe**

Once PySpark is installed and configured, you can install HoWDe via `pip`:

```bash
pip install HoWDe
```

## Usage

The core function of the HoWDe package is `HoWDe_labelling`, which performs the detection of home and work locations.

```python
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
    Perform Home and Work Detection (HoWDe)
    """
```

### 📥 Input Data

HoWDe expects the input to be a **PySpark DataFrame** containing one row per user stop, with the following columns:

| Column | Type | Description |
|:--|:--|:--|
| `useruuid` | *str* or *int* | Unique user identifier. |
| `loc` | *str* or *int* | Stop location ID (unique per `useruuid`). <br> ⚠️ Avoid using `-1` to label meaningful stops, as these are dropped following the [Infostop](https://github.com/ulfaslak/infostop?tab=readme-ov-file) convention. |
| `start` | *long* | Start time of the stop (Unix timestamp). |
| `end` | *long* | End time of the stop (Unix timestamp). |
| `tz_hour_start`, `tz_minute_start` | *int* | Optional. Time zone offsets (hours and minutes) used to convert UTC timestamps to local time, if applicable. |
| `country` | *int* | Optional. Country code; if not provided, a default `"GL0B"` label is assigned. |

#### Example

```python
+---------+-----+-------------+-------------+---------------+----------------+---------+
| useruuid| loc | start       | end         | tz_hour_start | tz_minute_start| country |
+---------+-----+-------------+-------------+---------------+----------------+---------+
| 1001    |  1 | 1704031200  | 1704034800  | 1             | 0              | DK      |
| 1001    |  2 | 1704056400  | 1704060000  | 1             | 0              | DK      |
+---------+-----+-------------+-------------+---------------+----------------+---------+
```

💡 Scalability Tip: This package involves heavy computations (e.g., window functions, UDFs). To ensure efficient parallel processing, use df.repartition("useruuid") to distribute data across partitions evenly. This reduces memory bottlenecks and improves resource utilization.

### ⚙️ Key Parameters
<!-- 
- `range_window_home` (float or list): Sliding window size (in days) used to detect home locations.
- `range_window_work` (float or list): Size of the window used to detect work locations. 
- `dhn` (float or list): Minimum number of night-/work-hour bins with data required in a day. 
- `dn_H` (float or list):  Maximum fraction of missing days allowed within the window for a home location to be detected on a given day. 
- `dn_W` (float or list):  Maximum fraction of missing days allowed within the window for a work location to be detected on a given day. 
- `hf_H` (float or list): Minimum average fraction of night-hour bins (across days in the window) required for a location to qualify as ‘Home’. 
- `hf_W` (float or list): Minimum average fraction of work-hour bins (across days in the window) required for a location to qualify as ‘Work’. 
- `df_W` (float or list): Minimum fraction of days within the window a location must be visited to qualify as ‘Work’. 
-->

| Parameter | Type | Description | Suggested value and range |
|:--|:--|:--|:--|
| `range_window_home` | *float* or *list* | Sliding window size (in days) used to detect home locations. | 28 [14-112] |
| `range_window_work` | *float* or *list* | Sliding window size (in days) used to detect work locations. | 42 [14-112] |
| `dhn` | *float* or *list* | Minimum number of night-/work-hour bins with data required per day. | 3 [3-6]|
| `dn_H` | *float* or *list* | Maximum fraction of missing days allowed within the window for a home location to be detected. | 0.7 [0.5-0.9]|
| `dn_W` | *float* or *list* | Maximum fraction of missing days allowed within the window for a work location to be detected. | 0.5 [0.4-0.6]|
| `hf_H` | *float* or *list* | Minimum average fraction of night-hour bins (across days in the window) required for a location to qualify as *Home*. | 0.7 [0.5-0.9] |
| `hf_W` | *float* or *list* | Minimum average fraction of work-hour bins (across days in the window) required for a location to qualify as *Work*. |  0.4 [0.4-0.6] |
| `df_W` | *float* or *list* | Minimum fraction of days within the window a location must be visited to qualify as *Work*. | 0.6 [0.5-0.8] |



All parameters listed above can also be provided as lists to explore multiple configurations in a single run.

💡 **Tuning Tip:** When adjusting detection parameters, start by refining the data quality constraints (`dn_H`, `dn_W`) and frequency thresholds (`hf_H`, `hf_W`, `df_W`). These strongly influence how strict the algorithm is in identifying consistent home/work locations.
<!-- Suggested parameter ranges to explore  -->


### 🔧 Other Parameters
- `edit_config_default` (dict, optional):
Optional dictionary that allows overriding the default settings in [`howde/config.py`](howde/config.py) to fine-tune preprocessing and detection behavior.  
The dictionary should include parameters:
  - **`is_time_local`** — interpret timestamps as local time (`True`) or UTC (`False`)  
  - **`min_stop_t`** — minimum stop duration (seconds)  
  - **`start_hour_day`, `end_hour_day`** — hours used for *home* detection  
  - **`start_hour_work`, `end_hour_work`** — hours used for *work* detection  
  - **`data_for_predict`** — use only past data for estimation  

- `stops_output` (bool): If `stop`, returns stop-level data with `location_type` and one row per stop. If `change`, returns a compact DataFrame with only one row per day with home/work location changes.
- `verbose` (bool): If True, reports processing steps.


### 📤 Returns

If a single parameter configuration is used, the function returns a PySpark DataFrame with three additional columns:
- `detect_H_loc` The location ID (`loc`) identified as Home. Assigned if the location satisfies all filtering criteria. As such, it represents a day-level assessment, taking into account observations within a sliding window of t ± `range_window_home` / 2 days.
- `detect_W_loc`  The location ID (`loc`) identified as Work. Assigned if the location satisfies all filtering criteria. As such, it represents a day-level assessment, taking into account observations within a sliding window of t ± `range_window_work` / 2 days.
- `location_type`  Indicates the detected location type for each stop ('H' for Home, 'W' for Work, or 'O' for Other), based on matching the stop location to the inferred home/work labels.

If multiple parameter configurations are provided (as lists), the function returns a list of dictionaries, each with keys:
- `configs`: including the configuration used
- `res`: including the resulting labeled PySpark DataFrame (as described above)


## Example Usage

```python
from pyspark.sql import SparkSession
from howde import HoWDe_labelling

# Initialize Spark session
spark = SparkSession.builder.appName('HoWDeApp').getOrCreate()

# Load your stop location data
input_data = spark.read.parquet('path_to_your_data.parquet')

# Run HoWDe labelling
labeled_data = HoWDe_labelling(
    input_data,
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
)

# Show the results
labeled_data.show()
```

See more examples at [`/tutorials`](https://github.com/LLucchini/HoWDe/tree/main/tutorials)



## Data
Anonymized stop location data with true home and work labels _will be_ available at:

De Sojo Caso, Silvia; Lucchini, Lorenzo; Alessandretti, Laura (2025). Benchmark datasets for home and work location detection: stop sequences and annotated labels. Technical University of Denmark. Dataset. https://doi.org/10.11583/DTU.28846325

## License

This project is licensed under the MIT License. See the [License file](https://opensource.org/licenses/MIT) for details.
