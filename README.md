# HoWDe

**HoWDe** (Home and Work Detection) is a Python package designed to identify home and work locations from individual timestamped sequences of stop locations. It processes stop location data to label each location as 'Home', 'Work', or 'None' based on user-defined parameters and heuristics.

## Features

- Processes stop location datasets to detect home and work locations. 
- Allows customization through various parameters to fine-tune detection heuristics.
- Supports batch processing with multiple parameter configurations.
- Outputs results as a PySpark DataFrame for seamless integration with big data workflows.

## Installation

To install HoWDe, ensure you have Python 3.6 or later and PySpark installed. You can then install the package using pip:

```bash
pip install HoWDe
```

## Usage

The core function of the HoWDe package is `HoWDe_labelling`, which performs the detection of home and work locations.

### `HoWDe_labelling` Function

```python
def HoWDe_labelling(
    input_data=None,
    spark=None,
    HW_PATH='./',
    SAVE_PATH=None,
    SAVE_NAME='',
    save_multiple=False,
    edit_config_default=None,
    range_window=42,
    dhn=3,
    dn_H=0.4,
    dn_W=0.8,
    hf_H=0.2,
    hf_W=0.2,
    df_W=0.2,
    stops_output=True,
    verbose=False,
    driver_memory=250
):
    """
    Perform Home and Work Detection (HoWDe)
    """
```

#### Parameters

- `input_data` (PySpark DataFrame, default=None): Preloaded data containing all mandatory fields. If not provided, data will be loaded from the `HW_PATH` directory.
- `spark` (PySpark SparkSession, default=None): Spark session used to load the `input_data`. Mandatory if `input_data` is provided.
- `HW_PATH` (str, default='./'): Path to the stop location data in `.parquet` format.
- `SAVE_PATH` (str, default=None): Path where the labeled results should be saved. If not provided, the function returns the labeled DataFrame.
- `SAVE_NAME` (str, default=''): Name of the output file. Used as a suffix if `save_multiple` is True.
- `save_multiple` (bool, default=False): If True, saves multiple output files for each combination of parameters. Requires `SAVE_NAME` to be specified.
- `edit_config_default` (dict, default=None): Dictionary to override default configuration settings.
- `range_window` (float or list, default=42): Size of the window used to detect home and work locations. Can be a list to explore multiple values.
- `dhn` (float or list, default=6): Minimum hours of data required in a day. Can be a list to explore multiple values.
- `dn_H` (float or list, default=0.4): Minimum ratio of presence required at a location to label it as 'Home'. Can be a list to explore multiple values.
- `dn_W` (float or list, default=0.8): Minimum ratio of presence required at a location to label it as 'Work'. Can be a list to explore multiple values.
- `hf_H` (float or list, default=0.2): Minimum frequency of visits within the window for a location to be considered 'Home'. Can be a list to explore multiple values.
- `hf_W` (float or list, default=0.2): Minimum frequency of visits within work hours for a location to be considered 'Work'. Can be a list to explore multiple values.
- `df_W` (float or list, default=0.2): Minimum fraction of days with visits within the window for a location to be considered 'Work'. Can be a list to explore multiple values.
- `stops_output` (bool, default=True): If True, outputs results with stops split within day limits and an additional `location_type` column. If False, outputs a condensed DataFrame with only changes in detected home and work locations.
- `verbose` (bool, default=False): If True, reports processing steps.
- `driver_memory` (float, default=250): Driver memory allocation for the Spark session.

#### Returns

- A PySpark DataFrame with an additional column `location_type` indicating the detected location type ('H' for Home, 'W' for Work, or None). The label is assigned based on whether the location satisfies all filtering criteria within a sliding time window. As such, location_type represents a day-level assessment, taking into account observations from neighboring days within the range t+/- range_window/2.

## Example Usage

### Example 1: Providing Pre-loaded Data and Spark Session

```python
from pyspark.sql import SparkSession
from howde import HoWDe_labelling

# Initialize Spark session
spark = SparkSession.builder.appName('HoWDeApp').getOrCreate()

# Load your stop location data
input_data = spark.read.parquet('path_to_your_data.parquet')

# Run HoWDe labelling
labeled_data = HoWDe_labelling(
    input_data=input_data,
    spark=spark,
    range_window=42,
    dhn=6,
    dn_H=0.4,
    dn_W=0.8,
    hf_H=0.2,
    hf_W=0.2,
    df_W=0.2,
    stops_output=True,
    verbose=True
)

# Show the results
labeled_data.show()
```

### Example 2: Self-contained Usage

```python
from howde import HoWDe_labelling

# Define path to your stop location data
HW_PATH = './'

# Run HoWDe labelling
labeled_data = HoWDe_labelling(
    HW_PATH=HW_PATH,
    range_window=42,
    dhn=6,
    dn_H=0.4,
    dn_W=0.8,
    hf_H=0.2,
    hf_W=0.2,
    df_W=0.2,
    stops_output=True,
    verbose=True
)

# Show the results
labeled_data.show()
```

## License

This project is licensed under the MIT License. See the [License file](https://opensource.org/licenses/MIT) for details.
