# Change log

## [2.0.0] - 2025-11-22
### Updates
- Expanded documentation and tutorials for improved usability and clearer guidance.
- Improved package version stability: Added explicit versioning and regression tests using pytest to ensure consistent behavior across releases.
- Harmonised key parameters definition for HoWDe_labelling to improve consistency:
    - **C_hours (former dhn)**: Now defined as the minimum fraction of night/business hourly bins with data in a day, replacing the previous definition based on the minimum number of hours with data.
    - **C_days (former dh_W,H)**: Now defined as the minimum fraction of days with data within a window. This replaces the previous definition based on the maximum fraction of days with missing data (i.e, now C_days = 1 - dh_W,H).
    - Default parameters have been updated to reflect the new logic.
      
  ***WARNING***: These changes affect both parameter names and their underlying code. Please consult the updated documentation for full details.

- New parameter validation error: Parameters falling outside the allowed range in `config.py` or having an incorrect type now raise a `ValueError` to prevent unintended configurations.


## [1.1] - 2025-06-23
### Updates
- Reduced in-package features to simplify usage.
- Expansion of usage documentation with cleaner docs and dedicated tutorials.

## [0.1] - 2025-02-14
### Added
- Initial release of HoWDe.
- Implemented `HoWDe_labelling` for home/work detection.
