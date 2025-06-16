def validate_input_columns(df, required_columns, label="input data"):
    """
    Check that all required columns exist in the given DataFrame.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        The input DataFrame to validate.
    required_columns : list of str
        Columns that must be present.
    label : str, optional
        Label for error reporting (e.g. "stop file").

    Raises
    ------
    ValueError
        If any required columns are missing.
    """
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(
            f"Missing required columns in {label}: {missing}\n"
            f"Expected columns: {required_columns}\n"
            f"Got columns: {df.columns}"
        )


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
