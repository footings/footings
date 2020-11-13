import pandas as pd


def convert_to_records(frame: pd.DataFrame, column_case=None):
    """Convert a DataFrame into a list of records.

    Parameters
    ----------
    frame : pd.DataFrame
        The DataFrame to convert.
    column_case : str, optional
        Set to 'lower' to convert columns to lower case and 'upper' for upper case.

    Returns
    -------
    list
        A list of records from the DataFrame.
    """
    if column_case == "upper":
        frame.columns = [col.upper() for col in frame.columns]
    elif column_case == "lower":
        frame.columns = [col.lower() for col in frame.columns]
    return frame.to_dict(orient="records")
