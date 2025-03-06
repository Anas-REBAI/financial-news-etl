import numpy as np
import pandas as pd

def check_completeness(df: pd.DataFrame, required_columns: list) -> bool:
    """
    Checks if all required columns are present and non-empty in the DataFrame.
    """
    for col in required_columns:
        if col not in df.columns or df[col].isnull().all():
            return False
    return True


def check_validity(df: pd.DataFrame, column: str, min_value: float = None, max_value: float = None) -> bool:
    """
    Checks if the values in a specified column are within a valid range.
    """
    if min_value is not None and (df[column] < min_value).any():
        return False
    if max_value is not None and (df[column] > max_value).any():
        return False
    return True


def check_consistency(df: pd.DataFrame, date_column: str, group_column: str = None) -> bool:
    """
    Checks if the dates in the DataFrame are in chronological order.
    If a group_column is provided, checks consistency within each group.
    """
    if group_column:
        df_sorted = df.sort_values(by=[group_column, date_column])
        return df_sorted.equals(df.sort_values(by=[group_column, date_column]))
    else:
        df_sorted = df.sort_values(by=date_column)
        return df_sorted.equals(df)


def check_uniqueness(df: pd.DataFrame, subset: list) -> bool:
    """
    Checks if there are no duplicates in the specified columns.
    """
    return not df.duplicated(subset=subset).any()