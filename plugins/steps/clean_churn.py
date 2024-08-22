import pandas as pd


def remove_duplicates(data: pd.DataFrame) -> pd.DataFrame:
    """
    Removes duplicate rows from the input DataFrame based on
    the values in all columns except the "customer_id" column.

    Args:
        data (pd.DataFrame):
            The input DataFrame to remove duplicates from.

    Returns:
        pd.DataFrame:
            The input DataFrame with duplicate rows removed.
    """
    feature_cols = data.columns.drop("customer_id").tolist()
    is_duplicated_features = data.duplicated(subset=feature_cols, keep=False)
    data = data[~is_duplicated_features].reset_index(drop=True)
    return data


def iqr_filter(data: pd.DataFrame, k: float = 1.5) -> pd.DataFrame:
    """
    Filters the input DataFrame by removing rows that have
    values outside the interquartile range (IQR) for each
    numeric feature.

    Args:
        data (pd.DataFrame):
            The input DataFrame to filter.
        k (float):
            The multiplier for the IQR to determine
            the lower and upper bounds. Defaults to 1.5.

    Returns:
        pd.DataFrame:
            The input DataFrame without outliers.
    """
    for col in data.select_dtypes(["float"]).columns:
        Q1 = data[col].quantile(0.25)
        Q3 = data[col].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - k * IQR
        upper = Q3 + k * IQR
        data = data[(data[col] >= lower) & (data[col] <= upper)]
    return data


def fill_missing_values(data: pd.DataFrame) -> pd.DataFrame:
    """
    Fills missing values in the input DataFrame by replacing them
    with the mean for numeric columns and the mode for object columns.

    Args:
        data (pd.DataFrame):
            The input DataFrame to fill missing values in.

    Returns:
        pd.DataFrame:
            The input DataFrame with missing values filled.
    """
    cols_with_nans = data.isnull().sum()
    cols_with_nans = cols_with_nans[cols_with_nans > 0].index.drop("end_date")
    for col in cols_with_nans:
        if data[col].dtype in [float, int]:
            fill_value = data[col].mean()
        elif data[col].dtype == "object":
            fill_value = data[col].mode().iloc[0]
        data[col] = data[col].fillna(fill_value)
    return data
