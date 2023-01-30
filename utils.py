import pandas as pd

from typing import Union


def multi_melt(df: pd.DataFrame, id_vars: list, value_vars: Union[str, list], var_name: Union[str, list], value_name: Union[str, list]) -> pd.DataFrame:

    """
    Multi meld function

    Parameters:
    -----------
        df: pd.DataFrame
            A DataFrame.
        id_vars: list
            Column(s) to use as identifier variables.
        value_vars: Union[str, list]
            Column(s) to unpivot. If not specified, uses all columns that are not set as id_vars.
        var_name: Union[str, list]
            Name to use for the 'variable' column. If None uses frame.columns.name or 'variable'
        value_name: Union[str, list]
            Name to use for the 'value' column
    Returns:
    --------
        df_res: pd.DataFrame
            The multi meld DataFrame
    """

    dfi = []
    df = df.copy()

    # check if value_vars is a nested list
    if not any(isinstance(i, list) for i in value_vars):
        value_vars = [value_vars]
        var_name = [var_name]
        value_name = [value_name]
    
    for i in range(0, len(value_vars)):
        dfi.append(df.melt(id_vars, value_vars=value_vars[i], var_name=var_name[i], value_name=value_name[i]))

    df_res = pd.concat([dfi[i] for i in range(0, len(value_vars))], axis=1)
    df_res = df_res[id_vars + value_name]
    df_res = df_res.loc[:, ~df_res.columns.duplicated()].copy()

    return df_res


def deduplicate_columns(df: pd.DataFrame) -> pd.DataFrame:

    df_columns = df.columns
    new_columns = []

    for item in df_columns:
        counter = 0
        newitem = item
        while newitem in new_columns:
            counter += 1
            newitem = f"{item}_{counter}"
        new_columns.append(newitem)
    df.columns = new_columns
    return df
