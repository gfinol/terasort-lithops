import polars as pl
from typing import List
import gc
from terasort_faas.df.serialize import deserialize

def construct_df(key_list, value_list) -> pl.DataFrame:

    data = {
        "0": key_list,
        "1": value_list
    }

    df = pl.DataFrame(
        data
        )
    
    return df

def concat_progressive(
        data: List[bytes]) \
        -> pl.DataFrame:
    df = []

    for r_i, r in enumerate(data):

        if len(data[r_i]) > 0:

            new_chunk = deserialize(data[r_i])

            # Remove data for memory efficiency
            data[r_i] = b""
            gc.collect()

            df.extend(new_chunk)
    # todo check if df is list of bytes or str
    return df