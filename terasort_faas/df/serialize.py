import polars as pl
from io import BytesIO
import numpy as np
from typing import Dict, List


def serialize(partition_obj: pl.DataFrame) -> bytes:

    obj = BytesIO()
    partition_obj.write_parquet(obj, compression="snappy", use_pyarrow=True)
    return obj.getvalue()

def deserialize(b: bytes) -> pl.DataFrame:

    return pl.read_parquet(BytesIO(b), use_pyarrow=True)


def serialize_partitions(num_partitions: int,
                         partition_obj: List[List[bytes]]) \
        -> List[bytes]:
    # print(partition_obj)
    serialized_partitions = [b''.join(part) for part in partition_obj]
    return serialized_partitions


def _serialize_partition(partition_id: int,
                         partition_obj: pl.DataFrame,
                         hash_list: np.ndarray) \
        -> bytes:
    # Get rows corresponding to this worker
    pointers_ni = np.where(hash_list == partition_id)[0]

    pointers_ni = np.sort(pointers_ni.astype("uint32"))

    obj = pl.from_pandas(partition_obj.to_pandas(use_pyarrow_extension_array=True).iloc[pointers_ni])

    return serialize(obj)
