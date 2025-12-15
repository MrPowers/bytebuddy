from typing import List, Union
import pyarrow.parquet as pq

def parquet_bytes(paths: Union[str, List[str]]):
    def compute_bytes(path):
        pf = pq.ParquetFile(path)
        uncomp = 0
        comp = 0

        for rg in range(pf.num_row_groups):
            rg_meta = pf.metadata.row_group(rg)
            for c in range(rg_meta.num_columns):
                col = rg_meta.column(c)
                uncomp += col.total_uncompressed_size
                comp += col.total_compressed_size

        return uncomp, comp

    if isinstance(paths, str):
        return compute_bytes(paths)
    else:
        uncomp = 0
        comp = 0
        for path in paths:
            u, c = compute_bytes(path)
            uncomp = uncomp + u
            comp = comp + c
        return uncomp, comp


def parquet_bytes_by_row_group(path):
    res = {"uncompressed": [], "compressed": []}
    pf = pq.ParquetFile(path)
    for rg in range(pf.num_row_groups):
        rg_meta = pf.metadata.row_group(rg)
        for c in range(rg_meta.num_columns):
            col = rg_meta.column(c)
            res["uncompressed"].append([c, col.total_uncompressed_size])
            res["compressed"].append([c, col.total_compressed_size])
    return res


def bytes_to_gb(bytes_value: int) -> float:
    """Convert bytes to gigabytes (GB)"""
    return bytes_value / (1024 ** 3)
