# bytebuddy

Helper methods to easily find the sizes of files or data lakes.

Hope to add functions to show the distribution of data amongst row groups for tabular and spatial data.

## Example

Here's how to compute the number of uncompressed and compressed bytes in the `vehicle.parquet` file:

```python
uncomp, comp = bytebuddy.parquet_bytes("tests/vehicle.parquet")
```

Here's how to compute the number of uncompressed and compressed bytes for each row group:

```python
bytebuddy.parquet_bytes_by_row_group("tests/vehicle.parquet")
```
