Data source: 

> Khazaei, Bahram; Read, Laura K; Casali, Matthew; Sampson, Kevin M; Yates, David N (2022): GLOBathy, the Global Lakes Bathymetry Dataset. figshare. Collection. https://doi.org/10.6084/m9.figshare.c.5243309.v1

To re-create the `lakes.parquet` file use DuckDB: 

```sql
COPY (SELECT Hylak_id, Lake_name, Country, Dmax_use_m AS Depth_m 
	    FROM read_csv('~/Downloads/GLOBathy_basic_parameters/GLOBathy_basic_parameters(1-100K LAKES).csv',AUTO_DETECT=TRUE))
  TO 'lakes.parquet' (FORMAT 'PARQUET', CODEC 'ZSTD');
```