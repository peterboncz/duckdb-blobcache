BlobCache will intercept reads that go through httpfs, s3, azure, r2 and gcp and will write what you read to a local file. Subsequent reads will be served from that.

Hence, it is an SSD cache for cloud-based data, such as DuckLake tables.

By default, it will only cache parquet files, and only if set enable_http_metadata_cache = true (false by default)

Because, when parquet metadata is cached, DuckDB assumes that parquet files do not change and can be cached. Note that DuckDB since 1.3 also has a RAM cache (enable_http_metadata_cache) which is enabled by default. However, that cache does check the etags and will reload a parquet file if a newer version became available.

This blobcache when it is active will supplement DuckDB's RAM cache. the blobcache does not do RAM caching itself (except the boundary case where a cached Read is still being written to SSD by its bacground writers and another Read already requests it).

This blobcache can greatly expand how much data can be cached compared to the RAM cache, because SSDs are much larger. And they are quite fast.

The blobcache uses simple synchronous I/O but tends to write largish sequential blocks, so that can still be efficient. It also employs writer background threads to do so.

You can configure with: CALL blobcache_config(directory, max_size_mb, num_writer_threads, regexps="")
You can inspect the configuration by invoking that without parameters.
You can reconfigure an existing cache by changing all parameters except the first (the directory). If you change the directory (where the cached file ranges are stored), then the cache gets cleared.

The current contents of the cache can be queried with FROM blobcache_stats();

The regexps parameter contains semicolon-separated regexps that allow more aggresive caching: they will cache any URL that matches one of the regexps.
