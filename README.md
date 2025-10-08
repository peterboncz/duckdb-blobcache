BlobCache will intercept reads that go through httpfs, s3, hf, azure, r2 and gcp and will write what you read to a local file. Subsequent reads will be served from that.
Hence, it is a local-disk (i.e., SSD) cache for cloud-based data, such as DuckLake tables.

It actually maintains two caches: one for small read requests (<=2KB) and one for larger requests. The idea is that in parquet reads, meta-data reads are small, whereas data (column-chunks) are larger. Given the high latency of S3 requests, the small reads are more painful than the larger ones but cost almost no cache space. So small ranges should stay longer in the cache. 
The large cache gets 90% of the capacity, the snall cache gets everything not used by the large cache. This means that in a workload with only small reads, the small cache can get 100% (instead of the normal 10%). LRU on the file level is used to keep both caches within their respecitve maximum capacities.  

By default, it will only cache parquet files, and only if set enable_http_metadata_cache = true (false by default)
Because, when parquet metadata is cached, DuckDB assumes that parquet files do not change and can be cached. Note that DuckDB since 1.3 also has a RAM cache (enable_http_metadata_cache) which is enabled by default. However, that cache does check the etags and will reload a parquet file if a newer version became available.

This blobcache when it is active will supplement DuckDB's RAM cache. The blobcache does not do RAM caching itself (except the boundary case where a cached Read is still being written to SSD by its background writing threads while another Read already requests it). 
This blobcache can greatly expand how much data can be cached compared to the RAM cache, because SSDs are much larger. And they are quite fast.
The blobcache uses simple synchronous I/O but tends to write largish sequential blocks, so that can still be efficient. 

You can configure with: CALL blobcache_config(directory, max_size_mb, num_writer_threads, regexps="")
You can inspect the configuration by invoking that without parameters.
You can reconfigure an existing cache by changing all parameters except the first (the directory). If you change the directory (where the cached file ranges are stored), then the cache gets cleared.

The current contents of the cache can be queried with FROM blobcache_stats();

The regexps parameter contains semicolon-separated regexps that allow more aggresive caching: they will cache any URL that matches one of the regexps.
