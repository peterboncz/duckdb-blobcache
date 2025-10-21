smartcache will intercept reads that go through httpfs, s3, hf, azure, r2 and gcp and will write what you read to a local file. Subsequent reads will be served from that.
Hence, it is a local-disk (i.e., SSD) cache for cloud-based data, such as DuckLake tables.

The SSD files in the cache furter get RAM caching through the DuckDB buffer pool through a new instance of the DuckDB ExternalFileCache. note that that smartcache turns off the official ExternalFileCache in the database instance (automatic SET external_file_cache = false), in order to be able to intercept the traffic on external files. 

smartcache actually maintains two caches: one for small read requests (<=2KB) and one for larger requests. The idea is that in parquet reads, meta-data reads are small, whereas data (column-chunks) are larger. Given the high latency of S3 requests, the small reads are more painful than the larger ones but cost almost no cache space. So small ranges should stay longer in the cache. 
The large cache gets 90% of the capacity, the small cache gets everything not used by the large cache. This means that in a workload with only small reads, the small cache can get 100% (instead of the normal 10%). 

Small read ranges are stored together in files of 256KB max. the files are spread over 4096 subdirectories so one can have millions of them without overwhelming the local filesystem with huge amounts of directory entries. Large read ranges are cached in a separate file per request. This makes it possible to do fine-grained caching: if attention shifts to a a subset of rowgroups or columns inside tge same file, the cache will store only tge hottest regions. The policy is LRU (for both caches separately) on the SSD stored cacge files. 

This smart caching strategy tailored to actual read patterns on parquet files, is what sets smartcache apart from other DuckDB caching extensions.

By default, it will only cache parquet files, and only if set enable_http_metadata_cache = true (false by default)
Because, when parquet metadata is cached, DuckDB assumes that parquet files do not change and can be cached. This is not necessary for parquet files managed by DuckLake, since DuckDB in that case assumes it is the only dtstem potentially modifying them (=deleting them, practically, since files on cloud blob storage would not get modified anyway).

smartcache supports a fakes3://X filesystem which acts like a local filesystem, but adds fake network latencies similar to S3 latencies as observed inside the same region. this is a handy tool for local performance debugging without having to spin up an EC2 instance.

You can configure with: FROM smartcache_config(directory, max_size_mb, num_io_threads, regexps="")
You can inspect the configuration by invoking that without parameters.
You can reconfigure an existing cache by changing all parameters except the first (the directory). If you change the directory (where the cached file ranges are stored), then the cache gets cleared.

The regexps parameter contains semicolon-separated regexps that allow more aggressive caching: they will cache any URL that matches one of the regexps.

The current contents of the cache can be queried with FROM smartcache_stats(); It lists the cache contents in reverse LRU order for both the small- and large-range cache (hottest ranges first). One possible usage of this TableFunction could be to store the (leading) part of these ranges in a DuckDB table. because, smartcache provides a smartcache_prefetch(URL, start, size) function that uses massively parallel IO to read and cache these ranges. this allows for quick hydration of the smartcache from a previous state.
