#include "blobcache.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// Helper to check if a range is stale (its CacheFile has been deleted)
//===----------------------------------------------------------------------===//
static bool IsRangeStale(const BlobCacheFileRange *range,
                         const unordered_map<string, unique_ptr<BlobCacheFile>> *filepath_cache) {
	if (!range || !filepath_cache || range->cache_filepath.empty()) {
		return false;
	}
	auto cache_file_it = filepath_cache->find(range->cache_filepath);
	if (cache_file_it != filepath_cache->end()) {
		return cache_file_it->second->deleted.load(std::memory_order_acquire);
	}
	// CacheFile not found in map - it has been evicted, so this range is stale
	return true;
}

//===----------------------------------------------------------------------===//
// AnalyzeRange that helps with Inserting and Reading ranges from a cache
// Now includes lazy deletion: removes ranges whose CacheFile has been deleted
//===----------------------------------------------------------------------===//

static BlobCacheFileRange *AnalyzeRange(map<idx_t, unique_ptr<BlobCacheFileRange>> &ranges, idx_t position,
                                        idx_t &max_nr_bytes,
                                        unordered_map<string, unique_ptr<BlobCacheFile>> *filepath_cache) {
	if (ranges.empty()) {
		return nullptr;
	}

	// Lazy deletion loop: keep searching until we find a valid range or exhaust all stale ranges
	while (true) {
		auto it = ranges.upper_bound(position);

		// Check if the previous range covers our position
		BlobCacheFileRange *hit_range = nullptr;
		if (it != ranges.begin()) {
			auto prev_it = std::prev(it);
			auto &prev_range = prev_it->second;
			if (prev_range && prev_range->range_end > position) {
				if (IsRangeStale(prev_range.get(), filepath_cache)) {
					ranges.erase(prev_it); // Stale range found (CacheFile has been deleted) - delete it and loop again
					continue;
				}
				// Range is usable if: (1) memcache_buffer exists (in ExternalFileCache), OR (2) disk write completed
				bool has_memcache = prev_range->memcache_buffer != nullptr;
				bool disk_complete = prev_range->disk_write_completed.load(std::memory_order_acquire);
				if (has_memcache || disk_complete) {
					hit_range = prev_range.get();
				}
			}
		}

		// Check the next range to see if we need to cut short max_nr_bytes
		if (it != ranges.end() && it->second) {
			// Check if this range is stale (CacheFile has been deleted)
			if (IsRangeStale(it->second.get(), filepath_cache)) {
				ranges.erase(it); // Stale range found (CacheFile has been deleted) - delete it and loop again
				continue;
			}
			if (it->second->range_start < position + max_nr_bytes) {
				max_nr_bytes = it->second->range_start - position;
			}
		}

		return hit_range;
	}
}

idx_t BlobCache::ReadFromCache(const string &cache_key, const string &filename, idx_t pos, void *buffer, idx_t &len) {
	BlobCacheType cache_type = // Determine which blobcache to use based on request size
	    (len < config.small_range_threshold) ? BlobCacheType::SMALL_RANGE : BlobCacheType::LARGE_RANGE;
	idx_t position = pos;
	idx_t max_nr_bytes = len;
	std::unique_lock<std::mutex> lock(blobcache_mutex);
	auto &blobcache_map = GetCacheMap(cache_type);
	BlobCacheEntry *blobcache_entry = blobcache_map.FindFile(cache_key, filename);
	if (!blobcache_entry) {
		return 0; // Nothing cached for this file
	}

	auto hit_range = AnalyzeRange(blobcache_entry->ranges, position, max_nr_bytes, blobcache_map.filepath_cache.get());
	if (!hit_range) {
		return 0; // No overlapping range
	}

	// Touch the LRU for the CacheFile (lookup by filepath)
	if (!hit_range->cache_filepath.empty()) {
		auto cache_file_it = blobcache_map.filepath_cache->find(hit_range->cache_filepath);
		if (cache_file_it != blobcache_map.filepath_cache->end()) {
			blobcache_map.TouchLRU(cache_file_it->second.get());
		}
	}

	idx_t hit_size = std::min(max_nr_bytes, hit_range->range_end - position);
	idx_t offset = position - hit_range->range_start;
	hit_range->bytes_from_cache += hit_size;
	hit_range->usage_count++;

	// Copy data needed before unlocking
	idx_t cached_blobcache_range_start = hit_range->blobcache_range_start;
	string blobcache_filepath = hit_range->cache_filepath;
	idx_t saved_range_start = hit_range->range_start;
	lock.unlock();

	// Read from cache file unlocked
	idx_t bytes_from_mem = 0;
	if (!blobcache_map.ReadFromCacheFile(blobcache_filepath, cached_blobcache_range_start + offset, buffer, hit_size,
	                                     bytes_from_mem)) {
		return 0; // Read failed
	}

	// Update bytes_from_mem counter if we had a memory hit
	if (bytes_from_mem > 0) {
		lock.lock();
		blobcache_entry = blobcache_map.FindFile(cache_key, filename);
		if (blobcache_entry) {
			auto range_it = blobcache_entry->ranges.find(saved_range_start);
			if (range_it != blobcache_entry->ranges.end()) {
				range_it->second->bytes_from_mem += bytes_from_mem;
			}
		}
		lock.unlock();
	}
	return hit_size;
}

// we had to read from the original source (e.g. S3). Now try to cache this buffer in the disk-based blobcache
void BlobCache::InsertCache(const string &cache_key, const string &filename, idx_t pos, void *buffer, idx_t len) {
	if (!config.blobcache_initialized || len == 0 || len > config.total_cache_capacity) {
		return;
	}
	BlobCacheType cache_type = // Determine blobcache type based on original request size
	    (len < config.small_range_threshold) ? BlobCacheType::SMALL_RANGE : BlobCacheType::LARGE_RANGE;

	std::lock_guard<std::mutex> lock(regex_mutex);
	auto &cache_map = GetCacheMap(cache_type);
	auto cache_entry = cache_map.UpsertFile(cache_key, filename);
	if (!cache_entry) {
		return; // name collision (rare)
	}
	// Check (under lock) if range already cached (in the meantime, due to concurrent reads)
	auto hit_range = AnalyzeRange(cache_entry->ranges, pos, len, cache_map.filepath_cache.get());
	idx_t offset = 0, actual_bytes = 0, range_start = pos, range_end = range_start + len;
	if (hit_range) { // another thread cached the same range in the meantime
		offset = hit_range->range_end - range_start;
		range_start = hit_range->range_end; // cache only from the end
	}
	if (range_end > range_start) {
		actual_bytes = range_end - range_start;
	}
	if (actual_bytes == 0) {
		return; // nothing to cache
	}
	// Don't protect cache_key during eviction - let LRU work naturally.
	// The original protection caused a bug: when a file is larger than cache capacity,
	// all existing ranges from that file get protected, preventing eviction.
	// This causes the cache to fill up and refuse new insertions.
	if (!EvictToCapacity(cache_type, actual_bytes, "")) {
		config.LogError("InsertCache: EvictToCapacity failed for " + to_string(actual_bytes) +
		                " bytes (cache_type=" + (cache_type == BlobCacheType::LARGE_RANGE ? "large" : "small") +
		                ", large_size=" + to_string(largerange_blobcache->current_cache_size) +
		                ", small_size=" + to_string(smallrange_blobcache->current_cache_size) + ")");
		return; // failed to make room
	}
	InsertRangeInternal(cache_type, cache_entry, cache_key, filename, range_start, range_end,
	                    ((char *)buffer) + offset);
}

void BlobCache::InsertRangeInternal(BlobCacheType cache_type, BlobCacheEntry *blobcache_entry, const string &cache_key,
                                    const string &filename, idx_t range_start, idx_t range_end, const void *buffer) {
	idx_t actual_bytes = range_end - range_start;
	BufferHandle buffer_handle;
	if (!AllocateInMemCache(buffer_handle, actual_bytes)) {
		config.LogError("InsertRangeInternal: AllocateInMemCache failed for " + to_string(actual_bytes) + " bytes");
		return; // allocation from DDB buffer pool failed
	}
	std::memcpy(buffer_handle.Ptr(), static_cast<const char *>(buffer), actual_bytes);

	// Get or create the CacheFile for this range
	auto &blobcache_map = GetCacheMap(cache_type);
	auto cache_file =
	    blobcache_map.GetOrCreateCacheFile(blobcache_entry, cache_key, cache_type, range_start, actual_bytes);
	if (!cache_file) {
		config.LogError("InsertRangeInternal: failed to get or create cache file");
		return;
	}

	// Create new range and update stats
	auto new_range = make_uniq<BlobCacheFileRange>(range_start, range_end);
	auto range_ptr = new_range.get();
	range_ptr->cache_filepath = cache_file->filepath; // Store filepath for lookup in filepath_cache
	blobcache_entry->ranges[range_start] = std::move(new_range);
	blobcache_entry->cached_file_size += actual_bytes;
	blobcache_map.current_cache_size += actual_bytes;
	blobcache_map.num_ranges++;

	// Assign smallrange_id
	if (cache_type == BlobCacheType::SMALL_RANGE) {
		// For small ranges: assign unique ID and update file handle's last_smallrange_id
		range_ptr->smallrange_id = smallrange_id_counter.fetch_add(1, std::memory_order_relaxed);
		cache_file->last_smallrange_id = range_ptr->smallrange_id;
	} else {
		// For large ranges: capture the last small range ID from the file handle
		range_ptr->smallrange_id = cache_file->last_smallrange_id;
	}

	// Pre-compute blobcache_range_start (offset in CacheFile where this range will be written)
	range_ptr->blobcache_range_start = cache_file->current_file_size;
	cache_file->current_file_size += actual_bytes;

	// Register this cached piece in the memcache
	string blobcache_filepath = cache_file->filepath;
	InsertRangeIntoMemcache(blobcache_filepath, range_ptr->blobcache_range_start, buffer_handle, actual_bytes);

	// Schedule the disk write, using thread partitioning based on file_id
	auto file_buffer =
	    duckdb::make_shared_ptr<BlobCacheFileBuffer>(std::move(buffer_handle), actual_bytes, cache_key, filename);
	file_buffer->disk_write_completed_ptr = &range_ptr->disk_write_completed;
	range_ptr->memcache_buffer = file_buffer;
	idx_t partition = cache_file->file_id % num_io_threads;
	QueueIOWrite(blobcache_filepath, partition, file_buffer);
}

//===----------------------------------------------------------------------===//
// Memory cache helpers
//===----------------------------------------------------------------------===//

void BlobCache::InsertRangeIntoMemcache(const string &blobcache_filepath, idx_t blobcache_range_start,
                                        BufferHandle &buffer_handle, idx_t length) {
	auto &memcache_file = blobfile_memcache->GetOrCreateCachedFile(blobcache_filepath);
	auto memcache_range = make_shared_ptr<ExternalFileCache::CachedFileRange>(buffer_handle.GetBlockHandle(), length,
	                                                                          blobcache_range_start, "");
	auto lock_guard = memcache_file.lock.GetExclusiveLock();
	memcache_file.Ranges(lock_guard)[blobcache_range_start] = memcache_range;
	config.LogDebug("InsertRangeIntoMemcache: inserted into memcache: '" + blobcache_filepath + "' at offset " +
	                std::to_string(blobcache_range_start) + " length " + std::to_string(length));
}

bool BlobCache::TryReadFromMemcache(const string &blobcache_filepath, idx_t blobcache_range_start, void *buffer,
                                    idx_t &length) {
	if (!blobfile_memcache) {
		return false;
	}
	// Check if the range is already cached in memory
	auto &memcache_file = blobfile_memcache->GetOrCreateCachedFile(blobcache_filepath);
	auto memcache_ranges_guard = memcache_file.lock.GetSharedLock();
	auto &memcache_ranges = memcache_file.Ranges(memcache_ranges_guard);
	auto it = memcache_ranges.find(blobcache_range_start);
	if (it == memcache_ranges.end()) {
		return false; // Range not found in memory cache
	}
	auto &memcache_range = *it->second;
	if (memcache_range.nr_bytes == 0) {
		return false; // Empty range
	}
	config.LogDebug("TryReadFromMemcache: memcache hit for " + to_string(length) + " bytes in '" + blobcache_filepath +
	                "',  offset " + to_string(blobcache_range_start) + " length " + to_string(memcache_range.nr_bytes));
	auto &buffer_manager = blobfile_memcache->GetBufferManager();
	auto pin = buffer_manager.Pin(memcache_range.block_handle);
	if (!pin.IsValid()) {
		config.LogDebug("TryReadFromMemcache: pinning cache hit failed -- apparently there is high memory pressure");
		return false;
	}
	if (memcache_range.nr_bytes < length) {
		length = memcache_range.nr_bytes;
	}
	std::memcpy(buffer, pin.Ptr(), length); // Memory hit - read from BufferHandle
	return true;
}

//===----------------------------------------------------------------------===//
// Multi-threaded background cache writer implementation
//===----------------------------------------------------------------------===//

void BlobCache::QueueIOWrite(const string &filepath, idx_t partition, duckdb::shared_ptr<BlobCacheFileBuffer> buffer) {
	{
		std::lock_guard<std::mutex> lock(io_mutexes[partition]);
		write_queues[partition].emplace(filepath, buffer);
	}
	io_cvs[partition].notify_one();
}

void BlobCache::QueueIORead(const string &filename, const string &cache_key, idx_t range_start, idx_t range_size) {
	// Round-robin assignment across all threads (no partitioning needed for reads)
	idx_t target_thread = read_job_counter.fetch_add(1, std::memory_order_relaxed) % num_io_threads;
	{
		std::lock_guard<std::mutex> lock(io_mutexes[target_thread]);
		read_queues[target_thread].emplace(filename, cache_key, range_start, range_size);
	}
	io_cvs[target_thread].notify_one();
}

void BlobCache::StartIOThreads(idx_t thread_count) {
	if (thread_count > MAX_IO_THREADS) {
		thread_count = MAX_IO_THREADS;
		config.LogDebug("StartIOThreads: limiting IO threads to maximum allowed: " + std::to_string(MAX_IO_THREADS));
	}
	shutdown_io_threads = false;
	num_io_threads = thread_count;

	config.LogDebug("StartIOThreads: starting " + std::to_string(num_io_threads) + " blobcache IO threads");

	for (idx_t i = 0; i < num_io_threads; i++) {
		io_threads[i] = std::thread([this, i] { MainIOThreadLoop(i); });
	}
}

void BlobCache::StopIOThreads() {
	if (num_io_threads == 0) {
		return; // Skip if no threads are running
	}
	shutdown_io_threads = true; // Signal shutdown to all threads

	// Notify all threads to wake up and check shutdown flag
	for (idx_t i = 0; i < num_io_threads; i++) {
		io_cvs[i].notify_all();
	}
	// Wait for all threads to finish gracefully
	for (idx_t i = 0; i < num_io_threads; i++) {
		if (io_threads[i].joinable()) {
			try {
				io_threads[i].join();
			} catch (const std::exception &) {
				// Ignore join errors during shutdown - thread may have already terminated
			}
		}
	}
	// Only log if not shutting down
	if (!config.blobcache_shutting_down) {
		config.LogDebug("StopIOThreads: stopped " + std::to_string(num_io_threads) + " cache writer threads");
	}
	num_io_threads = 0; // Reset thread count
}

void BlobCache::ProcessWriteJob(const BlobCacheWriteJob &job) {
	// Determine cache type from filename format:
	// Small: /cachedir/{4hex}/small{id}
	// Large: /cachedir/{4hex}/{hex}{range_start}_{file_id}{suffix}
	size_t last_sep = job.filepath.find_last_of(config.path_sep);
	string filename_part = job.filepath.substr(last_sep + 1);
	BlobCacheType cache_type =
	    StringUtil::StartsWith(filename_part, "small") ? BlobCacheType::SMALL_RANGE : BlobCacheType::LARGE_RANGE;
	BlobCacheMap &cache = GetCacheMap(cache_type);

	idx_t blobcache_range_start;
	if (cache.WriteToCacheFile(job.filepath, job.buffer->buffer_handle.Ptr(), job.buffer->size,
	                           blobcache_range_start)) {
		// Mark disk write as completed via backpointer
		job.buffer->disk_write_completed_ptr->store(true, std::memory_order_release);
		// Unpin the buffer now that write is complete - allows buffer manager to evict if needed
		job.buffer->Unpin();
	} else {
		// Write failed - log error (memcache has stale data, but eviction is unsafe from background thread)
		config.LogError("ProcessWriteJob: failed to write '" + job.filepath + "'");
	}
}

void BlobCache::ProcessReadJob(const BlobCacheReadJob &job) {
	try {
		// Open file and allocate buffer
		auto &fs = config.GetFileSystem();
		auto handle = fs.OpenFile(job.filename, FileOpenFlags::FILE_FLAGS_READ);
		auto buffer = unique_ptr<char[]>(new char[job.range_size]);

		// Read data from file
		fs.Read(*handle, buffer.get(), job.range_size, job.range_start);

		// Insert into cache (this will queue a write job)
		InsertCache(job.cache_key, job.filename, job.range_start, buffer.get(), job.range_size);
	} catch (const std::exception &e) {
		config.LogError("ProcessReadJob: failed to read '" + job.filename + "' at " + to_string(job.range_start) +
		                ": " + string(e.what()));
	}
}

void BlobCache::MainIOThreadLoop(idx_t thread_id) {
	config.LogDebug("MainIOThreadLoop " + std::to_string(thread_id) + " started");
	while (!shutdown_io_threads) {
		std::unique_lock<std::mutex> lock(io_mutexes[thread_id]);
		io_cvs[thread_id].wait(lock, [this, thread_id] {
			return !write_queues[thread_id].empty() || !read_queues[thread_id].empty() || shutdown_io_threads;
		});

		if (shutdown_io_threads && write_queues[thread_id].empty() && read_queues[thread_id].empty()) {
			break;
		}

		// Process writes with priority
		if (!write_queues[thread_id].empty()) {
			auto write_job = std::move(write_queues[thread_id].front());
			write_queues[thread_id].pop();
			lock.unlock();
			ProcessWriteJob(write_job);
		} else if (!read_queues[thread_id].empty()) {
			auto read_job = std::move(read_queues[thread_id].front());
			read_queues[thread_id].pop();
			lock.unlock();
			ProcessReadJob(read_job);
		}
	}

	// Only log thread shutdown if not during database shutdown to avoid access to destroyed instance
	if (!config.blobcache_shutting_down) {
		config.LogDebug("MainIOThreadLoop " + std::to_string(thread_id) + " stopped");
	}
}

//===----------------------------------------------------------------------===//
// BlobCacheMap - CacheFile management
//===----------------------------------------------------------------------===//

BlobCacheFile *BlobCacheMap::GetOrCreateCacheFile(BlobCacheEntry *cache_entry, const string &cache_key,
                                                  BlobCacheType cache_type, idx_t range_start, idx_t range_size) {
	// Note: Must be called with blobcache_mutex held
	if (cache_type == BlobCacheType::SMALL_RANGE && cache_entry->cached_file_size + range_size <= 256 * 1024) {
		auto filepath = config.GenCacheFilePath(current_file_id, cache_key, cache_type);
		auto cache_file_it = filepath_cache->find(filepath);
		if (cache_file_it != filepath_cache->end()) {
			auto cache_file = cache_file_it->second.get();
			if (!cache_file->deleted.load()) { // ok, we can append to this existing CacheFile
				cache_entry->cached_file_size += range_size;
				TouchLRU(cache_file);
				config.LogDebug("GetOrCreateCacheFile: append range of " + to_string(range_size) + " to " + filepath);
				return cache_file;
			}
		}
	}
	// Create a new CacheFile
	auto filepath = config.GenCacheFilePath(++current_file_id, cache_key, cache_type);
	auto new_cache_file = make_uniq<BlobCacheFile>(filepath, current_file_id, cache_key);
	auto cache_file_ptr = new_cache_file.get();
	(*filepath_cache)[filepath] = std::move(new_cache_file);
	AddToLRUFront(cache_file_ptr);
	config.LogDebug("GetOrCreateCacheFile: create " + filepath + " for range of " + to_string(range_size));
	return cache_file_ptr;
}

//===----------------------------------------------------------------------===//
// BlobCacheMap - eviction logic
//===----------------------------------------------------------------------===//

bool BlobCacheMap::EvictToCapacity(idx_t required_space, const string &exclude_cache_key) {
	// Try to evict CacheFiles to make space, returns true if successful
	// Note: This is called with blobcache_mutex already held
	idx_t freed_space = 0;
	auto *current = lru_tail; // Start from least recently used
	idx_t files_checked = 0;
	idx_t files_skipped_empty = 0;
	idx_t files_skipped_excluded = 0;
	idx_t max_files = filepath_cache->size() + 1; // Safety limit to prevent infinite loops

	while (required_space > freed_space && current && files_checked < max_files) {
		files_checked++;
		// Check if this CacheFile was already deleted (shouldn't happen, but safety check)
		if (current->deleted.load(std::memory_order_acquire)) {
			current = current->lru_prev;
			continue;
		}

		// Save next candidate before we potentially evict current
		auto *next_to_try = current->lru_prev;

		// Skip if this CacheFile belongs to the cache_key we're currently inserting
		if (!exclude_cache_key.empty() && current->cache_key == exclude_cache_key) {
			config.LogDebug("EvictToCapacity: skipping CacheFile for excluded cache_key '" + exclude_cache_key + "'");
			files_skipped_excluded++;
			current = next_to_try; // Move to next file in LRU
			continue;
		}

		// Evict this CacheFile
		idx_t evicted_bytes = current->current_file_size;
		string filepath = current->filepath;

		// Mark as deleted (ranges will be cleaned up lazily by AnalyzeRange)
		current->deleted.store(true, std::memory_order_release);

		// Remove from LRU and filepath_cache
		RemoveFromLRU(current);
		filepath_cache->erase(filepath);

		// Delete the physical file
		if (DeleteCacheFile(filepath)) {
			freed_space += evicted_bytes;
			current_cache_size -= std::min<idx_t>(current_cache_size, evicted_bytes);
			config.LogDebug("EvictToCapacity: evicted CacheFile '" + filepath + "' freeing " +
			                std::to_string(evicted_bytes) + " bytes");
		} else {
			config.LogError("EvictToCapacity: failed to delete file '" + filepath + "'");
			// Continue trying other files even if deletion failed
		}

		current = next_to_try; // Move to next file in LRU
	}

	if (files_checked >= max_files) {
		config.LogError("EvictToCapacity: hit safety limit after checking " + std::to_string(files_checked) + " files");
	}

	if (freed_space < required_space) {
		// Count total files in filepath_cache for diagnostics
		idx_t total_files = filepath_cache ? filepath_cache->size() : 0;
		config.LogError(
		    "EvictToCapacity: needed " + std::to_string(required_space) + " bytes but only freed " +
		    std::to_string(freed_space) + " bytes (current_cache_size=" + std::to_string(current_cache_size) +
		    ", total_files=" + std::to_string(total_files) + ", exclude_cache_key='" + exclude_cache_key +
		    "', files_checked=" + std::to_string(files_checked) + ", files_skipped_empty=" +
		    std::to_string(files_skipped_empty) + ", files_skipped_excluded=" + std::to_string(files_skipped_excluded) +
		    ", lru_head=" + (lru_head ? "present" : "null") + ", lru_tail=" + (lru_tail ? "present" : "null") + ")");
		return false;
	}
	return true;
}

vector<BlobCacheRangeInfo>
BlobCacheMap::GetStatistics() const { // produce list of cached ranges for blobcache_stats() TF
	vector<BlobCacheRangeInfo> result;
	result.reserve(num_ranges);

	// Iterate through all CacheEntries (not LRU, since LRU is now for CacheFiles)
	for (const auto &cache_pair : *key_cache) {
		const auto &cache_entry = cache_pair.second;
		BlobCacheRangeInfo info;
		info.protocol = "unknown";
		info.filename = cache_entry->filename;
		auto pos = info.filename.find("://");
		if (pos != string::npos) {
			info.protocol = info.filename.substr(0, pos);
			info.filename = info.filename.substr(pos + 3, info.filename.length() - (pos + 3));
		}
		for (const auto &range_pair : cache_entry->ranges) {
			// Skip stale ranges (whose CacheFile has been deleted)
			if (IsRangeStale(range_pair.second.get(), filepath_cache.get())) {
				continue;
			}
			info.blobcache_range_start = range_pair.second->blobcache_range_start;
			info.range_start = range_pair.second->range_start;
			info.range_size = range_pair.second->range_end - range_pair.second->range_start;
			info.usage_count = range_pair.second->usage_count;
			info.bytes_from_cache = range_pair.second->bytes_from_cache;
			info.bytes_from_mem = range_pair.second->bytes_from_mem;
			info.smallrange_id = range_pair.second->smallrange_id;
			result.push_back(info);
		}
	}
	return result;
}

//===----------------------------------------------------------------------===//
// BlobCacheMap file management
//===----------------------------------------------------------------------===//

unique_ptr<FileHandle> BlobCacheMap::TryOpenCacheFile(const string &cache_filepath) {
	if (!config.db_instance) {
		return nullptr;
	}
	try {
		auto &fs = config.GetFileSystem();
		return fs.OpenFile(cache_filepath, FileOpenFlags::FILE_FLAGS_READ);
	} catch (const std::exception &e) {
		// File was evicted between metadata check and open - this is expected
		config.LogDebug("TryOpenCacheFile: file not found (likely evicted): '" + cache_filepath + "'");
		return nullptr;
	}
}

bool BlobCacheMap::ReadFromCacheFile(const string &blobcache_filepath, idx_t blobcache_range_start, void *buffer,
                                     idx_t &length, idx_t &out_bytes_from_mem) {
	if (config.parent_cache->TryReadFromMemcache(blobcache_filepath, blobcache_range_start, buffer, length)) {
		out_bytes_from_mem = length;
		return true; // reading from memcache first succeeded
	}
	// Not in memory cache - read from disk and memcache it
	out_bytes_from_mem = 0; // Initialize to 0 (disk read)
	auto handle = TryOpenCacheFile(blobcache_filepath);
	if (!handle) {
		return false; // File was evicted or doesn't exist - signal cache miss
	}
	// allocate memory using the DuckDB buffer manager
	BufferHandle buffer_handle;
	if (!config.parent_cache->AllocateInMemCache(buffer_handle, length)) {
		return false; // allocation failed
	}
	auto buffer_ptr = buffer_handle.Ptr();
	try {
		config.GetFileSystem().Read(*handle, buffer_ptr, length, blobcache_range_start); // Read from disk into buffer
	} catch (const std::exception &e) {
		// File was evicted/deleted after opening but before reading - signal cache miss to fall back to original source
		buffer_handle.Destroy();
		config.LogDebug("ReadFromCacheFile: read failed (likely evicted during read): '" + blobcache_filepath +
		                "': " + string(e.what()));
		return false;
	}
	std::memcpy(buffer, buffer_ptr, length); // Copy to output buffer
	config.parent_cache->InsertRangeIntoMemcache(blobcache_filepath, blobcache_range_start, buffer_handle, length);
	return true;
}

bool BlobCacheMap::WriteToCacheFile(const string &blobcache_filepath, const void *buffer, idx_t length,
                                    idx_t &blobcache_range_start) {
	if (!config.db_instance) {
		return false;
	}
	try {
		auto &fs = config.GetFileSystem();
		auto flags = // Open file for writing in append mode (create if not exists)
		    FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE | FileOpenFlags::FILE_FLAGS_APPEND;
		auto handle = fs.OpenFile(blobcache_filepath, flags);
		if (!handle) {
			config.LogError("WriteToCacheFile: failed to open: '" + blobcache_filepath + "'");
			return false;
		}
		// Get current file size to know where we're appending
		blobcache_range_start = static_cast<idx_t>(fs.GetFileSize(*handle));
		int64_t bytes_written = fs.Write(*handle, const_cast<void *>(buffer), length);
		handle->Close(); // Close handle explicitly
		if (bytes_written != static_cast<int64_t>(length)) {
			config.LogError("WriteToCacheFile: failed to write all bytes to '" + blobcache_filepath + "' (wrote " +
			                std::to_string(bytes_written) + " of " + std::to_string(length) + ")");
			return false;
		}
	} catch (const std::exception &e) {
		config.LogError("WriteToCacheFile: failed to write to '" + blobcache_filepath + "': " + string(e.what()));
		return false;
	}
	return true;
}

bool BlobCacheMap::DeleteCacheFile(const string &cache_filepath) {
	if (!config.db_instance)
		return false;
	try {
		auto &fs = config.GetFileSystem();
		fs.RemoveFile(cache_filepath);
		config.LogDebug("DeleteCacheFile: deleted file '" + cache_filepath + "'");
		return true;
	} catch (const std::exception &e) {
		config.LogError("DeleteCacheFile: failed to delete file '" + cache_filepath + "': " + string(e.what()));
		return false;
	}
}

//===----------------------------------------------------------------------===//
// BlobCache eviction from both (small,large) range caches
//===----------------------------------------------------------------------===//

bool BlobCache::EvictToCapacity(BlobCacheType cache_type, idx_t new_range_size, const string &exclude_cache_key) {
	/* CRITICAL REASONING - DO NOT MODIFY WITHOUT UNDERSTANDING THIS LOGIC:
	 *
	 * Small and large caches share the SAME total capacity pool (e.g., 1GB total).
	 * Small cache gets 10% (100MB), large cache gets 90% (900MB) of the total.
	 *
	 * KEY INSIGHT: When large cache grows, small cache capacity SHRINKS (they're coupled).
	 * Example: If large cache grows from 800MB to 850MB (still below its 900MB limit), small cache's
	 * effective capacity reduces from 200MB to 150MB, and even though it hasn't grown, it is is now "too large"
	 *
	 * Therefore, when evicting for large insertion:
	 * 1. Evict from large cache to make room for new_range_size (the new insertion)
	 * 2. ALWAYS check if small cache needs eviction, regardless of step 1's outcome
	 * 3. if the large eviction already made room for new_range_size, it can be set to zero for the small cache checl
	 * 4. because even if new_range_size is satisfied by large cache eviction, small cache may still be over limit
	 */
	auto result = true;
	if (cache_type == BlobCacheType::LARGE_RANGE) {
		idx_t large_cap = GetCacheCapacity(BlobCacheType::LARGE_RANGE);
		if (largerange_blobcache->current_cache_size + new_range_size > large_cap) {
			if (!largerange_blobcache->EvictToCapacity(
			        largerange_blobcache->current_cache_size + new_range_size - large_cap, exclude_cache_key)) {
				result = false;
			} else {
				new_range_size = 0; // we have made space, but still will check small cache
			}
		}
	}
	idx_t small_cap = GetCacheCapacity(BlobCacheType::SMALL_RANGE);
	if (smallrange_blobcache->current_cache_size + new_range_size > small_cap) {
		result &= smallrange_blobcache->EvictToCapacity(
		    smallrange_blobcache->current_cache_size + new_range_size - small_cap, exclude_cache_key);
	}
	return result;
}

//===----------------------------------------------------------------------===//
// BlobCache (re-) configuration
//===----------------------------------------------------------------------===//

void BlobCache::ConfigureCache(const string &base_dir, idx_t max_size_bytes, idx_t max_io_threads,
                               idx_t small_threshold) {
	std::lock_guard<std::mutex> lock(blobcache_mutex);
	auto directory = base_dir + (StringUtil::EndsWith(base_dir, config.path_sep) ? "" : config.path_sep);
	if (!config.blobcache_initialized) {
		// Release lock before calling InitializeCache to avoid deadlock
		config.blobcache_dir = directory;
		config.total_cache_capacity = max_size_bytes;
		config.small_range_threshold = small_threshold;

		config.LogDebug("ConfigureCache: initializing cache: directory='" + config.blobcache_dir +
		                "' max_size=" + std::to_string(config.total_cache_capacity) +
		                " bytes io_threads=" + std::to_string(max_io_threads) +
		                " small_threshold=" + std::to_string(config.small_range_threshold));
		if (!config.InitCacheDir()) {
			config.LogError("ConfigureCache: initializing cache directory='" + config.blobcache_dir + "' failed");
		}
		smallrange_blobcache->Clear();
		largerange_blobcache->Clear();
		config.blobcache_initialized = true;
		// Initialize our own ExternalFileCache instance (always enabled for memory caching)
		blobfile_memcache = make_uniq<ExternalFileCache>(*config.db_instance, true);
		config.LogDebug("ConfigureCache: initialized blobfile_memcache for memory caching of disk-cached files");
		StartIOThreads(max_io_threads);
		return;
	}

	// Cache already initialized, check what needs to be changed
	bool need_restart_threads = (num_io_threads != max_io_threads);
	bool directory_changed = (config.blobcache_dir != directory);
	bool size_reduced = (max_size_bytes < config.total_cache_capacity);
	bool size_changed = (config.total_cache_capacity != max_size_bytes);
	bool threshold_changed = (config.small_range_threshold != small_threshold);
	if (!directory_changed && !need_restart_threads && !size_changed && !threshold_changed) {
		config.LogDebug("ConfigureCache: configuration unchanged, no action needed");
		return;
	}

	// Stop existing threads if we need to change thread count or directory
	config.LogDebug("ConfigureCache: old_dir='" + config.blobcache_dir + "' new_dir='" + directory + "' old_size=" +
	                std::to_string(config.total_cache_capacity) + " new_size=" + std::to_string(max_size_bytes) +
	                " old_threshold=" + std::to_string(config.small_range_threshold) + " new_threshold=" +
	                std::to_string(small_threshold) + " old_threads=" + std::to_string(num_io_threads) +
	                " new_threads=" + std::to_string(max_io_threads));
	if (num_io_threads > 0 && (need_restart_threads || directory_changed)) {
		config.LogDebug("ConfigureCache: stopping existing cache IO threads for reconfiguration");
		StopIOThreads();
	}

	// Clear existing cache only if directory changed or threshold changed
	if (directory_changed || threshold_changed) {
		config.LogDebug("ConfigureCache: directory or threshold changed, clearing cache");
		smallrange_blobcache->Clear();
		largerange_blobcache->Clear();
		if (directory_changed) {
			if (!config.CleanCacheDir()) { // Clean old directory before switching
				config.LogError("ConfigureCache: cleaning cache directory='" + config.blobcache_dir + "' failed");
			}
		}
		config.blobcache_dir = directory;
		config.small_range_threshold = small_threshold;
		if (!config.InitCacheDir()) {
			config.LogError("ConfigureCache: initializing cache directory='" + config.blobcache_dir + "' failed");
		}
		// Reinitialize blobfile_memcache when directory changes
		blobfile_memcache = make_uniq<ExternalFileCache>(*config.db_instance, true);
		config.LogDebug("ConfigureCache: reinitialized blobfile_memcache after directory change");
	}
	// Same directory, just update capacity and evict if needed
	config.total_cache_capacity = max_size_bytes;
	if (size_reduced && !EvictToCapacity()) {
		config.LogError("ConfigureCache: failed to reduce the directory sizes to the new lower capacity/");
	}
	// Start threads if they were stopped or thread count changed
	if (need_restart_threads || directory_changed) {
		StartIOThreads(max_io_threads);
	}
	config.LogDebug("ConfigureCache complete: directory='" + config.blobcache_dir + "' max_size=" +
	                to_string(config.total_cache_capacity) + " bytes io_threads=" + to_string(max_io_threads) +
	                " small_threshold=" + to_string(config.small_range_threshold));
}

//===----------------------------------------------------------------------===//
// caching policy based on regexps
//===----------------------------------------------------------------------===//

void BlobCache::UpdateRegexPatterns(const string &regex_patterns_str) {
	std::lock_guard<std::mutex> lock(regex_mutex);

	cached_regexps.clear(); // Clear existing patterns
	if (regex_patterns_str.empty()) {
		// Conservative mode: empty regexps
		config.LogDebug("UpdateRegexPatterns: updated to conservative mode (empty regex patterns)");
		return;
	}
	// Aggressive mode: parse semicolon-separated patterns
	vector<string> pattern_strings = StringUtil::Split(regex_patterns_str, ';');
	for (const auto &pattern_str : pattern_strings) {
		if (!pattern_str.empty()) {
			try {
				cached_regexps.emplace_back(pattern_str, std::regex_constants::icase);
				config.LogDebug("UpdateRegexPatterns: compiled regex pattern: '" + pattern_str + "'");
			} catch (const std::regex_error &e) {
				config.LogError("UpdateRegexPatterns: wrong regex pattern '" + pattern_str + "': " + string(e.what()));
			}
		}
	}
	config.LogDebug("UpdateRegexPatterns: now using " + std::to_string(cached_regexps.size()) + " regex patterns");
}

bool BlobCache::ShouldCacheFile(const string &filename, optional_ptr<FileOpener> opener) const {
	std::lock_guard<std::mutex> lock(regex_mutex);
	if (StringUtil::StartsWith(StringUtil::Lower(filename), "file://")) {
		return false; // Never cache file:// URLs as they are already local
	}
	if (StringUtil::StartsWith(StringUtil::Lower(filename), "fakes3://")) {
		return true; // Always cache fakes3:// URLs for testing
	}
	if (!cached_regexps.empty()) {
		// Aggressive mode: use cached compiled regex patterns
		for (const auto &compiled_pattern : cached_regexps) {
			if (std::regex_search(filename, compiled_pattern)) {
				return true;
			}
		}
	} else if (StringUtil::EndsWith(StringUtil::Lower(filename), ".parquet") && opener) {
		Value parquet_cache_value; // Conservative mode: only cache .parquet files if parquet_metadata_cache=true
		auto parquet_result = FileOpener::TryGetCurrentSetting(opener, "parquet_metadata_cache", parquet_cache_value);
		if (parquet_result) {
			return BooleanValue::Get(parquet_cache_value);
		}
	}
	return false;
}

//===----------------------------------------------------------------------===//
// BlobCacheConfig - Configuration and utility methods
//===----------------------------------------------------------------------===//

bool BlobCacheConfig::CleanCacheDir() {
	if (!db_instance)
		return false;
	auto &fs = GetFileSystem();
	if (!fs.DirectoryExists(blobcache_dir)) {
		return true; // Directory doesn't exist, nothing to clean
	}
	auto success = true;
	try {
		// List all subdirectories in blobcache_dir
		fs.ListFiles(blobcache_dir, [&](const string &name, bool is_dir) {
			if (name == "." || name == "..") {
				return;
			}
			string subdir_path = blobcache_dir + path_sep + name;
			if (is_dir) {
				try { // Remove all files in subdirectory
					fs.ListFiles(subdir_path, [&](const string &subname, bool sub_is_dir) {
						if (subname == "." || subname == "..") {
							return;
						}
						string file_path = subdir_path + path_sep + subname;
						try {
							fs.RemoveFile(file_path);
						} catch (const std::exception &) {
							success = false;
						}
					});
				} catch (const std::exception &) {
					success = false;
				}
				// Remove subdirectory
				try {
					fs.RemoveDirectory(subdir_path);
				} catch (const std::exception &) {
					success = false;
				}
			}
		});
	} catch (const std::exception &) {
		success = false;
	}
	return success;
}

} // namespace duckdb
