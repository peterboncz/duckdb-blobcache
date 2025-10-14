#include "blobcache.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// AnalyzeRange that helps with Inserting and Reading ranges from a cache
//===----------------------------------------------------------------------===//

static BlobCacheFileRange *AnalyzeRange(map<idx_t, unique_ptr<BlobCacheFileRange>> &ranges, idx_t position,
                                        idx_t &max_nr_bytes) {
	auto it = ranges.upper_bound(position);
	BlobCacheFileRange *hit_range = nullptr;

	if (it != ranges.begin()) { // is there a range that starts <= start_pos?
		auto &prev_range = std::prev(it)->second;
		if (prev_range->range_end > position) { // it covers the start of this range
			// Range is usable if: (1) memcache_buffer exists (in ExternalFileCache), OR (2) disk write completed
			if (prev_range->memcache_buffer || prev_range->disk_write_completed.load(std::memory_order_acquire)) {
				hit_range = prev_range.get();
			}
		}
	}
	if (it != ranges.end() && it->second->range_start < position + max_nr_bytes) {
		max_nr_bytes = it->second->range_start - position; // cut short our range because the end is already cached now
	}
	return hit_range;
}

idx_t BlobCache::ReadFromCache(const string &cache_key, const string &filename, idx_t position, void *buffer,
                               idx_t &max_nr_bytes) {
	BlobCacheType blobcache_type = // Determine which blobcache to use based on request size
	    (max_nr_bytes <= config.small_range_threshold) ? BlobCacheType::SMALL_RANGE : BlobCacheType::LARGE_RANGE;
	BlobCacheMap &blobcache = GetCacheMap(blobcache_type);

	std::unique_lock<std::mutex> lock(blobcache_mutex); // lock caches

	BlobCacheFile *blobcache_file = blobcache.FindFile(cache_key, filename);
	if (!blobcache_file) {
		return 0; // nothing cached for this file
	}
	blobcache.TouchLRU(blobcache_file);
	auto hit_range = AnalyzeRange(blobcache_file->ranges, position, max_nr_bytes);
	if (!hit_range) {
		return 0; // some data is cached, but there is no overlapping range
	}
	idx_t hit_size = std::min(max_nr_bytes, hit_range->range_end - position);
	idx_t offset = position - hit_range->range_start;
	hit_range->bytes_from_cache += hit_size;
	hit_range->usage_count++;

	// CRITICAL: Copy everything we need before unlocking
	idx_t cached_blobcache_range_start = hit_range->blobcache_range_start;
	auto file_id = blobcache_file->file_id;
	idx_t saved_range_start = hit_range->range_start; // Save range start to relocate it after reacquiring lock
	lock.unlock(); // do the longer-running things unlocked (note: blobcache_file might get deleted!)

	// Always read via ReadFromCacheFile, which tries memcache first, then disk
	idx_t bytes_from_mem = 0;
	string blobcache_filepath = config.GenCacheFilePath(file_id, cache_key, blobcache_type);
	if (!blobcache.ReadFromCacheFile(blobcache_filepath, cached_blobcache_range_start + offset, buffer,
	                                 hit_size, bytes_from_mem)) { // hit_size may be reduced even on success
		hit_size = 0; // read from cached file failed! -- can be legal as it could have been deleted by eviction
	}

	// If we had a memory blobcache hit, update the bytes_from_mem counter
	if (bytes_from_mem > 0) {
		lock.lock(); // Reacquire lock and update range's bytes_from_mem counter (handle eviction race)
		blobcache_file = blobcache.FindFile(cache_key, filename);
		if (blobcache_file && blobcache_file->file_id == file_id) {
			auto range_it = blobcache_file->ranges.find(saved_range_start);
			if (range_it != blobcache_file->ranges.end()) {
				range_it->second->bytes_from_mem += bytes_from_mem;
			}
		}
		lock.unlock();
	}
	return hit_size; // No blobcache hit, return 0 to indicate wrapped_fs should be used
}

// we had to read from the original source (e.g. S3). Now try to cache this buffer in the disk-based blobcache
void BlobCache::InsertCache(const string &cache_key, const string &filename, idx_t position, void *buffer,
                            idx_t max_nr_bytes) {
	if (!config.blobcache_initialized || !max_nr_bytes ||
	    static_cast<idx_t>(max_nr_bytes) > config.total_cache_capacity) {
		return;
	}
	BlobCacheType blobcache_type = // Determine blobcache type based on original request size
	    (max_nr_bytes <= config.small_range_threshold) ? BlobCacheType::SMALL_RANGE : BlobCacheType::LARGE_RANGE;
	BlobCacheMap &blobcache = GetCacheMap(blobcache_type);

	std::lock_guard<std::mutex> lock(regex_mutex);
	auto blobcache_file = blobcache.UpsertFile(cache_key, filename);
	if (!blobcache_file) {
		return; // name collision (rare)
	}
	// Check (under lock) if range already cached (in the meantime, due to concurrent reads)
	auto hit_range = AnalyzeRange(blobcache_file->ranges, position, max_nr_bytes);
	idx_t offset = 0, actual_bytes = 0, range_start_pos = position, range_end_pos = range_start_pos + max_nr_bytes;
	if (hit_range) { // another thread cached the same range in the meantime
		offset = hit_range->range_end - range_start_pos;
		range_start_pos = hit_range->range_end; // blobcache only from the end
	}
	if (range_end_pos > range_start_pos) {
		actual_bytes = range_end_pos - range_start_pos;
	}
	if (actual_bytes == 0 || !EvictToCapacity(actual_bytes, filename)) {
		return; // back off, do not cache this range
	}

	// Create new range and update stats
	auto new_range = make_uniq<BlobCacheFileRange>(range_start_pos, range_end_pos);
	BlobCacheFileRange *range_ptr = new_range.get(); // Get pointer before moving
	blobcache_file->ranges[range_start_pos] = std::move(new_range);
	blobcache_file->cached_file_size += actual_bytes;
	blobcache.current_size += actual_bytes;
	blobcache.num_ranges++;

	// Pre-compute blobcache_range_start (we append sequentially, so we know the offset in advance)
	range_ptr->blobcache_range_start = blobcache_file->current_blobcache_file_offset;
	blobcache_file->current_blobcache_file_offset += actual_bytes; // Update for next write

	// Generate blobcache filepath
	string blobcache_filepath = config.GenCacheFilePath(blobcache_file->file_id, cache_key, blobcache_type);

	// Allocate from BufferManager (for memory caching) and copy data outside lock
	auto &buffer_manager = blobfile_memcache->GetBufferManager();
	auto buffer_handle = buffer_manager.Allocate(MemoryTag::EXTERNAL_FILE_CACHE, actual_bytes);
	std::memcpy(buffer_handle.Ptr(), static_cast<const char *>(buffer) + offset, actual_bytes);

	// Now register this cached piece of blobcache file in the memcache (even though it is not written yet)
	InsertRangeIntoMemcache(blobcache_filepath, range_ptr->blobcache_range_start, buffer_handle, actual_bytes);

	// schedule the writing of this new ranfe
	auto file_buffer =
	    duckdb::make_shared_ptr<BlobCacheFileBuffer>(std::move(buffer_handle), actual_bytes, cache_key, filename);
	file_buffer->disk_write_completed_ptr = &range_ptr->disk_write_completed; // Set backpointer for write completion
	range_ptr->memcache_buffer = file_buffer;                                 // Store buffer for memory blobcache hits
	string hex_prefix = cache_key.substr(4, 2); // the first 4 hex is the dir, take the next 2 for the partition
	idx_t partition = std::stoul(hex_prefix, nullptr, 16) % num_io_threads;
	QueueIOWrite(blobcache_filepath, partition, file_buffer); // writing to disk is done by background threads
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
	config.LogDebug("Inserted into memcache: '" + blobcache_filepath + "' at offset " +
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
	auto &memcache_range = *it->second;
	if (it == memcache_ranges.end() || memcache_range.nr_bytes == 0) {
		return false; // mem-cached range not found or empty
	}
	config.LogDebug("Memory cache hit for " +  to_string(length) + " bytes in '" + blobcache_filepath + "',  offset "
	                + to_string(blobcache_range_start) + " with length " + to_string(memcache_range.nr_bytes));
	auto &buffer_manager = blobfile_memcache->GetBufferManager();
	auto pin = buffer_manager.Pin(memcache_range.block_handle);
	if (!pin.IsValid()) { // Try to pin the block handle
		config.LogDebug("Pinning cache hit failed -- apparently there is high memory pressure");
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
	{ // Use first 2 hex chars from filepath for partitioning
		std::lock_guard<std::mutex> lock(io_mutexes[partition]);
		io_queues[partition].emplace(filepath, buffer);
	}
	io_cvs[partition].notify_one();
}

void BlobCache::StartIOThreads(idx_t thread_count) {
	if (thread_count > MAX_IO_THREADS) {
		thread_count = MAX_IO_THREADS;
		config.LogDebug("Limiting IO threads to maximum allowed: " + std::to_string(MAX_IO_THREADS));
	}
	shutdown_io_threads = false;
	num_io_threads = thread_count;

	config.LogDebug("Starting " + std::to_string(num_io_threads) + " blobcache IO threads");

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
		config.LogDebug("Stopped " + std::to_string(num_io_threads) + " cache writer threads");
	}
	num_io_threads = 0; // Reset thread count
}

void BlobCache::MainIOThreadLoop(idx_t thread_id) {
	config.LogDebug("Cache writer thread " + std::to_string(thread_id) + " started");
	while (!shutdown_io_threads) {
		pair<string, duckdb::shared_ptr<BlobCacheFileBuffer>> job;
		bool has_job = false;
		{ // Wait for a job or shutdown signal for this thread's queue
			std::unique_lock<std::mutex> lock(io_mutexes[thread_id]);
			io_cvs[thread_id].wait(lock,
			                       [this, thread_id] { return !io_queues[thread_id].empty() || shutdown_io_threads; });

			if (shutdown_io_threads && io_queues[thread_id].empty()) {
				break;
			}
			if (!io_queues[thread_id].empty()) {
				job = std::move(io_queues[thread_id].front());
				io_queues[thread_id].pop();
				has_job = true;
			}
		}
		if (!has_job) {
			continue;
		}
		// Process the cache write job
		const string &blobcache_filepath = job.first;
		auto file_buffer = job.second;

		// Determine cache type from filename: /cachedir/1234/567890ABCDEFsID:FILE:PROT -> 's' means SMALL_RANGE
		size_t last_sep = blobcache_filepath.find_last_of(config.path_sep);
		char type_char = blobcache_filepath[last_sep + 12 + 1]; // after / there are 12 hex chars and then 's' or 'l'
		BlobCacheType cache_type = (type_char == 's') ? BlobCacheType::SMALL_RANGE : BlobCacheType::LARGE_RANGE;
		BlobCacheMap &cache = GetCacheMap(cache_type);

		idx_t blobcache_range_start;
		if (cache.WriteToCacheFile(blobcache_filepath, file_buffer->buffer_handle.Ptr(), file_buffer->size,
		                           blobcache_range_start)) {
			// Mark disk write as completed via backpointer
			if (file_buffer->disk_write_completed_ptr) {
				file_buffer->disk_write_completed_ptr->store(true, std::memory_order_release);
			}
			config.LogDebug("Background writer completed write for blobcache_filepath '" + blobcache_filepath + "'");
		} else {
			// Write failed - log error (memcache has stale data, but eviction is unsafe from background thread)
			config.LogError("Failed to write to blobcache file '" + blobcache_filepath + "'");
		}
	}
	// Only log thread shutdown if not during database shutdown to avoid access to destroyed instance
	if (!config.blobcache_shutting_down) {
		config.LogDebug("Cache writer thread " + std::to_string(thread_id) + " stopped");
	}
}

//===----------------------------------------------------------------------===//
// BlobCacheMap - eviction logic
//===----------------------------------------------------------------------===//

bool BlobCacheMap::EvictToCapacity(idx_t required_space, BlobCacheType cache_type, const string &exclude_filename) {
	// Try to evict files to make space, returns true if successful
	// Skip files with unfinished writes and the exclude_key
	// Note: This is called with blobcache_mutex already held
	idx_t freed_space = 0;
	BlobCacheFile *file_to_evict = nullptr;
	while (required_space > freed_space && lru_tail && lru_tail != file_to_evict) {
		file_to_evict = lru_tail;
		if (file_to_evict->filename == exclude_filename) {
			config.LogDebug("Skipping eviction of'" + exclude_filename + "'");
			TouchLRU(file_to_evict); // Move it to front of LRU so we try something else
			continue;
		}
		freed_space += EvictCacheKey(config.GenCacheKey(file_to_evict->filename), cache_type);
	}
	if (freed_space < required_space) {
		config.LogError("Cannot evict below " + std::to_string(current_size) + " to make room for " +
		                std::to_string(required_space) + " bytes");
		return false;
	}
	return true;
}

size_t BlobCacheMap::EvictCacheKey(const string &cache_key, BlobCacheType cache_type) {
	idx_t evicted_bytes = 0;
	auto it = key_cache->find(cache_key);
	if (it == key_cache->end()) {
		config.LogError("Evictkey:  '" + cache_key + "' -not found");
		return 0;
	}
	for (auto &range : it->second->ranges) {
		evicted_bytes += range.second->range_end - range.second->range_start;
	}
	// evict - ranges_mutex is locked so no other thread is using this file
	RemoveFromLRU(it->second.get());
	current_size -= std::min<idx_t>(current_size, evicted_bytes);
	num_ranges -= std::min<idx_t>(num_ranges, it->second->ranges.size());
	auto file_path = config.GenCacheFilePath(it->second->file_id, cache_key, cache_type);
	key_cache->erase(it);
	return DeleteCacheFile(file_path) ? evicted_bytes : 0;
}

void BlobCacheMap::PurgeCacheForPatternChange(const vector<std::regex> &regexps, optional_ptr<FileOpener> opener,
                                              BlobCacheType cache_type) {
	// First pass: collect keys to evict (avoid iterator invalidation)
	vector<string> keys_to_evict;
	for (const auto &cache_pair : *key_cache) { // Check all files in this cache
		auto should_cache = false;              // Check if this file should still be cached
		if (regexps.empty()) {
			// Conservative mode: only cache .parquet files when parquet_metadata_cache=true
			if (opener && StringUtil::EndsWith(StringUtil::Lower(cache_pair.second->filename), ".parquet")) {
				Value parquet_cache_value;
				auto parquet_result =
				    FileOpener::TryGetCurrentSetting(opener, "parquet_metadata_cache", parquet_cache_value);
				should_cache = (parquet_result && BooleanValue::Get(parquet_cache_value));
			}
		} else { // Aggressive mode: use regex patterns
			for (const auto &pattern : regexps) {
				if (std::regex_search(cache_pair.second->filename, pattern)) {
					should_cache = true;
					break;
				}
			}
		}
		if (!should_cache) {
			keys_to_evict.push_back(cache_pair.first);
		}
	}
	// Second pass: evict collected keys (safe, no iterator invalidation)
	for (const auto &key : keys_to_evict) {
		EvictCacheKey(key, cache_type);
	}
}

vector<BlobCacheRangeInfo>
BlobCacheMap::GetStatistics() const { // produce list of cached ranges for blobcache_stats() TF
	vector<BlobCacheRangeInfo> result;
	result.reserve(num_ranges);
	BlobCacheFile *current = lru_tail;
	while (current) {
		BlobCacheRangeInfo info;
		info.protocol = "unknown";
		info.filename = current->filename;
		auto pos = info.filename.find("://");
		if (pos != string::npos) {
			info.protocol = info.filename.substr(0, pos);
			info.filename = info.filename.substr(pos + 3, info.filename.length() - (pos + 3));
		}
		for (const auto &range_pair : current->ranges) {
			info.blobcache_range_start = range_pair.second->blobcache_range_start;
			info.range_start = range_pair.second->range_start;
			info.range_size = range_pair.second->range_end - range_pair.second->range_start;
			info.usage_count = range_pair.second->usage_count;
			info.bytes_from_cache = range_pair.second->bytes_from_cache;
			info.bytes_from_mem = range_pair.second->bytes_from_mem;
			result.push_back(info);
		}
		current = current->lru_prev;
	}
	return result;
}

//===----------------------------------------------------------------------===//
// BlobCacheMap file management
//===----------------------------------------------------------------------===//

void BlobCacheMap::EnsureSubdirectoryExists(const string &cache_filepath) {
	if (!config.db_instance) {
		return;
	}
	// Extract subdirectory from cache_filepath
	size_t last_sep = cache_filepath.find_last_of("/\\");
	if (last_sep == string::npos) {
		return; // Format: /cachedir/1234/s567890 -> apparently /cachedir/1234/ does not exist
	}
	// Extract just the subdir name for hashing (e.g., "1234")
	string subdir_path = cache_filepath.substr(0, last_sep);
	size_t prev_sep = subdir_path.find_last_of(config.path_sep);
	string subdir_name = (prev_sep != string::npos) ? subdir_path.substr(prev_sep + 1) : subdir_path;
	uint16_t subdir_index = std::hash<string> {}(subdir_name) % config.subdirs_created.size();
	if (config.subdirs_created[subdir_index]) {
		return; // Already exists
	}
	try {
		auto &fs = config.GetFileSystem();
		if (!fs.DirectoryExists(subdir_path)) {
			fs.CreateDirectory(subdir_path);
			config.LogDebug("Created cache subdirectory '" + subdir_path + "'");
		}
		config.subdirs_created[subdir_index] = true;
	} catch (const std::exception &e) {
		config.LogError("Failed to create cache subdirectory '" + subdir_path + "': " + string(e.what()));
	}
}

unique_ptr<FileHandle> BlobCacheMap::TryOpenCacheFile(const string &cache_filepath) {
	if (!config.db_instance) {
		return nullptr;
	}
	try {
		auto &fs = config.GetFileSystem();
		return fs.OpenFile(cache_filepath, FileOpenFlags::FILE_FLAGS_READ);
	} catch (const std::exception &e) {
		// File was evicted between metadata check and open - this is expected
		config.LogDebug("Cache file not found (likely evicted): '" + cache_filepath + "'");
		return nullptr;
	}
}

bool BlobCacheMap::ReadFromCacheFile(const string &blobcache_filepath, idx_t blobcache_range_start, void *buffer,
                                     idx_t &length, idx_t &out_bytes_from_mem) {
	if (config.parent_cache->TryReadFromMemcache(blobcache_filepath, blobcache_range_start, buffer, length)) {
		out_bytes_from_mem = length;
		return true; // reading from memcache first succeeded
	}
	// Not in memory cache - read from disk and cache it
	out_bytes_from_mem = 0; // Initialize to 0 (disk read)
	auto handle = TryOpenCacheFile(blobcache_filepath);
	if (!handle) {
		return false; // File was evicted or doesn't exist - signal cache miss
	}
	// allocate memory using the DuckDB buffer manager
	auto &buffer_manager = config.parent_cache->blobfile_memcache->GetBufferManager();
	auto buffer_handle = buffer_manager.Allocate(MemoryTag::EXTERNAL_FILE_CACHE, length);
	auto buffer_ptr = buffer_handle.Ptr();
	config.GetFileSystem().Read(*handle, buffer_ptr, length, blobcache_range_start); // Read from disk into buffer
	std::memcpy(buffer, buffer_ptr, length);                                         // Copy to output buffer
	config.parent_cache->InsertRangeIntoMemcache(blobcache_filepath, blobcache_range_start, buffer_handle, length);
	return true;
}

bool BlobCacheMap::WriteToCacheFile(const string &blobcache_filepath, const void *buffer, idx_t length,
                                    idx_t &blobcache_range_start) {
	if (!config.db_instance)
		return false;

	EnsureSubdirectoryExists(blobcache_filepath);
	try {
		auto &fs = config.GetFileSystem();
		auto flags = // Open file for writing in append mode (create if not exists)
		    FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE | FileOpenFlags::FILE_FLAGS_APPEND;
		auto handle = fs.OpenFile(blobcache_filepath, flags);
		if (!handle) {
			config.LogError("Failed to open blobcache file for writing: '" + blobcache_filepath + "'");
			return false;
		}
		// Get current file size to know where we're appending
		blobcache_range_start = static_cast<idx_t>(fs.GetFileSize(*handle));
		int64_t bytes_written = fs.Write(*handle, const_cast<void *>(buffer), length);
		handle->Close(); // Close handle explicitly
		if (bytes_written != static_cast<int64_t>(length)) {
			config.LogError("Failed to write all bytes to blobcache file (wrote " + std::to_string(bytes_written) +
			                " of " + std::to_string(length) + ")");
			return false;
		}
	} catch (const std::exception &e) {
		config.LogError("Failed to write to blobcache file '" + blobcache_filepath + "': " + string(e.what()));
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
		config.LogDebug("Deleted cache file '" + cache_filepath + "'");
		return true;
	} catch (const std::exception &e) {
		config.LogError("Failed to delete cache file '" + cache_filepath + "': " + string(e.what()));
		return false;
	}
}

//===----------------------------------------------------------------------===//
// BlobCacheMap LRU management methods
//===----------------------------------------------------------------------===//
void BlobCacheMap::TouchLRU(BlobCacheFile *cache_file) {
	// Move cache_file to front of LRU list (most recently used)
	// Note: Must be called with blobcache_mutex held
	if (cache_file == lru_head) {
		return; // Already at front
	}
	RemoveFromLRU(cache_file); // Remove from current position
	AddToLRUFront(cache_file); // Add to front
}

void BlobCacheMap::RemoveFromLRU(BlobCacheFile *cache_file) {
	// Remove cache_file from LRU list
	// Note: Must be called with blobcache_mutex held
	if (cache_file->lru_prev) {
		cache_file->lru_prev->lru_next = cache_file->lru_next;
	} else {
		lru_head = cache_file->lru_next;
	}
	if (cache_file->lru_next) {
		cache_file->lru_next->lru_prev = cache_file->lru_prev;
	} else {
		lru_tail = cache_file->lru_prev;
	}
	cache_file->lru_prev = nullptr;
	cache_file->lru_next = nullptr;
}

void BlobCacheMap::AddToLRUFront(BlobCacheFile *cache_file) {
	// Add cache_file to front of LRU list (most recently used)
	// Note: Must be called with blobcache_mutex held
	cache_file->lru_prev = nullptr;
	cache_file->lru_next = lru_head;
	if (lru_head) {
		lru_head->lru_prev = cache_file;
	}
	lru_head = cache_file;
	if (!lru_tail) {
		lru_tail = cache_file;
	}
}

//===----------------------------------------------------------------------===//
// BlobCache eviction from both (small,large) range caches
//===----------------------------------------------------------------------===//

bool BlobCache::EvictToCapacity(idx_t extra_bytes, const string &exclude_filename) {
	// note: mutex should already be held
	// Evict from large cache first (since it gets 90% capacity)
	idx_t large_capacity = GetCacheCapacity(BlobCacheType::LARGE_RANGE);
	auto result = true;
	if (largerange_blobcache->current_size + extra_bytes > large_capacity) {
		if (!largerange_blobcache->EvictToCapacity(largerange_blobcache->current_size + extra_bytes - large_capacity,
		                                           BlobCacheType::LARGE_RANGE, exclude_filename)) {
			result = false;
		} else {
			extra_bytes = 0;
		}
	}
	// Then evict from small cache if needed
	idx_t small_capacity = GetCacheCapacity(BlobCacheType::SMALL_RANGE);
	if (smallrange_blobcache->current_size + extra_bytes > small_capacity) {
		result &=
		    smallrange_blobcache->EvictToCapacity(smallrange_blobcache->current_size + extra_bytes - small_capacity,
		                                          BlobCacheType::SMALL_RANGE, exclude_filename);
	}
	return result;
}

//===----------------------------------------------------------------------===//
// BlobCache (re-) configuration
//===----------------------------------------------------------------------===//

void
BlobCache::ConfigureCache(const string &base_dir, idx_t max_size_bytes, idx_t max_io_threads, idx_t small_threshold) {
	std::lock_guard<std::mutex> lock(blobcache_mutex);
	auto directory = base_dir + (StringUtil::EndsWith(base_dir, config.path_sep) ? "" : config.path_sep);
	if (!config.blobcache_initialized) {
		// Release lock before calling InitializeCache to avoid deadlock
		config.blobcache_dir = directory;
		config.total_cache_capacity = max_size_bytes;
		config.small_range_threshold = small_threshold;

		config.LogDebug("Initializing cache: directory='" + config.blobcache_dir +
		                "' max_size=" + std::to_string(config.total_cache_capacity) +
		                " bytes io_threads=" + std::to_string(max_io_threads) +
		                " small_threshold=" + std::to_string(config.small_range_threshold));
		if (!config.InitCacheDir()) {
			config.LogError("Initializing cache directory='" + config.blobcache_dir + "' failed");
		}
		Clear();
		config.blobcache_initialized = true;
		// Initialize our own ExternalFileCache instance (always enabled for memory caching)
		blobfile_memcache = make_uniq<ExternalFileCache>(*config.db_instance, true);
		config.LogDebug("Initialized blobfile_memcache for memory caching of disk-cached files");
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
		config.LogDebug("Cache configuration unchanged, no action needed");
		return;
	}

	// Stop existing threads if we need to change thread count or directory
	config.LogDebug("Configuring cache: old_dir='" + config.blobcache_dir + "' new_dir='" + directory + "' old_size=" +
	                std::to_string(config.total_cache_capacity) + " new_size=" + std::to_string(max_size_bytes)
	                + " old_threshold=" + std::to_string(config.small_range_threshold)
	                + " new_threshold=" + std::to_string(small_threshold) + " old_threads="
	                + std::to_string(num_io_threads) + " new_threads=" + std::to_string(max_io_threads));
	if (num_io_threads > 0 && (need_restart_threads || directory_changed)) {
		config.LogDebug("Stopping existing cache IO threads for reconfiguration");
		StopIOThreads();
	}

	// Clear existing cache only if directory changed or threshold changed
	if (directory_changed || threshold_changed) {
		config.LogDebug("Directory or threshold changed, clearing cache");
		Clear();
		if (directory_changed) {
			if (!config.CleanCacheDir()) { // Clean old directory before switching
				config.LogError("Cleaning cache directory='" + config.blobcache_dir + "' failed");
			}
		}
		config.blobcache_dir = directory;
		config.small_range_threshold = small_threshold;
		if (!config.InitCacheDir()) {
			config.LogError("Initializing cache directory='" + config.blobcache_dir + "' failed");
		}
		// Reinitialize blobfile_memcache when directory changes
		blobfile_memcache = make_uniq<ExternalFileCache>(*config.db_instance, true);
		config.LogDebug("Reinitialized blobfile_memcache after directory change");
	}
	// Same directory, just update capacity and evict if needed
	config.total_cache_capacity = max_size_bytes;
	if (size_reduced && !EvictToCapacity()) {
		config.LogError("Failed to reduce the directory sizes to the new lower capacity/");
	}
	// Start threads if they were stopped or thread count changed
	if (need_restart_threads || directory_changed) {
		StartIOThreads(max_io_threads);
	}
	config.LogDebug("Cache configuration complete: directory='" + config.blobcache_dir + "' max_size=" +
	                to_string(config.total_cache_capacity) + " bytes io_threads=" +
	                to_string(max_io_threads) + " small_threshold=" + to_string(config.small_range_threshold));
}

//===----------------------------------------------------------------------===//
// caching policy based on regexps
//===----------------------------------------------------------------------===//

void BlobCache::UpdateRegexPatterns(const string &regex_patterns_str) {
	std::lock_guard<std::mutex> lock(regex_mutex);

	cached_regexps.clear(); // Clear existing patterns
	if (regex_patterns_str.empty()) {
		// Conservative mode: empty regexps
		config.LogDebug("Updated to conservative mode (empty regex patterns)");
		return;
	}
	// Aggressive mode: parse semicolon-separated patterns
	vector<string> pattern_strings = StringUtil::Split(regex_patterns_str, ';');
	for (const auto &pattern_str : pattern_strings) {
		if (!pattern_str.empty()) {
			try {
				cached_regexps.emplace_back(pattern_str, std::regex_constants::icase);
				config.LogDebug("Compiled regex pattern: '" + pattern_str + "'");
			} catch (const std::regex_error &e) {
				config.LogError("Invalid regex pattern '" + pattern_str + "': " + string(e.what()));
			}
		}
	}
	config.LogDebug("Updated to aggressive mode with " + std::to_string(cached_regexps.size()) + " regex patterns");
}

bool BlobCache::ShouldCacheFile(const string &filename, optional_ptr<FileOpener> opener) const {
	std::lock_guard<std::mutex> lock(regex_mutex);
	if (StringUtil::StartsWith(StringUtil::Lower(filename), "file://")) {
		return false; // Never cache file:// URLs as they are already local
	}
	if (StringUtil::StartsWith(StringUtil::Lower(filename), "debug://")) {
		return true; // Always cache debug:// URLs for testing
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
