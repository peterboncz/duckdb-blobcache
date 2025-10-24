#include "blobcache.hpp"

namespace duckdb {

static inline bool IsRangeStale(const BlobCacheFileRange &range,
                                const unordered_map<string, unique_ptr<BlobCacheFile>> &file_cache) {
	auto cache_file_it = file_cache.find(range.file);
	return (cache_file_it == file_cache.end()); // file was deleted
}

//===----------------------------------------------------------------------===//
// AnalyzeRange that helps with Inserting and Reading ranges from a cache
// Now includes lazy deletion: removes ranges whose CacheFile has been deleted
//===----------------------------------------------------------------------===//

BlobCacheFileRange *AnalyzeRange(BlobCacheMap &map, const string &key, const string &uri, idx_t pos, idx_t &len) {
	BlobCacheEntry *blobcache_entry = map.FindFile(key, uri);
	if (!blobcache_entry || blobcache_entry->ranges.empty()) {
		return nullptr;
	}
	while (true) { // Lazy deletion loop: keep searching until we find a valid range or exhaust all stale ranges
		auto it = blobcache_entry->ranges.upper_bound(pos);
		BlobCacheFileRange *hit_range = nullptr;
		if (it != blobcache_entry->ranges.begin()) { // Check if the previous range covers our position
			auto prev_it = std::prev(it);
			auto &prev_range = prev_it->second;
			if (prev_range && prev_range->uri_range_end > pos) {
				if (IsRangeStale(*prev_range, *map.file_cache)) {
					blobcache_entry->ranges.erase(prev_it);
					continue; // Stale range found (CacheFile has been deleted) - delete it and loop again
				}
				if (prev_range->disk_write_completed) {
					hit_range = prev_range.get();
				}
			}
		}
		// Check the next range to see if we need to cut short max_nr_bytes
		if (it != blobcache_entry->ranges.end() && it->second) {
			// Check if this range is stale (CacheFile has been deleted)
			if (IsRangeStale(*it->second, *map.file_cache)) {
				blobcache_entry->ranges.erase(it);
				continue; // Stale range found (CacheFile has been deleted) - delete it and loop again
			}
			if (it->second->uri_range_start < pos + len) {
				len = it->second->uri_range_start - pos;
			}
		}
		return hit_range;
	}
}

idx_t BlobCache::ReadFromCache(const string &key, const string &uri, idx_t pos, idx_t &len, void *buf) {
	BlobCacheFileRange *hit_range = nullptr;
	idx_t orig_len = len, off = pos, hit_size = 0;

	// check the small and large range caches (where appropriate) if the prefix of this request is cached
	std::unique_lock<std::mutex> lock(blobcache_mutex);
	auto map = smallrange_blobcache.get();
	if (len < BlobCacheState::SMALL_RANGE_THRESHOLD) {
		hit_range = AnalyzeRange(*map, key, uri, off, len); // may adjust len downward to match a next range
	}
	if (!hit_range) {
		map = largerange_blobcache.get();
		hit_range = AnalyzeRange(*map, key, uri, off, len); // may adjust len downward to match a next range
	}
	if (hit_range) {
		hit_size = std::min(orig_len, hit_range->uri_range_end - pos);
	}
	if (hit_size == 0) {
		lock.unlock();
		return 0;
	}

	// the read we want to do at 'pos' finds a hit of its 'hit_size' first bytes in 'hit_range'
	// it may have reduced the following non-cached read until 'len' to a next cached range
	hit_range->bytes_from_cache += hit_size;
	hit_range->usage_count++;
	auto cache_file_it = map->file_cache->find(hit_range->file);
	if (cache_file_it != map->file_cache->end()) {
		map->TouchLRU(cache_file_it->second.get());
	}

	// save the critical values before unlock (after unlock, hit_range* might get deallocated in an concurrent evict)
	string file = hit_range->file;
	idx_t uri_range_start = hit_range->uri_range_start;
	idx_t file_pos = hit_range->file_range_start + (pos - uri_range_start);
	lock.unlock();

	// Read from on-dick cache file unlocked
	idx_t bytes_from_mem = 0;
	if (!map->ReadFromCacheFile(file, file_pos, buf, hit_size, bytes_from_mem)) {
		return 0; // Read failed
	}

	if (bytes_from_mem > 0) { // Update bytes_from_mem counter if we had a memory hit
		lock.lock();
		auto blobcache_entry = map->FindFile(key, uri);
		if (blobcache_entry) {
			auto range_it = blobcache_entry->ranges.find(uri_range_start);
			if (range_it != blobcache_entry->ranges.end()) {
				range_it->second->bytes_from_mem += bytes_from_mem;
			}
		}
		lock.unlock();
	}
	return hit_size;
}

// we had to read from the original source (e.g. S3). Now try to cache this buffer in the disk-based blobcache
void BlobCache::InsertCache(const string &key, const string &uri, idx_t pos, idx_t len, void *buf) {
	if (!state.blobcache_initialized || len == 0 || len > state.total_cache_capacity) {
		return; // bail out if non initialized or impossible length
	}
	BlobCacheType cache_type = // Determine blobcache type based on original request size
	    (len < BlobCacheState::SMALL_RANGE_THRESHOLD) ? BlobCacheType::SMALL_RANGE : BlobCacheType::LARGE_RANGE;

	std::lock_guard<std::mutex> lock(regex_mutex);
	auto &cache_map = GetCacheMap(cache_type);
	auto cache_entry = cache_map.UpsertFile(key, uri);
	if (!cache_entry) {
		return; // name collision (rare)
	}
	// Check (under lock) if range already cached (in the meantime, due to concurrent reads)
	auto hit_range = AnalyzeRange(cache_map, key, uri, pos, len);
	idx_t offset = 0, final_size = 0, range_start = pos, range_end = range_start + len;
	if (hit_range) { // another thread cached the same range in the meantime
		offset = hit_range->uri_range_end - range_start;
		range_start = hit_range->uri_range_end; // cache only from the end
	}
	if (range_end > range_start) {
		final_size = range_end - range_start;
	}
	if (final_size == 0) {
		return; // nothing to cache
	}

	if (!EvictToCapacity(cache_type, final_size)) { // make sure we have room for 'final_bytes' extra data
		state.LogError("InsertCache: EvictToCapacity failed for " + to_string(final_size) +
		               " bytes (cache_type=" + (cache_type == BlobCacheType::LARGE_RANGE ? "large" : "small") +
		               ", large_size=" + to_string(largerange_blobcache->current_cache_size) +
		               ", small_size=" + to_string(smallrange_blobcache->current_cache_size) + ")");
		return; // failed to make room
	}

	// allocate a buffer and copy the data into it
	BufferHandle buffer_handle;
	if (!state.AllocateInMemCache(buffer_handle, final_size)) {
		state.LogError("InsertRangeInternal: AllocateInMemCache failed for " + to_string(final_size) + " bytes");
		return; // allocation from DDB buffer pool failed
	}
	std::memcpy(buffer_handle.Ptr(), static_cast<const char *>(buf) + offset, final_size);

	// Ensure the subdirectories exist for this key and cache type
	state.EnsureDirectoryExists(key, cache_type);

	// Get or create the CacheFile for this range
	auto &blobcache_map = GetCacheMap(cache_type);
	auto cache_file = blobcache_map.GetOrCreateCacheFile(cache_entry, key, cache_type, final_size);
	if (!cache_file) {
		state.LogError("InsertRangeInternal: failed to get or create cache file");
		return;
	}

	// Create file_buffer and new_range. They refer to each other..
	auto new_range = make_uniq<BlobCacheFileRange>(*cache_file, range_start, range_end);
	idx_t file_range_start = new_range->file_range_start;    // Capture before moving new_range
	cache_entry->ranges[range_start] = std::move(new_range); // transfer ownership to map

	// update stats
	cache_file->file_size += final_size;
	cache_entry->total_cached_size += final_size;
	blobcache_map.current_cache_size += final_size;
	blobcache_map.nr_ranges++;

	// Schedule the disk write, using thread partitioning based on file_id
	cache_file->ongoing_writes++;
	BlobCacheWriteJob job;
	job.file = cache_file->file;
	job.handle = std::move(buffer_handle);
	job.nr_bytes = final_size;
	job.disk_write_completed_ptr = &cache_entry->ranges[range_start]->disk_write_completed;

	// Register in memcache AFTER moving handle to job (memcache stores BlockHandle which stays valid)
	state.InsertRangeIntoMemcache(cache_file->file, file_range_start, job.handle, final_size);

	QueueIOWrite(job, cache_file->file_id % nr_io_threads);
}

//===----------------------------------------------------------------------===//
// Memory cache helpers
//===----------------------------------------------------------------------===//

void BlobCacheState::InsertRangeIntoMemcache(const string &file, idx_t range_start, BufferHandle &handle, idx_t len) {
	auto &memcache_file = blobfile_memcache->GetOrCreateCachedFile(file);
	auto memcache_range =
	    make_shared_ptr<ExternalFileCache::CachedFileRange>(handle.GetBlockHandle(), len, range_start, "");
	auto lock_guard = memcache_file.lock.GetExclusiveLock();
	memcache_file.Ranges(lock_guard)[range_start] = memcache_range;
	LogDebug("InsertRangeIntoMemcache: inserted into memcache: '" + file + "' at offset " +
	         std::to_string(range_start) + " length " + std::to_string(len));
}

bool BlobCacheState::TryReadFromMemcache(const string &file, idx_t range_start, void *buf, idx_t &len) {
	if (!blobfile_memcache) {
		return false;
	}
	// Check if the range is already cached in memory
	auto &memcache_file = blobfile_memcache->GetOrCreateCachedFile(file);
	auto memcache_ranges_guard = memcache_file.lock.GetSharedLock();
	auto &memcache_ranges = memcache_file.Ranges(memcache_ranges_guard);
	auto it = memcache_ranges.find(range_start);
	if (it == memcache_ranges.end()) {
		return false; // Range not found in memory cache
	}
	auto &memcache_range = *it->second;
	if (memcache_range.nr_bytes == 0) {
		return false; // Empty range
	}
	LogDebug("TryReadFromMemcache: memcache hit for " + to_string(len) + " bytes in '" + file + "',  offset " +
	         to_string(range_start) + " length " + to_string(memcache_range.nr_bytes));
	auto &buffer_manager = blobfile_memcache->GetBufferManager();
	auto pin = buffer_manager.Pin(memcache_range.block_handle);
	if (!pin.IsValid()) {
		LogDebug("TryReadFromMemcache: pinning cache hit failed -- apparently there is high memory pressure");
		return false;
	}
	if (memcache_range.nr_bytes < len) {
		len = memcache_range.nr_bytes;
	}
	std::memcpy(buf, pin.Ptr(), len); // Memory hit - read from BufferHandle
	return true;
}

//===----------------------------------------------------------------------===//
// Multi-threaded background cache writer implementation
//===----------------------------------------------------------------------===//

void BlobCache::QueueIOWrite(BlobCacheWriteJob &job, idx_t partition) {
	{
		std::lock_guard<std::mutex> lock(io_mutexes[partition]);
		write_queues[partition].emplace(std::move(job));
	}
	io_cvs[partition].notify_one();
}

void BlobCache::QueueIORead(BlobCacheReadJob &job) {
	// Round-robin assignment across all threads (no partitioning needed for reads)
	idx_t target_thread = read_job_counter.fetch_add(1, std::memory_order_relaxed) % nr_io_threads;
	{
		std::lock_guard<std::mutex> lock(io_mutexes[target_thread]);
		read_queues[target_thread].emplace(std::move(job));
	}
	io_cvs[target_thread].notify_one();
}

void BlobCache::StartIOThreads(idx_t thread_count) {
	if (thread_count > MAX_IO_THREADS) {
		thread_count = MAX_IO_THREADS;
		state.LogDebug("StartIOThreads: limiting IO threads to maximum allowed: " + std::to_string(MAX_IO_THREADS));
	}
	shutdown_io_threads = false;
	nr_io_threads = thread_count;

	state.LogDebug("StartIOThreads: starting " + std::to_string(nr_io_threads) + " blobcache IO threads");

	for (idx_t i = 0; i < nr_io_threads; i++) {
		io_threads[i] = std::thread([this, i] { MainIOThreadLoop(i); });
	}
}

void BlobCache::StopIOThreads() {
	if (nr_io_threads == 0) {
		return; // Skip if no threads are running
	}
	shutdown_io_threads = true; // Signal shutdown to all threads

	// Notify all threads to wake up and check shutdown flag
	for (idx_t i = 0; i < nr_io_threads; i++) {
		io_cvs[i].notify_all();
	}
	// Wait for all threads to finish gracefully
	for (idx_t i = 0; i < nr_io_threads; i++) {
		if (io_threads[i].joinable()) {
			try {
				io_threads[i].join();
			} catch (const std::exception &) {
				// Ignore join errors during shutdown - thread may have already terminated
			}
		}
	}
	// Only log if not shutting down
	if (!state.blobcache_shutting_down) {
		state.LogDebug("StopIOThreads: stopped " + std::to_string(nr_io_threads) + " cache writer threads");
	}
	nr_io_threads = 0; // Reset thread count
}

void BlobCache::ProcessWriteJob(BlobCacheWriteJob &job) {
	// Determine cache type from uri format:
	// Small: /cachedir/{2hex}/small{id}
	// Large: /cachedir/{2hex}/{2hex}/{13hex}{uri_range_start}_{file_id}{suffix}
	size_t last_sep = job.file.find_last_of(state.path_sep);
	string uri_part = job.file.substr(last_sep + 1);
	BlobCacheType cache_type =
	    StringUtil::StartsWith(uri_part, "small") ? BlobCacheType::SMALL_RANGE : BlobCacheType::LARGE_RANGE;
	BlobCacheMap &cache = GetCacheMap(cache_type);

	bool write_success = cache.WriteToCacheFile(job.file, job.handle.Ptr(), job.nr_bytes);
	if (write_success) {
		*job.disk_write_completed_ptr = true; // Safe: ongoing_writes prevents CacheFile eviction
	} else {
		state.LogError("ProcessWriteJob: write failed for '" + job.file + "' - removing file");
	}
	job.handle.Destroy(); // Unpin buffer - allows buffer manager to evict if needed

	// Decrement ongoing_writes and handle failures under mutex
	std::lock_guard<std::mutex> lock(blobcache_mutex);
	// Look up the CacheFile to decrement ongoing_writes
	auto file_it = cache.file_cache->find(job.file);
	if (file_it == cache.file_cache->end()) { // not found??
		state.LogError("ProcessWriteJob: '" + job.file + "' was found missing while having " +
		               to_string(file_it->second->ongoing_writes) + " ongoing writes");
	} else if (write_success) { // normal path
		file_it->second->ongoing_writes--;
	} else { // Write failed - must delete CacheFile as partial writes would corrupt file offsets
		cache.RemoveCacheFile(file_it->second.get());
	}
}

void BlobCache::ProcessReadJob(BlobCacheReadJob &job) {
	try {
		// Open file and allocate buffer
		auto &fs = FileSystem::GetFileSystem(*state.db_instance);
		auto handle = fs.OpenFile(job.uri, FileOpenFlags::FILE_FLAGS_READ);
		auto buffer = unique_ptr<char[]>(new char[job.range_size]);

		// Read data from file
		fs.Read(*handle, buffer.get(), job.range_size, job.range_start);

		// Insert into cache (this will queue a write job)
		InsertCache(job.key, job.uri, job.range_start, job.range_size, buffer.get());
	} catch (const std::exception &e) {
		state.LogError("ProcessReadJob: failed to read '" + job.uri + "' at " + to_string(job.range_start) + ": " +
		               string(e.what()));
	}
}

void BlobCache::MainIOThreadLoop(idx_t thread_id) {
	state.LogDebug("MainIOThreadLoop " + std::to_string(thread_id) + " started");
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
	if (!state.blobcache_shutting_down) {
		state.LogDebug("MainIOThreadLoop " + std::to_string(thread_id) + " stopped");
	}
}

//===----------------------------------------------------------------------===//
// BlobCacheMap - CacheFile management
//===----------------------------------------------------------------------===//

BlobCacheFile *BlobCacheMap::GetOrCreateCacheFile(BlobCacheEntry *cache_entry, const string &key,
                                                  BlobCacheType cache_type, idx_t range_size) {
	// Note: Must be called with blobcache_mutex held
	if (cache_type == BlobCacheType::SMALL_RANGE && cache_entry->total_cached_size + range_size <= 256 * 1024) {
		auto file = state.GenCacheFilePath(current_file_id, key, cache_type);
		auto file_it = file_cache->find(file);
		if (file_it != file_cache->end()) {
			auto cache_file = file_it->second.get();
			cache_entry->total_cached_size += range_size;
			TouchLRU(cache_file);
			state.LogDebug("GetOrCreateCacheFile: append range of " + to_string(range_size) + " to " + file);
			return cache_file;
		}
	}
	// Create a new CacheFile
	auto file = state.GenCacheFilePath(++current_file_id, key, cache_type);
	auto new_cache_file = make_uniq<BlobCacheFile>(file, current_file_id);
	auto cache_file_ptr = new_cache_file.get();
	(*file_cache)[file] = std::move(new_cache_file);
	AddToLRUFront(cache_file_ptr);
	state.LogDebug("GetOrCreateCacheFile: create " + file + " for range of " + to_string(range_size));
	return cache_file_ptr;
}

//===----------------------------------------------------------------------===//
// BlobCacheMap - eviction logic
//===----------------------------------------------------------------------===//

bool BlobCacheMap::EvictToCapacity(idx_t required_space) {
	// Try to evict CacheFiles to make space, returns true if successful
	// Note: This is called with blobcache_mutex already held
	idx_t freed_space = 0;
	auto *current = lru_tail; // Start from least recently used
	idx_t files_checked = 0;
	idx_t files_skipped_excluded = 0;
	idx_t max_files = file_cache->size() + 1; // Safety limit to prevent infinite loops

	while (required_space > freed_space && current && files_checked < max_files) {
		files_checked++;

		// Skip files with ongoing writes
		if (current->ongoing_writes > 0) {
			files_skipped_excluded++;
			current = current->lru_prev;
			continue;
		}

		// Save next candidate before we potentially evict current
		auto *next_to_try = current->lru_prev;

		// Evict this CacheFile using helper method
		idx_t evicted_bytes = current->file_size;
		RemoveCacheFile(current);
		freed_space += evicted_bytes;

		current = next_to_try; // Move to next file in LRU
	}

	if (files_checked >= max_files) {
		state.LogError("EvictToCapacity: hit safety limit after checking " + std::to_string(files_checked) + " files");
	}

	if (freed_space < required_space) {
		// Count total files in file_cache for diagnostics
		idx_t total_files = file_cache ? file_cache->size() : 0;
		state.LogError(
		    "EvictToCapacity: needed " + std::to_string(required_space) + " bytes but only freed " +
		    std::to_string(freed_space) + " bytes (current_cache_size=" + std::to_string(current_cache_size) +
		    ", total_files=" + std::to_string(total_files) + "', files_checked=" + std::to_string(files_checked) +
		    ", files_skipped_excluded=" + std::to_string(files_skipped_excluded) +
		    ", lru_head=" + (lru_head ? "present" : "null") + ", lru_tail=" + (lru_tail ? "present" : "null") + ")");
		return false;
	}
	return true;
}

vector<BlobCacheRangeInfo>
BlobCacheMap::GetStatistics() const { // produce list of cached ranges for blobcache_stats() TF
	vector<BlobCacheRangeInfo> result;
	result.reserve(nr_ranges);

	// Iterate through all CacheEntries (not LRU, since LRU is now for CacheFiles)
	for (const auto &cache_pair : *key_cache) {
		const auto &cache_entry = cache_pair.second;
		BlobCacheRangeInfo info;
		info.protocol = "unknown";
		info.uri = cache_entry->uri;
		auto pos = info.uri.find("://");
		if (pos != string::npos) {
			info.protocol = info.uri.substr(0, pos);
			info.uri = info.uri.substr(pos + 3, info.uri.length() - (pos + 3));
		}
		for (const auto &range_pair : cache_entry->ranges) {
			// Skip stale ranges (whose CacheFile has been deleted)
			if (IsRangeStale(*range_pair.second, *file_cache)) {
				continue;
			}
			info.file = range_pair.second->file;
			info.file_range_start = range_pair.second->file_range_start;
			info.uri_range_start = range_pair.second->uri_range_start;
			info.uri_range_size = range_pair.second->uri_range_end - range_pair.second->uri_range_start;
			info.usage_count = range_pair.second->usage_count;
			info.bytes_from_cache = range_pair.second->bytes_from_cache;
			info.bytes_from_mem = range_pair.second->bytes_from_mem;
			result.push_back(info);
		}
	}
	return result;
}

//===----------------------------------------------------------------------===//
// BlobCacheMap file management
//===----------------------------------------------------------------------===//

unique_ptr<FileHandle> BlobCacheMap::TryOpenCacheFile(const string &file) {
	if (!state.db_instance) {
		return nullptr;
	}
	try {
		auto &fs = FileSystem::GetFileSystem(*state.db_instance);
		return fs.OpenFile(file, FileOpenFlags::FILE_FLAGS_READ);
	} catch (const std::exception &e) {
		// File was evicted between metadata check and open - this is expected
		state.LogDebug("TryOpenCacheFile: file not found (likely evicted): '" + file + "'");
		return nullptr;
	}
}

bool BlobCacheMap::ReadFromCacheFile(const string &file, idx_t blobcache_range_start, void *buffer, idx_t &length,
                                     idx_t &out_bytes_from_mem) {
	if (state.TryReadFromMemcache(file, blobcache_range_start, buffer, length)) {
		out_bytes_from_mem = length;
		return true; // reading from memcache first succeeded
	}
	// Not in memory cache - read from disk and memcache it
	out_bytes_from_mem = 0; // Initialize to 0 (disk read)
	auto handle = TryOpenCacheFile(file);
	if (!handle) {
		return false; // File was evicted or doesn't exist - signal cache miss
	}
	// allocate memory using the DuckDB buffer manager
	BufferHandle buffer_handle;
	if (!state.AllocateInMemCache(buffer_handle, length)) {
		return false; // allocation failed
	}
	auto buffer_ptr = buffer_handle.Ptr();
	try {
		auto &fs = FileSystem::GetFileSystem(*state.db_instance);
		fs.Read(*handle, buffer_ptr, length, blobcache_range_start); // Read from disk into buffer
	} catch (const std::exception &e) {
		// File was evicted/deleted after opening but before reading - signal cache miss to fall back to original source
		buffer_handle.Destroy();
		state.LogDebug("ReadFromCacheFile: read failed (likely evicted during read): '" + file +
		               "': " + string(e.what()));
		return false;
	}
	std::memcpy(buffer, buffer_ptr, length); // Copy to output buffer
	state.InsertRangeIntoMemcache(file, blobcache_range_start, buffer_handle, length);
	return true;
}

bool BlobCacheMap::WriteToCacheFile(const string &file, const void *buffer, idx_t length) {
	if (!state.db_instance) {
		return false;
	}
	try {
		auto &fs = FileSystem::GetFileSystem(*state.db_instance);
		auto flags = // Open file for writing in append mode (create if not exists)
		    FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE | FileOpenFlags::FILE_FLAGS_APPEND;
		auto handle = fs.OpenFile(file, flags);
		if (!handle) {
			state.LogError("WriteToCacheFile: failed to open: '" + file + "'");
			return false;
		}
		// Get current file size to know where we're appending
		int64_t bytes_written = fs.Write(*handle, const_cast<void *>(buffer), length);
		handle->Close(); // Close handle explicitly
		if (bytes_written != static_cast<int64_t>(length)) {
			state.LogError("WriteToCacheFile: failed to write all bytes to '" + file + "' (wrote " +
			               std::to_string(bytes_written) + " of " + std::to_string(length) + ")");
			return false;
		}
	} catch (const std::exception &e) {
		state.LogError("WriteToCacheFile: failed to write to '" + file + "': " + string(e.what()));
		return false;
	}
	return true;
}

bool BlobCacheMap::DeleteCacheFile(const string &file) {
	if (!state.db_instance)
		return false;
	try {
		auto &fs = FileSystem::GetFileSystem(*state.db_instance);
		fs.RemoveFile(file);
		state.LogDebug("DeleteCacheFile: deleted file '" + file + "'");
		return true;
	} catch (const std::exception &e) {
		state.LogError("DeleteCacheFile: failed to delete file '" + file + "': " + string(e.what()));
		return false;
	}
}

void BlobCacheMap::RemoveCacheFile(BlobCacheFile *cache_file) {
	// Note: This must be called with blobcache_mutex held
	// Remove from LRU, file_cache map, and delete physical file
	idx_t evicted_bytes = cache_file->file_size;
	string file = cache_file->file;

	RemoveFromLRU(cache_file);
	file_cache->erase(file);

	if (DeleteCacheFile(file)) {
		current_cache_size -= std::min<idx_t>(current_cache_size, evicted_bytes);
		state.LogDebug("RemoveCacheFile: removed CacheFile '" + file + "' freeing " + std::to_string(evicted_bytes) +
		               " bytes");
	} else {
		state.LogError("RemoveCacheFile: failed to delete file '" + file + "'");
	}
}

//===----------------------------------------------------------------------===//
// BlobCache eviction from both (small,large) range caches
//===----------------------------------------------------------------------===//

bool BlobCache::EvictToCapacity(BlobCacheType cache_type, idx_t new_range_size) {
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
			if (!largerange_blobcache->EvictToCapacity(largerange_blobcache->current_cache_size + new_range_size -
			                                           large_cap)) {
				result = false;
			} else {
				new_range_size = 0; // we have made space, but still will check small cache
			}
		}
	}
	idx_t small_cap = GetCacheCapacity(BlobCacheType::SMALL_RANGE);
	if (smallrange_blobcache->current_cache_size + new_range_size > small_cap) {
		result &= smallrange_blobcache->EvictToCapacity(smallrange_blobcache->current_cache_size + new_range_size -
		                                                small_cap);
	}
	return result;
}

//===----------------------------------------------------------------------===//
// Directory management
//===----------------------------------------------------------------------===//

void BlobCacheState::EnsureDirectoryExists(const string &key, BlobCacheType type) {
	// Extract the first 2 hex chars (XX) for all types
	// For large ranges, also extract next 2 hex chars (YY)
	uint8_t xx = std::stoi(key.substr(0, 2), nullptr, 16);
	uint8_t yy = (type == BlobCacheType::LARGE_RANGE) ? std::stoi(key.substr(2, 2), nullptr, 16) : 0;

	// Calculate bitset index: XX * 256 + YY (for small ranges, YY=0)
	idx_t bitset_idx = (xx << 8) | yy;

	// Fast path: check if already created (lock-free read)
	if (subdir_created.test(bitset_idx)) {
		return; // Already created
	}

	// Slow path: need to create directories
	std::lock_guard<std::mutex> lock(subdir_mutex);

	// Double-check after acquiring lock (another thread may have created it)
	if (subdir_created.test(bitset_idx)) {
		return;
	}
	// Create XX/ directory (may already exist if created for different YY)
	std::ostringstream xx_oss;
	xx_oss << std::hex << std::uppercase << std::setw(2) << std::setfill('0') << (int)xx;
	string xx_dir = blobcache_dir + xx_oss.str();

	try {
		auto &fs = FileSystem::GetFileSystem(*db_instance);
		if (!fs.DirectoryExists(xx_dir)) {
			fs.CreateDirectory(xx_dir);
		}

		if (type == BlobCacheType::LARGE_RANGE) {
			// Create XX/YY/ directory for large ranges
			std::ostringstream yy_oss;
			yy_oss << std::hex << std::uppercase << std::setw(2) << std::setfill('0') << (int)yy;
			string xxyy_dir = xx_dir + path_sep + yy_oss.str();

			if (!fs.DirectoryExists(xxyy_dir)) {
				fs.CreateDirectory(xxyy_dir);
			}
		}

		// Mark as created
		subdir_created.set(bitset_idx);
	} catch (const std::exception &e) {
		LogError("EnsureDirectoryExists: failed to create directories for key '" + key + "': " + string(e.what()));
	}
}

//===----------------------------------------------------------------------===//
// BlobCache (re-) configuration
//===----------------------------------------------------------------------===//

void BlobCache::ConfigureCache(const string &base_dir, idx_t max_size_bytes, idx_t max_io_threads) {
	std::lock_guard<std::mutex> lock(blobcache_mutex);
	auto directory = base_dir + (StringUtil::EndsWith(base_dir, state.path_sep) ? "" : state.path_sep);
	if (!state.blobcache_initialized) {
		// Release lock before calling InitializeCache to avoid deadlock
		state.blobcache_dir = directory;
		state.total_cache_capacity = max_size_bytes;

		state.LogDebug("stateureCache: initializing cache: directory='" + state.blobcache_dir +
		               "' max_size=" + std::to_string(state.total_cache_capacity) +
		               " bytes io_threads=" + std::to_string(max_io_threads) +
		               " small_threshold=" + std::to_string(BlobCacheState::SMALL_RANGE_THRESHOLD));
		if (!state.InitCacheDir()) {
			state.LogError("ConfigureCache: initializing cache directory='" + state.blobcache_dir + "' failed");
		}
		smallrange_blobcache->Clear();
		largerange_blobcache->Clear();
		state.blobcache_initialized = true;
		// Initialize our own ExternalFileCache instance (always enabled for memory caching)
		state.blobfile_memcache = make_uniq<ExternalFileCache>(*state.db_instance, true);
		state.LogDebug("ConfigureCache: initialized blobfile_memcache for memory caching of disk-cached files");
		StartIOThreads(max_io_threads);
		return;
	}

	// Cache already initialized, check what needs to be changed
	bool need_restart_threads = (nr_io_threads != max_io_threads);
	bool directory_changed = (state.blobcache_dir != directory);
	bool size_reduced = (max_size_bytes < state.total_cache_capacity);
	bool size_changed = (state.total_cache_capacity != max_size_bytes);
	if (!directory_changed && !need_restart_threads && !size_changed) {
		state.LogDebug("ConfigureCache: stateuration unchanged, no action needed");
		return;
	}

	// Stop existing threads if we need to change thread count or directory
	state.LogDebug("ConfigureCache: old_dir='" + state.blobcache_dir + "' new_dir='" + directory + "' old_size=" +
	               std::to_string(state.total_cache_capacity) + " new_size=" + std::to_string(max_size_bytes) +
	               " old_threshold=" + std::to_string(BlobCacheState::SMALL_RANGE_THRESHOLD) +
	               " old_threads=" + std::to_string(nr_io_threads) + " new_threads=" + std::to_string(max_io_threads));
	if (nr_io_threads > 0 && (need_restart_threads || directory_changed)) {
		state.LogDebug("ConfigureCache: stopping existing cache IO threads for restateuration");
		StopIOThreads();
	}

	// Clear existing cache only if directory changed or threshold changed
	if (directory_changed) {
		state.LogDebug("ConfigureCache: directory or threshold changed, clearing cache");
		smallrange_blobcache->Clear();
		largerange_blobcache->Clear();
		if (directory_changed) {
			if (!state.CleanCacheDir()) { // Clean old directory before switching
				state.LogError("ConfigureCache: cleaning cache directory='" + state.blobcache_dir + "' failed");
			}
		}
		state.blobcache_dir = directory;
		if (!state.InitCacheDir()) {
			state.LogError("ConfigureCache: initializing cache directory='" + state.blobcache_dir + "' failed");
		}
		// Reinitialize blobfile_memcache when directory changes
		state.blobfile_memcache = make_uniq<ExternalFileCache>(*state.db_instance, true);
		state.LogDebug("ConfigureCache: reinitialized blobfile_memcache after directory change");
	}
	// Same directory, just update capacity and evict if needed
	state.total_cache_capacity = max_size_bytes;
	if (size_reduced && !EvictToCapacity()) {
		state.LogError("ConfigureCache: failed to reduce the directory sizes to the new lower capacity/");
	}
	// Start threads if they were stopped or thread count changed
	if (need_restart_threads || directory_changed) {
		StartIOThreads(max_io_threads);
	}
	state.LogDebug("ConfigureCache complete: directory='" + state.blobcache_dir + "' max_size=" +
	               to_string(state.total_cache_capacity) + " bytes io_threads=" + to_string(max_io_threads) +
	               " small_threshold=" + to_string(BlobCacheState::SMALL_RANGE_THRESHOLD));
}

//===----------------------------------------------------------------------===//
// caching policy based on regexps
//===----------------------------------------------------------------------===//

void BlobCache::UpdateRegexPatterns(const string &regex_patterns_str) {
	std::lock_guard<std::mutex> lock(regex_mutex);

	cached_regexps.clear(); // Clear existing patterns
	if (regex_patterns_str.empty()) {
		// Conservative mode: empty regexps
		state.LogDebug("UpdateRegexPatterns: updated to conservative mode (empty regex patterns)");
		return;
	}
	// Aggressive mode: parse semicolon-separated patterns
	vector<string> pattern_strings = StringUtil::Split(regex_patterns_str, ';');
	for (const auto &pattern_str : pattern_strings) {
		if (!pattern_str.empty()) {
			try {
				cached_regexps.emplace_back(pattern_str, std::regex_constants::icase);
				state.LogDebug("UpdateRegexPatterns: compiled regex pattern: '" + pattern_str + "'");
			} catch (const std::regex_error &e) {
				state.LogError("UpdateRegexPatterns: wrong regex pattern '" + pattern_str + "': " + string(e.what()));
			}
		}
	}
	state.LogDebug("UpdateRegexPatterns: now using " + std::to_string(cached_regexps.size()) + " regex patterns");
}

bool BlobCache::ShouldCacheFile(const string &uri, optional_ptr<FileOpener> opener) const {
	std::lock_guard<std::mutex> lock(regex_mutex);
	if (StringUtil::StartsWith(StringUtil::Lower(uri), "file://")) {
		return false; // Never cache file:// URLs as they are already local
	}
	if (StringUtil::StartsWith(StringUtil::Lower(uri), "fakes3://")) {
		return true; // Always cache fakes3:// URLs for testing
	}
	if (!cached_regexps.empty()) {
		// Aggressive mode: use cached compiled regex patterns
		for (const auto &compiled_pattern : cached_regexps) {
			if (std::regex_search(uri, compiled_pattern)) {
				return true;
			}
		}
	} else if (StringUtil::EndsWith(StringUtil::Lower(uri), ".parquet") && opener) {
		Value parquet_cache_value; // Conservative mode: only cache .parquet files if parquet_metadata_cache=true
		auto parquet_result = FileOpener::TryGetCurrentSetting(opener, "parquet_metadata_cache", parquet_cache_value);
		if (parquet_result) {
			return BooleanValue::Get(parquet_cache_value);
		}
	}
	return false;
}

//===----------------------------------------------------------------------===//
// BlobCacheState - configuration and utility methods
//===----------------------------------------------------------------------===//

bool BlobCacheState::CleanCacheDir() {
	if (!db_instance)
		return false;
	auto &fs = FileSystem::GetFileSystem(*db_instance);
	if (!fs.DirectoryExists(blobcache_dir)) {
		return true; // Directory doesn't exist, nothing to clean
	}
	auto success = true;

	// Recursive helper lambda to remove directory contents
	std::function<void(const string &)> remove_dir_contents = [&](const string &dir_path) {
		try {
			fs.ListFiles(dir_path, [&](const string &name, bool is_dir) {
				if (name == "." || name == "..") {
					return;
				}
				string item_path = dir_path + path_sep + name;
				if (is_dir) {
					// Recursively remove subdirectory contents first
					remove_dir_contents(item_path);
					// Then remove the subdirectory itself
					try {
						fs.RemoveDirectory(item_path);
					} catch (const std::exception &) {
						success = false;
					}
				} else {
					// Remove file
					try {
						fs.RemoveFile(item_path);
					} catch (const std::exception &) {
						success = false;
					}
				}
			});
		} catch (const std::exception &) {
			success = false;
		}
	};

	// Clean the blobcache directory recursively
	try {
		remove_dir_contents(blobcache_dir);
	} catch (const std::exception &) {
		success = false;
	}

	return success;
}

bool BlobCacheState::InitCacheDir() {
	if (!db_instance) {
		return false;
	}
	auto &fs = FileSystem::GetFileSystem(*db_instance);
	if (!fs.DirectoryExists(blobcache_dir)) {
		try {
			fs.CreateDirectory(blobcache_dir);
		} catch (const std::exception &e) {
			LogError("Failed to create cache directory: " + string(e.what()));
			return false;
		}
	} else {
		if (!CleanCacheDir()) {
			return false;
		}
	}

	// Clear the subdirectory bitset - directories will be created on demand
	std::lock_guard<std::mutex> lock(subdir_mutex);
	subdir_created.reset();
	LogDebug("InitCacheDir: cleared subdirectory creation tracking bitset");
	return true;
}

} // namespace duckdb
