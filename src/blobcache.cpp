#include "blobcache.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// AnalyzeRange that helps with Inserting and Reading ranges from a cache
//===----------------------------------------------------------------------===//

static BlobCacheFileRange *AnalyzeRange(map<idx_t, unique_ptr<BlobCacheFileRange>> &ranges, idx_t position,
                                        idx_t &max_nr_bytes) {
	if (ranges.empty()) {
		return nullptr;
	}
	auto it = ranges.upper_bound(position);
	BlobCacheFileRange *hit_range = nullptr;
	if (it != ranges.begin()) { // is there a range that starts <= start_pos?
		auto &prev_range = std::prev(it)->second;
		if (prev_range && prev_range->range_end > position) { // it covers the start of this range
			// Range is usable if: (1) memcache_buffer exists (in ExternalFileCache), OR (2) disk write completed
			if (prev_range->memcache_buffer || prev_range->disk_write_completed.load(std::memory_order_acquire)) {
				hit_range = prev_range.get();
			}
		}
	}
	if (it != ranges.end() && it->second && it->second->range_start < position + max_nr_bytes) {
		max_nr_bytes = it->second->range_start - position; // cut short our range because the end is already cached now
	}
	return hit_range;
}

idx_t BlobCache::ReadFromCache(const string &cache_key, const string &filename, idx_t pos, void *buffer, idx_t &len) {
	BlobCacheType blobcache_type = // Determine which blobcache to use based on request size
	    (len <= config.small_range_threshold) ? BlobCacheType::SMALL_RANGE : BlobCacheType::LARGE_RANGE;
	idx_t hit_size = ReadFromCacheInternal(blobcache_type, cache_key, filename, pos, buffer, len);
	if (hit_size > 0 || blobcache_type == BlobCacheType::LARGE_RANGE) {
		return hit_size; // hit or large request
	}
	// for SMALL_RANGE, also try the LARGE_RANGE cache, but if found, replicate the range in the smallrange cache
	hit_size = ReadFromCacheInternal(BlobCacheType::LARGE_RANGE, cache_key, filename, pos, buffer, len);
	if (hit_size == 0) {
		return 0; // we failed to read a meaningful range into buffer
	}

	// we found something in the largerange cache: try to insert the range in smallrange cache as well
	std::lock_guard<std::mutex> lock(regex_mutex);
	auto cache_file = smallrange_blobcache->UpsertFile(cache_key, filename);
	if (!cache_file || !EvictToCapacity(hit_size, filename) || AnalyzeRange(cache_file->ranges, pos, hit_size)) {
		return hit_size; // should't insert in smallrange after all (collision, eviction failure, concurrent insertion)
	}
	config.LogDebug("DuplicateRangeToSmallCache: duplicate " + to_string(hit_size) +
	                " bytes from large cache to small cache for '" + filename + "' at position " + to_string(pos));
	InsertRangeInternal(BlobCacheType::SMALL_RANGE, cache_file, cache_key, filename, pos, pos + hit_size, buffer);
	return hit_size;
}

idx_t BlobCache::ReadFromCacheInternal(BlobCacheType cache_type, const string &cache_key, const string &filename,
                                       idx_t position, void *buffer, idx_t &max_nr_bytes) {
	std::unique_lock<std::mutex> lock(blobcache_mutex);
	auto &blobcache_map = GetCacheMap(cache_type);
	BlobCacheFile *blobcache_file = blobcache_map.FindFile(cache_key, filename);
	if (!blobcache_file) {
		return 0; // Nothing cached for this file
	}
	blobcache_map.TouchLRU(blobcache_file);
	auto hit_range = AnalyzeRange(blobcache_file->ranges, position, max_nr_bytes);
	if (!hit_range) {
		return 0; // No overlapping range
	}

	idx_t hit_size = std::min(max_nr_bytes, hit_range->range_end - position);
	idx_t offset = position - hit_range->range_start;
	hit_range->bytes_from_cache += hit_size;
	hit_range->usage_count++;

	// Copy data needed before unlocking
	idx_t cached_blobcache_range_start = hit_range->blobcache_range_start;
	auto file_id = blobcache_file->file_id;
	idx_t saved_range_start = hit_range->range_start;
	lock.unlock();

	// Read from cache file unlocked
	idx_t bytes_from_mem = 0;
	string blobcache_filepath = config.GenCacheFilePath(file_id, cache_key, cache_type);
	if (!blobcache_map.ReadFromCacheFile(blobcache_filepath, cached_blobcache_range_start + offset, buffer, hit_size,
	                                     bytes_from_mem)) {
		return 0; // Read failed
	}

	// Update bytes_from_mem counter if we had a memory hit
	if (bytes_from_mem > 0) {
		lock.lock();
		blobcache_file = blobcache_map.FindFile(cache_key, filename);
		if (blobcache_file && blobcache_file->file_id == file_id) {
			auto range_it = blobcache_file->ranges.find(saved_range_start);
			if (range_it != blobcache_file->ranges.end()) {
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
	    (len <= config.small_range_threshold) ? BlobCacheType::SMALL_RANGE : BlobCacheType::LARGE_RANGE;

	std::lock_guard<std::mutex> lock(regex_mutex);
	auto cache_file = GetCacheMap(cache_type).UpsertFile(cache_key, filename);
	if (!cache_file) {
		return; // name collision (rare)
	}
	// Check (under lock) if range already cached (in the meantime, due to concurrent reads)
	auto hit_range = AnalyzeRange(cache_file->ranges, pos, len);
	idx_t offset = 0, actual_bytes = 0, range_start = pos, range_end = range_start + len;
	if (hit_range) { // another thread cached the same range in the meantime
		offset = hit_range->range_end - range_start;
		range_start = hit_range->range_end; // cache only from the end
	}
	if (range_end > range_start) {
		actual_bytes = range_end - range_start;
	}
	if (actual_bytes == 0 || !EvictToCapacity(actual_bytes, filename)) {
		return; // back off, do not cache this range (it's empty, or we failed to make room)
	}
	InsertRangeInternal(cache_type, cache_file, cache_key, filename, range_start, range_end, ((char *)buffer) + offset);
}

void BlobCache::InsertRangeInternal(BlobCacheType cache_type, BlobCacheFile *blobcache_file, const string &cache_key,
                                    const string &filename, idx_t range_start, idx_t range_end, const void *buffer) {
	idx_t actual_bytes = range_end - range_start;
	BufferHandle buffer_handle;
	if (!AllocateInMemCache(buffer_handle, actual_bytes)) {
		return; // allocation from DDB buffer pool failed
	}
	std::memcpy(buffer_handle.Ptr(), static_cast<const char *>(buffer), actual_bytes);

	// Create new range and update stats
	auto new_range = make_uniq<BlobCacheFileRange>(range_start, range_end);
	auto range_ptr = new_range.get();
	blobcache_file->ranges[range_start] = std::move(new_range);
	blobcache_file->cached_file_size += actual_bytes;
	auto &blobcache_map = GetCacheMap(cache_type);
	blobcache_map.current_size += actual_bytes;
	blobcache_map.num_ranges++;

	// Pre-compute blobcache_range_start (we append sequentially, so we know the offset in advance)
	range_ptr->blobcache_range_start = blobcache_file->current_blobcache_file_offset;
	blobcache_file->current_blobcache_file_offset += actual_bytes;

	// Register this cached piece in the memcache
	string blobcache_filepath = config.GenCacheFilePath(blobcache_file->file_id, cache_key, cache_type);
	InsertRangeIntoMemcache(blobcache_filepath, range_ptr->blobcache_range_start, buffer_handle, actual_bytes);

	// Schedule the disk write
	auto file_buffer =
	    duckdb::make_shared_ptr<BlobCacheFileBuffer>(std::move(buffer_handle), actual_bytes, cache_key, filename);
	file_buffer->disk_write_completed_ptr = &range_ptr->disk_write_completed;
	range_ptr->memcache_buffer = file_buffer;
	string hex_prefix = cache_key.substr(4, 2);
	idx_t partition = std::stoul(hex_prefix, nullptr, 16) % num_io_threads;
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
	// Determine cache type from filename: /cachedir/1234/567890ABCDEFsID:FILE:PROT -> 's' means SMALL_RANGE
	size_t last_sep = job.filepath.find_last_of(config.path_sep);
	char type_char = job.filepath[last_sep + 12 + 1]; // after / there are 12 hex chars and then 's' or 'l'
	BlobCacheType cache_type = (type_char == 's') ? BlobCacheType::SMALL_RANGE : BlobCacheType::LARGE_RANGE;
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
// BlobCacheMap - eviction logic
//===----------------------------------------------------------------------===//

bool BlobCacheMap::EvictToCapacity(idx_t required_space, BlobCacheType cache_type, const string &exclude_filename) {
	// Try to evict files to make space (for a new range in exclude_filename), returns true if successful
	// Note: This is called with blobcache_mutex already held
	idx_t freed_space = 0;
	BlobCacheFile *last_attempted_evict = nullptr;
	while (required_space > freed_space && lru_tail && lru_tail != last_attempted_evict) {
		auto *file_to_evict = lru_tail;
		size_t file_space = (file_to_evict->filename == exclude_filename)
		                        ? 0
		                        : EvictCacheKey(config.GenCacheKey(file_to_evict->filename), cache_type);
		if (file_space == 0) {
			config.LogDebug("EvictToCapacity: could not evict '" + exclude_filename + "'");
			TouchLRU(file_to_evict); // Move it to front of LRU so we try something else
			last_attempted_evict = file_to_evict;
			continue;
		}
		// file_to_evict is now freed - do not reference it again
		freed_space += file_space;
		last_attempted_evict = nullptr; // Reset since we made progress
	}
	if (freed_space < required_space) {
		config.LogError("EvictToCapacity: cannot evict below " + std::to_string(current_size) + " to make room for " +
		                std::to_string(required_space) + " bytes");
		return false;
	}
	return true;
}

size_t BlobCacheMap::EvictCacheKey(const string &cache_key, BlobCacheType cache_type) {
	idx_t evicted_bytes = 0;
	auto it = key_cache->find(cache_key);
	if (it == key_cache->end()) {
		config.LogError("EvictCacheKey:  '" + cache_key + "' -not found");
		return 0;
	}
	for (auto &range : it->second->ranges) {
		if (!range.second->disk_write_completed.load()) {
			config.LogDebug("EvictCacheKey:  '" + cache_key + "' is skipped because it has ongoing writes.");
			return 0;
		}
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
			config.LogDebug("EnsureSubdirectoryExists: created cache subdirectory '" + subdir_path + "'");
		}
		config.subdirs_created[subdir_index] = true;
	} catch (const std::exception &e) {
		config.LogError("EnsureSubdirectoryExists: failed to mkdir '" + subdir_path + "': " + string(e.what()));
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
	if (!config.db_instance)
		return false;

	EnsureSubdirectoryExists(blobcache_filepath);
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
		Clear();
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
		config.LogDebug("Cache configuration unchanged, no action needed");
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
		Clear();
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
