#include "blobcache.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// BlobCacheConfig - Configuration and utility methods
//===----------------------------------------------------------------------===//

bool BlobCacheConfig::CleanCacheDir() {
	if (!db_instance) {
		return false;
	}
	auto &fs = GetFileSystem();
	if (!fs.DirectoryExists(cache_dir)) {
		return true; // Directory doesn't exist, nothing to clean
	}

	auto success = true;
	try {
		// List all subdirectories in cache_dir
		fs.ListFiles(cache_dir, [&](const string &name, bool is_dir) {
			if (name == "." || name == "..") {
				return;
			}
			string subdir_path = cache_dir + path_sep + name;

			if (is_dir) {
				// Remove all files in subdirectory
				try {
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

//===----------------------------------------------------------------------===//
// AnalyzeRange that helps with Inserting and Reading ranges from a cache
//===----------------------------------------------------------------------===//

static BlobCacheFileRange *AnalyzeRange(map<idx_t, unique_ptr<BlobCacheFileRange>> &ranges, idx_t position,
                                        idx_t &max_nr_bytes) {
	auto it = ranges.upper_bound(position);
	BlobCacheFileRange *hit_range = nullptr;

	if (it != ranges.begin()) { // is there a range that starts <= start_pos?
		auto &prev_range = std::prev(it)->second;
		if (prev_range->end > position) { // it covers the start of this range
			// Check if write completed or buffer available
			idx_t offset_value = prev_range->file_offset.load(std::memory_order_acquire);
			if (prev_range->memory_buffer || offset_value != BlobCacheFileRange::WRITE_NOT_COMPLETED_YET) {
				hit_range = prev_range.get(); // so it is a hit
			}
		}
	}
	if (it != ranges.end() && it->second->start < position + max_nr_bytes) {
		max_nr_bytes = it->second->start - position; // cut short our range because the end is already cached now
	}
	return hit_range;
}

idx_t BlobCache::ReadFromCache(const string &cache_key, const string &filename, idx_t position, void *buffer,
                               idx_t &max_nr_bytes) {
	// Determine which cache to use based on request size
	BlobCacheType cache_type =
	    (max_nr_bytes <= config.small_range_threshold) ? BlobCacheType::SMALL_RANGE : BlobCacheType::LARGE_RANGE;
	BlobCacheMap &cache = GetCacheMap(cache_type);

	std::unique_lock<std::mutex> lock(cache_mutex); // lock caches

	BlobCacheFile *cache_file = cache.FindFile(cache_key, filename);
	if (!cache_file) {
		return 0; // nothing cached for this file
	}
	cache.TouchLRU(cache_file);
	auto hit_range = AnalyzeRange(cache_file->ranges, position, max_nr_bytes);
	if (!hit_range) {
		return 0; // some data is cached, but there is no overlapping range
	}
	idx_t hit_size = std::min(max_nr_bytes, hit_range->end - position);
	idx_t offset = position - hit_range->start;
	hit_range->bytes_from_cache += hit_size;
	hit_range->usage_count++;

	// CRITICAL: Copy everything we need before unlocking
	idx_t cached_file_offset = hit_range->file_offset.load(std::memory_order_acquire);
	bool write_in_progress = (cached_file_offset == BlobCacheFileRange::WRITE_NOT_COMPLETED_YET);
	duckdb::shared_ptr<BlobCacheFileBuffer> mem_buffer = write_in_progress ? hit_range->memory_buffer : nullptr;
	auto file_id = cache_file->file_id;
	idx_t range_start = hit_range->start; // Save range start to relocate it after reacquiring lock
	lock.unlock();                        // do the longer-running things unlocked (note: cache_file might get deleted!)

	idx_t bytes_from_ram = 0;
	if (mem_buffer) { // read from shared_ptr buffer that we got under lock above (for in progress write)
		std::memcpy(buffer, mem_buffer->data.get() + offset, hit_size);
	} else { // write has (long) finished. Read from the cache.
		string cache_filepath = config.GenCacheFilePath(file_id, cache_key, cache_type);
		if (!cache.ReadFromCacheFile(cache_filepath, cached_file_offset + offset, buffer, hit_size, bytes_from_ram)) {
			hit_size = 0; // read from cached file failed! -- can be legal as it could have been deleted by eviction
		}
	}

	// If we had a memory cache hit, update the bytes_from_ram counter
	if (bytes_from_ram > 0) {
		lock.lock();
		// Reacquire lock and update range's bytes_from_ram counter (handle eviction race)
		cache_file = cache.FindFile(cache_key, filename);
		if (cache_file) {
			auto range_it = cache_file->ranges.find(range_start);
			if (range_it != cache_file->ranges.end()) {
				range_it->second->bytes_from_ram += bytes_from_ram;
			}
		}
		lock.unlock();
	}

	return hit_size; // No cache hit, return 0 to indicate wrapped_fs should be used
}

void BlobCache::InsertCache(const string &cache_key, const string &filename, idx_t position, void *buffer,
                            idx_t max_nr_bytes) {
	// Determine cache type based on original request size
	BlobCacheType cache_type =
	    (max_nr_bytes <= config.small_range_threshold) ? BlobCacheType::SMALL_RANGE : BlobCacheType::LARGE_RANGE;
	BlobCacheMap &cache = GetCacheMap(cache_type);

	if (!config.cache_initialized || !max_nr_bytes || static_cast<idx_t>(max_nr_bytes) > config.total_cache_capacity) {
		return;
	}
	if (cache_type == BlobCacheType::LARGE_RANGE) {
		// do debug
	}

	std::unique_lock<std::mutex> lock(cache_mutex); // lock caches

	auto cache_file = cache.UpsertFile(cache_key, filename);
	if (!cache_file) {
		return; // name collision (rare)
	}

	// Check if range already cached
	auto hit_range = AnalyzeRange(cache_file->ranges, position, max_nr_bytes);
	idx_t offset = 0, actual_bytes = 0, start_pos = position, end_pos = start_pos + max_nr_bytes;
	if (hit_range) { // another thread cached the same range in the meantime
		offset = hit_range->end - start_pos;
		start_pos = hit_range->end; // cache only from the end
	}
	if (end_pos > start_pos) {
		actual_bytes = end_pos - start_pos;
	}
	if (actual_bytes == 0 || !EvictToCapacity(actual_bytes, filename)) {
		return; // back off, do not cache this range
	}

	// Create new range and update stats
	auto new_range = make_uniq<BlobCacheFileRange>(start_pos, end_pos);
	BlobCacheFileRange *range_ptr = new_range.get(); // Get pointer before moving
	cache_file->ranges[start_pos] = std::move(new_range);
	cache_file->cached_file_size += actual_bytes;
	cache.current_size += actual_bytes;
	cache.num_ranges++;

	// Generate cache filepath
	string cache_filepath = config.GenCacheFilePath(cache_file->file_id, cache_key, cache_type);

	lock.unlock(); // Release global mutex

	// allocate and copy data into file_buffer outside lock (can be slowish)
	auto file_buffer = duckdb::make_shared_ptr<BlobCacheFileBuffer>(actual_bytes);
	file_buffer->file_offset_ptr = &range_ptr->file_offset; // Set backpointer for write completion
	std::memcpy(file_buffer->data.get(), static_cast<const char *>(buffer) + offset, actual_bytes);
	range_ptr->memory_buffer = file_buffer;     // lock-free assignment sequence: assign after copy
	string hex_prefix = cache_key.substr(4, 2); // the first 4 hex is the dir, take the next 2 for the partition
	idx_t partition = std::stoul(hex_prefix, nullptr, 16) % num_writer_threads;
	QueueCacheWrite(cache_filepath, partition, file_buffer); // writing to disk is done by background threads
}

//===----------------------------------------------------------------------===//
// Multi-threaded background cache writer implementation
//===----------------------------------------------------------------------===//

void BlobCache::QueueCacheWrite(const string &filepath, idx_t partition,
                                duckdb::shared_ptr<BlobCacheFileBuffer> buffer) {
	// Use first 2 hex chars from filepath for partitioning
	{
		std::lock_guard<std::mutex> lock(write_queue_mutexes[partition]);
		write_job_queues[partition].emplace(filepath, buffer);
	}
	write_queue_cvs[partition].notify_one();
}

void BlobCache::StartCacheWriterThreads(idx_t thread_count) {
	if (thread_count > MAX_WRITER_THREADS) {
		thread_count = MAX_WRITER_THREADS;
		config.LogDebug("Limiting writer threads to maximum allowed: " + std::to_string(MAX_WRITER_THREADS));
	}
	shutdown_writer_threads = false;
	num_writer_threads = thread_count;

	config.LogDebug("Starting " + std::to_string(num_writer_threads) + " cache writer threads");

	for (idx_t i = 0; i < num_writer_threads; i++) {
		cache_writer_threads[i] = std::thread([this, i] { CacheWriterThreadLoop(i); });
	}
}

void BlobCache::StopCacheWriterThreads() {
	// Skip if no threads are running
	if (num_writer_threads == 0) {
		return;
	}
	// Signal shutdown to all threads
	shutdown_writer_threads = true;

	// Notify all threads to wake up and check shutdown flag
	for (idx_t i = 0; i < num_writer_threads; i++) {
		write_queue_cvs[i].notify_all();
	}
	// Wait for all threads to finish gracefully
	for (idx_t i = 0; i < num_writer_threads; i++) {
		if (cache_writer_threads[i].joinable()) {
			try {
				cache_writer_threads[i].join();
			} catch (const std::exception &) {
				// Ignore join errors during shutdown - thread may have already terminated
			}
		}
	}
	// Only log if not shutting down
	if (!config.database_shutting_down) {
		config.LogDebug("Stopped " + std::to_string(num_writer_threads) + " cache writer threads");
	}
	num_writer_threads = 0; // Reset thread count
}

void BlobCache::CacheWriterThreadLoop(idx_t thread_id) {
	config.LogDebug("Cache writer thread " + std::to_string(thread_id) + " started");

	while (!shutdown_writer_threads) {
		pair<string, duckdb::shared_ptr<BlobCacheFileBuffer>> job;
		bool has_job = false;
		// Wait for a job or shutdown signal for this thread's queue
		{
			std::unique_lock<std::mutex> lock(write_queue_mutexes[thread_id]);
			write_queue_cvs[thread_id].wait(
			    lock, [this, thread_id] { return !write_job_queues[thread_id].empty() || shutdown_writer_threads; });

			if (shutdown_writer_threads && write_job_queues[thread_id].empty()) {
				break;
			}
			if (!write_job_queues[thread_id].empty()) {
				job = std::move(write_job_queues[thread_id].front());
				write_job_queues[thread_id].pop();
				has_job = true;
			}
		}
		if (!has_job) {
			continue;
		}
		// Process the cache write job
		const string &cache_filepath = job.first;
		auto file_buffer = job.second;

		// Determine cache type from filename: /cachedir/1234/567890ABCDEFsID:FILE:PROT -> 's' means SMALL_RANGE
		size_t last_sep = cache_filepath.find_last_of(config.path_sep);
		char type_char = cache_filepath[last_sep + 12 + 1]; // after / there are 12 hex chars and then 's' or 'l'
		BlobCacheType cache_type = (type_char == 's') ? BlobCacheType::SMALL_RANGE : BlobCacheType::LARGE_RANGE;
		BlobCacheMap &cache = GetCacheMap(cache_type);

		idx_t file_offset;
		if (cache.WriteToCacheFile(cache_filepath, file_buffer->data.get(), file_buffer->size, file_offset)) {
			// Successfully wrote to cache file, update the BlobCacheFileRange::file_offset atomically via backpointer
			file_buffer->file_offset_ptr->store(file_offset, std::memory_order_release);
			config.LogDebug("Background writer completed write for cache_filepath '" + cache_filepath + "'");
		}
	}
	// Only log thread shutdown if not during database shutdown to avoid access to destroyed instance
	if (!config.database_shutting_down) {
		config.LogDebug("Cache writer thread " + std::to_string(thread_id) + " stopped");
	}
}

//===----------------------------------------------------------------------===//
// BlobCacheMap - eviction logic
//===----------------------------------------------------------------------===//

bool BlobCacheMap::EvictToCapacity(idx_t required_space, BlobCacheType cache_type, const string &exclude_filename) {
	// Try to evict files to make space, returns true if successful
	// Skip files with unfinished writes and the exclude_key
	// Note: This is called with cache_mutex already held
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
		evicted_bytes += range.second->end - range.second->start;
		// Check if write is still in progress
		if (range.second->file_offset.load() == BlobCacheFileRange::WRITE_NOT_COMPLETED_YET) {
			config.LogDebug("Skipping eviction of key '" + cache_key + "' - write in progress");
			TouchLRU(it->second.get()); // remove it from the tail
			return 0;
		}
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
			info.file_offset = range_pair.second->file_offset.load(std::memory_order_acquire);
			info.start = range_pair.second->start;
			info.end = range_pair.second->end;
			info.usage_count = range_pair.second->usage_count;
			info.bytes_from_cache = range_pair.second->bytes_from_cache;
			info.bytes_from_ram = range_pair.second->bytes_from_ram;
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
	// Format: /cachedir/1234/s567890 -> need to ensure /cachedir/1234/ exists
	size_t last_sep = cache_filepath.find_last_of("/\\");
	if (last_sep == string::npos) {
		return; // No subdirectory
	}
	string subdir_path = cache_filepath.substr(0, last_sep);

	// Extract just the subdir name for hashing (e.g., "1234")
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

bool BlobCacheMap::ReadFromCacheFile(const string &cache_filepath, idx_t file_offset, void *buffer, idx_t length,
                                     idx_t &out_bytes_from_ram) {
	out_bytes_from_ram = 0; // Initialize to 0 (disk read)

	if (!config.db_instance || !parent_cache || !parent_cache->blob_file_cache) {
		return false;
	}

	// Try to get cached file from our ExternalFileCache
	auto &file_cache = *parent_cache->blob_file_cache;
	auto &cached_file = file_cache.GetOrCreateCachedFile(cache_filepath);

	// Check if the range is already cached in memory
	auto ranges_guard = cached_file.lock.GetSharedLock();
	auto &ranges = cached_file.Ranges(ranges_guard);

	// Look for cached range at file_offset
	auto it = ranges.find(file_offset);
	if (it != ranges.end()) {
		auto &range = *it->second;
		if (range.nr_bytes == length) {
			// Try to pin the block handle
			auto &buffer_manager = file_cache.GetBufferManager();
			auto pin = buffer_manager.Pin(range.block_handle);
			if (pin.IsValid()) {
				// Memory hit - read from BufferHandle
				auto buffer_ptr = pin.Ptr();
				std::memcpy(buffer, buffer_ptr, length);
				out_bytes_from_ram = length; // Mark as memory hit
				config.LogDebug("Memory cache hit for '" + cache_filepath + "' at offset " +
				                std::to_string(file_offset) + " length " + std::to_string(length));
				return true;
			}
		}
	}

	// Not in memory cache - read from disk and cache it
	ranges_guard.reset();

	auto handle = TryOpenCacheFile(cache_filepath);
	if (!handle) {
		// File was evicted or doesn't exist - signal cache miss
		return false;
	}

	auto &fs = config.GetFileSystem();

	// Allocate BufferHandle from our ExternalFileCache's BufferManager
	auto &buffer_manager = file_cache.GetBufferManager();
	auto buffer_handle = buffer_manager.Allocate(MemoryTag::EXTERNAL_FILE_CACHE, length);
	auto buffer_ptr = buffer_handle.Ptr();

	// Read from disk into buffer
	fs.Read(*handle, buffer_ptr, length, file_offset);

	// Copy to output buffer
	std::memcpy(buffer, buffer_ptr, length);

	// Insert into cache for future use
	auto new_range =
	    make_shared_ptr<ExternalFileCache::CachedFileRange>(buffer_handle.GetBlockHandle(), length, file_offset, "");
	auto write_guard = cached_file.lock.GetExclusiveLock();
	auto &write_ranges = cached_file.Ranges(write_guard);
	write_ranges[file_offset] = std::move(new_range);

	config.LogDebug("Disk read and cached for '" + cache_filepath + "' at offset " + std::to_string(file_offset) +
	                " length " + std::to_string(length));
	return true;
}

bool BlobCacheMap::WriteToCacheFile(const string &cache_filepath, const void *buffer, idx_t length,
                                    idx_t &file_offset) {
	if (!config.db_instance) {
		return false;
	}

	EnsureSubdirectoryExists(cache_filepath);

	try {
		auto &fs = config.GetFileSystem();
		// Open file for writing in append mode (create if not exists)
		auto flags =
		    FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE | FileOpenFlags::FILE_FLAGS_APPEND;
		auto handle = fs.OpenFile(cache_filepath, flags);
		if (!handle) {
			config.LogError("Failed to open cache file for writing: '" + cache_filepath + "'");
			return false;
		}

		// Get current file size to know where we're appending
		file_offset = static_cast<idx_t>(fs.GetFileSize(*handle));

		// Write data using sequential write (append mode automatically positions at end)
		int64_t bytes_written = fs.Write(*handle, const_cast<void *>(buffer), length);

		if (bytes_written != static_cast<int64_t>(length)) {
			config.LogError("Failed to write all bytes to cache file (wrote " + std::to_string(bytes_written) + " of " +
			                std::to_string(length) + ")");
			handle->Close();
			return false;
		}

		// Flush to disk to ensure data is written
		fs.FileSync(*handle);

		// Close handle explicitly
		handle->Close();
		return true;
	} catch (const std::exception &e) {
		config.LogError("Failed to write to cache file '" + cache_filepath + "': " + string(e.what()));
		return false;
	}
}

bool BlobCacheMap::DeleteCacheFile(const string &cache_filepath) {
	if (!config.db_instance) {
		return false;
	}

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
	// Note: Must be called with cache_mutex held
	if (cache_file == lru_head) {
		return; // Already at front
	}
	RemoveFromLRU(cache_file); // Remove from current position
	AddToLRUFront(cache_file); // Add to front
}

void BlobCacheMap::RemoveFromLRU(BlobCacheFile *cache_file) {
	// Remove cache_file from LRU list
	// Note: Must be called with cache_mutex held
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
	// Note: Must be called with cache_mutex held
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
	if (large_cache->current_size + extra_bytes > large_capacity) {
		if (!large_cache->EvictToCapacity(large_cache->current_size + extra_bytes - large_capacity,
		                                  BlobCacheType::LARGE_RANGE, exclude_filename)) {
			result = false;
		} else {
			extra_bytes = 0;
		}
	}
	// Then evict from small cache if needed
	idx_t small_capacity = GetCacheCapacity(BlobCacheType::SMALL_RANGE);
	if (small_cache->current_size + extra_bytes > small_capacity) {
		result &= small_cache->EvictToCapacity(small_cache->current_size + extra_bytes - small_capacity,
		                                       BlobCacheType::SMALL_RANGE, exclude_filename);
	}
	return result;
}

//===----------------------------------------------------------------------===//
// BlobCache (re-) configuration
//===----------------------------------------------------------------------===//

void BlobCache::ConfigureCache(const string &base_dir, idx_t max_size_bytes, idx_t writer_threads,
                               idx_t small_threshold) {
	std::lock_guard<std::mutex> lock(cache_mutex);
	auto directory = base_dir + (StringUtil::EndsWith(base_dir, config.path_sep) ? "" : config.path_sep);
	if (!config.cache_initialized) {
		// Release lock before calling InitializeCache to avoid deadlock
		config.cache_dir = directory;
		config.total_cache_capacity = max_size_bytes;
		config.small_range_threshold = small_threshold;

		config.LogDebug("Initializing cache: directory='" + config.cache_dir +
		                "' max_size=" + std::to_string(config.total_cache_capacity) +
		                " bytes writer_threads=" + std::to_string(writer_threads) +
		                " small_threshold=" + std::to_string(config.small_range_threshold));
		if (!config.InitCacheDir()) {
			config.LogError("Initializing cache directory='" + config.cache_dir + "' failed");
		}
		ClearCache();
		config.cache_initialized = true;
		// Initialize our own ExternalFileCache instance (always enabled for memory caching)
		blob_file_cache = make_uniq<ExternalFileCache>(*config.db_instance, true);
		config.LogDebug("Initialized blob_file_cache for memory caching of disk-cached files");
		StartCacheWriterThreads(writer_threads);
		return;
	}

	// Cache already initialized, check what needs to be changed
	bool need_restart_threads = (num_writer_threads != writer_threads);
	bool directory_changed = (config.cache_dir != directory);
	bool size_reduced = (max_size_bytes < config.total_cache_capacity);
	bool size_changed = (config.total_cache_capacity != max_size_bytes);
	bool threshold_changed = (config.small_range_threshold != small_threshold);
	if (!directory_changed && !need_restart_threads && !size_changed && !threshold_changed) {
		config.LogDebug("Cache configuration unchanged, no action needed");
		return;
	}

	// Stop existing threads if we need to change thread count or directory
	config.LogDebug(
	    "Configuring cache: old_dir='" + config.cache_dir + "' new_dir='" + directory +
	    "' old_size=" + std::to_string(config.total_cache_capacity) + " new_size=" + std::to_string(max_size_bytes) +
	    " old_threads=" + std::to_string(num_writer_threads) + " new_threads=" + std::to_string(writer_threads) +
	    " old_threshold=" + std::to_string(config.small_range_threshold) +
	    " new_threshold=" + std::to_string(small_threshold));
	if (num_writer_threads > 0 && (need_restart_threads || directory_changed)) {
		config.LogDebug("Stopping existing cache writer threads for reconfiguration");
		StopCacheWriterThreads();
	}

	// Clear existing cache only if directory changed or threshold changed
	if (directory_changed || threshold_changed) {
		config.LogDebug("Directory or threshold changed, clearing cache");
		ClearCache();
		if (directory_changed) {
			if (!config.CleanCacheDir()) { // Clean old directory before switching
				config.LogError("Cleaning cache directory='" + config.cache_dir + "' failed");
			}
		}
		config.cache_dir = directory;
		config.small_range_threshold = small_threshold;
		if (!config.InitCacheDir()) {
			config.LogError("Initializing cache directory='" + config.cache_dir + "' failed");
		}
		// Reinitialize blob_file_cache when directory changes
		blob_file_cache = make_uniq<ExternalFileCache>(*config.db_instance, true);
		config.LogDebug("Reinitialized blob_file_cache after directory change");
	}
	// Same directory, just update capacity and evict if needed
	config.total_cache_capacity = max_size_bytes;
	if (size_reduced && !EvictToCapacity()) {
		config.LogError("Failed to reduce the directory sizes to the new lower capacity/");
	}
	// Start threads if they were stopped or thread count changed
	if (need_restart_threads || directory_changed) {
		StartCacheWriterThreads(writer_threads);
	}
	config.LogDebug("Cache configuration complete: directory='" + config.cache_dir +
	                "' max_size=" + std::to_string(config.total_cache_capacity) +
	                " bytes writer_threads=" + std::to_string(writer_threads) +
	                " small_threshold=" + std::to_string(config.small_range_threshold));
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

} // namespace duckdb
