#include "blobcache.hpp"

namespace duckdb {

static duckdb::shared_ptr<CacheRange> AnalyzeRange(CacheEntry *cache_entry, idx_t start_pos, idx_t &end_pos) {
	auto it = cache_entry->ranges.upper_bound(start_pos);

	if (it != cache_entry->ranges.begin()) { // is there a range that starts <= start_pos?
		auto hit_range = std::prev(it)->second;
		if (hit_range->end > start_pos) { // it covers the start of this range
			if (hit_range->end < end_pos) {
				end_pos = hit_range->end;   // limit end to the cached end
			}
			return hit_range;
		}
	}
	if (it != cache_entry->ranges.end() && it->second->start < end_pos) {
		end_pos = it->second->start; // cut short our range because the end is already cached now
	}
	return nullptr;
}

idx_t BlobCache::ReadFromCache(const string &cache_key, idx_t start_pos, void *buffer, idx_t &nr_bytes) {
	duckdb::shared_ptr<SharedBuffer> mem_buffer = nullptr; // if set, read from temp in-memory write-buffer
	duckdb::shared_ptr<CacheRange> hit_range = nullptr;
	idx_t actual_bytes = 0, offset = 0, end_pos = start_pos + nr_bytes;
	{
		std::lock_guard<std::mutex> lock(cache_mutex);

		auto cache_it = key_cache.find(cache_key);
		if (cache_it == key_cache.end()) {
			return 0; // No cache entry
		}
		CacheEntry* cache_entry = cache_it->second.get();

		// Lock ranges mutex to prevent range modification during search
		std::lock_guard<std::mutex> ranges_lock(cache_entry->ranges_mutex);

		hit_range = AnalyzeRange(cache_entry, start_pos, end_pos); // adjusts lo and hi potentially
		actual_bytes = end_pos - start_pos;
		if (hit_range) { // hit
			TouchLRU(cache_entry);
			hit_range->bytes_from_cache += actual_bytes;
			hit_range->usage_count++;
			offset = start_pos - hit_range->start;
			if (!hit_range->disk_write_complete) {
				mem_buffer = hit_range->memory_buffer; // create shared_ptr ref to mem_buffer under lock
			}
		}
	}
	// All locks released here
	if (hit_range) {
		if (mem_buffer) { // read from shared_ptr buffer (in progress write)
			std::memcpy(buffer, mem_buffer->data.get() + offset, actual_bytes);
		} else if (!ReadFromCacheFile(cache_key, offset + hit_range->file_offset, buffer, actual_bytes)) {
			actual_bytes = 0; // read from cached file failed!
		}
		return actual_bytes;
	}
	return 0; // No cache hit, return 0 to indicate wrapped_fs should be used
}

void BlobCache::InvalidateCache(const string &filename) {
	if (!cache_initialized) {
		return; // Early exit if cache is not initialized - no need to invalidate
	}
	std::lock_guard<std::mutex> lock(cache_mutex);
	EvictFile(filename);
}

void BlobCache::InsertCache(const string &filename, idx_t position, const void *buffer, int64_t nr_bytes) {
	string cache_key = GenerateCacheKey(filename);
	InsertCacheWithKey(cache_key, filename, position, buffer, nr_bytes);
}

void BlobCache::InsertCacheWithKey(const string &cache_key, const string &filename, idx_t position, const void *buffer, int64_t nr_bytes) {
	idx_t actual_bytes = 0, start_pos = position, end_pos = start_pos + nr_bytes;

	if (nr_bytes < 0 || !cache_initialized || static_cast<idx_t>(nr_bytes) > cache_capacity) {
		return;
	}
	// though eventually we might not write everything, this is rare. Make a full copy for background writes
	auto tmp_buffer = duckdb::make_shared_ptr<SharedBuffer>(nr_bytes);
	CacheEntry *cache_entry = nullptr;

	// Acquire both mutexes: global first, then entry-specific
	std::unique_lock<std::mutex> lock(cache_mutex);

	auto cache_it = key_cache.find(cache_key);
	if (cache_it == key_cache.end()) {
		// Create new entry
		auto new_entry = make_uniq<CacheEntry>();
		new_entry->filename = filename;
		cache_entry = new_entry.get();
		key_cache[cache_key] = std::move(new_entry);
		AddToLRUFront(cache_entry);
	} else if (cache_it->second->filename == filename) {
		cache_entry = cache_it->second.get();
		TouchLRU(cache_entry);
	} else {
		return; // name collision (rare)
	}

	// Lock the entry's mutex while holding global mutex to prevent eviction
	std::lock_guard<std::mutex> ranges_lock(cache_entry->ranges_mutex);

	// Check if range already cached
	auto hit_range = AnalyzeRange(cache_entry, start_pos, end_pos); // adjusts lo and hi potentially
	if (hit_range) { // another thread cached the same range in the meantime
		start_pos = hit_range->end; // cache only from the end
	}

	if (end_pos > start_pos) {
		actual_bytes = end_pos - start_pos;
	}
	if (actual_bytes == 0 || !EvictToCapacity(actual_bytes, cache_key)) {
		return; // decide not to cache after all
	}
	current_cache_size += actual_bytes;

	// Add the range to cache entry
	auto new_range = duckdb::make_shared_ptr<CacheRange>(start_pos, end_pos, tmp_buffer);
	cache_entry->ranges[start_pos] = new_range;
	cache_entry->cached_file_size += actual_bytes;

	// Release global mutex - ranges_mutex still held for memcpy and job queue
	lock.unlock();
	std::memcpy(tmp_buffer->data.get(), static_cast<const char*>(buffer) + start_pos - position, actual_bytes);
	QueueCacheWrite(cache_key, new_range);
}

//===----------------------------------------------------------------------===//
// Multi-threaded background cache writer implementation
//===----------------------------------------------------------------------===//

void BlobCache::StartCacheWriterThreads(idx_t thread_count) {
	if (thread_count > MAX_WRITER_THREADS) {
		thread_count = MAX_WRITER_THREADS;
		LogDebug(StringUtil::Format("Limiting writer threads to maximum allowed: %zu", MAX_WRITER_THREADS));
	}
	shutdown_writer_threads = false;
	num_writer_threads = thread_count;
	
	LogDebug(StringUtil::Format("Starting %zu cache writer threads", num_writer_threads));
	
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
	// Only log if not shutting down to prevent database access issues
	if (!database_shutting_down.load()) {
		LogDebug(StringUtil::Format("Stopped %zu cache writer threads", num_writer_threads));
	}
	num_writer_threads = 0; // Reset thread count
}

void BlobCache::CacheWriterThreadLoop(idx_t thread_id) {
	LogDebug(StringUtil::Format("Cache writer thread %zu started", thread_id));
	
	while (!shutdown_writer_threads) {
		pair<string, duckdb::shared_ptr<CacheRange>> job;
		bool has_job = false;
		// Wait for a job or shutdown signal for this thread's queue
		{
			std::unique_lock<std::mutex> lock(write_queue_mutexes[thread_id]);
			write_queue_cvs[thread_id].wait(lock, [this, thread_id] {
				return !write_job_queues[thread_id].empty() || shutdown_writer_threads;
			});

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
		const string &cache_key = job.first;
		auto range = job.second;
		idx_t data_size = range->end - range->start;
		if (WriteToCacheFile(cache_key, range->memory_buffer->data.get(), data_size, range->file_offset)) {
			// Successfully wrote to cache file, mark the range as disk-complete
			// No need to look up in cache - we modify the shared object directly
			range->disk_write_complete = true;
			LogDebug(StringUtil::Format("Background writer completed range [%lld, %lld) for cache_key '%s'",
										   range->start, range->end, cache_key.c_str()));
		}
	}
	// Only log thread shutdown if not during database shutdown to avoid access to destroyed instance
	if (!database_shutting_down.load()) {
		LogDebug(StringUtil::Format("Cache writer thread %zu stopped", thread_id));
	}
}

void BlobCache::QueueCacheWrite(const string &cache_key, duckdb::shared_ptr<CacheRange> range) {
	idx_t partition = GetPartitionForKey(cache_key);
	{
		std::lock_guard<std::mutex> lock(write_queue_mutexes[partition]);
		write_job_queues[partition].emplace(cache_key, range);
	}
	write_queue_cvs[partition].notify_one();
}


bool BlobCache::HasUnfinishedWrites(const string &cache_key) {
	auto cache_it = key_cache.find(cache_key);
	if (cache_it == key_cache.end()) return false;
	std::lock_guard<std::mutex> ranges_lock(cache_it->second->ranges_mutex);
	for (const auto &range_pair : cache_it->second->ranges) {
		if (range_pair.second->memory_buffer && !range_pair.second->disk_write_complete) {
			return true;
		}
	}
	return false;
}

// LRU List Management methods moved to inline implementations in blobcache.hpp

bool BlobCache::EvictToCapacity(idx_t required_space, const string &exclude_key) {
	// Try to evict entries to make space, returns true if successful
	// Skip entries with unfinished writes and the exclude_key
	// Note: This is called with cache_mutex already held
	idx_t attempts = 0;
	const idx_t max_attempts = key_cache.empty() ? 0 : key_cache.size() * 2; // Prevent infinite loops

	while (current_cache_size + required_space > cache_capacity && lru_tail && attempts < max_attempts) {
		attempts++;
		CacheEntry* entry_to_evict = lru_tail;

		// Find the cache key for this entry
		string cache_key_to_evict;
		for (const auto& kv : key_cache) {
			if (kv.second.get() == entry_to_evict) {
				cache_key_to_evict = kv.first;
				break;
			}
		}

		if (cache_key_to_evict.empty()) {
			LogDebug("Failed to find cache key for LRU entry");
			break;
		}

		// Skip if this is the key we're trying to insert
		if (!exclude_key.empty() && cache_key_to_evict == exclude_key) {
			LogDebug(StringUtil::Format("Skipping eviction of current insert key '%s'", cache_key_to_evict.c_str()));
			// Move it to front of LRU so we try something else
			TouchLRU(entry_to_evict);
			continue;
		}
		// Skip if this entry has unfinished writes
		if (HasUnfinishedWrites(cache_key_to_evict)) {
			LogDebug(StringUtil::Format("Skipping eviction of key '%s' with unfinished writes", cache_key_to_evict.c_str()));
			// Move it to front of LRU so we try something else
			TouchLRU(entry_to_evict);
			continue;
		}
		// Safe to evict this entry
		string filename_to_evict = entry_to_evict->filename;
		idx_t file_bytes = CalculateFileBytes(cache_key_to_evict);
		LogDebug(StringUtil::Format("Evicting file '%s' (key=%s, size=%lld bytes) to make space",
		                           filename_to_evict.c_str(), cache_key_to_evict.c_str(), file_bytes));
		EvictKey(cache_key_to_evict);
	}
	if (current_cache_size + required_space > cache_capacity) {
		LogDebug(StringUtil::Format("Cannot evict below %lld at size %lld to make room for %lld bytes",
		                            current_cache_size, cache_capacity, required_space));
		return false;
	}
	return true;
}

void BlobCache::EvictFile(const string &filename) {
	EvictKey(GenerateCacheKey(filename));
}

void BlobCache::EvictKey(const string &cache_key) {
	idx_t file_bytes = CalculateFileBytes(cache_key);
	auto cache_it = key_cache.find(cache_key);
	if (cache_it != key_cache.end()) {
		// Try to lock the ranges mutex - if we can't, the entry is in use and shouldn't be evicted
		std::unique_lock<std::mutex> ranges_lock(cache_it->second->ranges_mutex, std::try_to_lock);
		if (!ranges_lock.owns_lock()) {
			// Entry is currently in use, skip eviction
			LogDebug(StringUtil::Format("Skipping eviction of key '%s' - entry is in use", cache_key.c_str()));
			return;
		}
		// Safe to evict - ranges_mutex is locked so no other thread is using this entry
		RemoveFromLRU(cache_it->second.get());
		key_cache.erase(cache_it);
		current_cache_size -= file_bytes;
	}
	DeleteCacheFile(cache_key);
}

idx_t BlobCache::CalculateFileBytes(const string &cache_key) {
	auto cache_it = key_cache.find(cache_key);
	return cache_it == key_cache.end() ? 0 : cache_it->second->cached_file_size;
}

// Cache key generation methods moved to inline implementations in blobcache.hpp

idx_t BlobCache::GetPartitionForKey(const string &cache_key) const {
	string hex_prefix = cache_key.substr(0, 2);
	idx_t partition_hash = std::stoul(hex_prefix, nullptr, 16);
	return partition_hash % num_writer_threads;
}


//===----------------------------------------------------------------------===//
// Disk cache helper methods
//===----------------------------------------------------------------------===//
void BlobCache::InitializeCache(const string &directory, idx_t max_size_bytes, idx_t writer_threads) {
	std::lock_guard<std::mutex> lock(cache_mutex);
	
	if (cache_initialized) {
		// Cache already initialized, check if we need to restart threads with different count
		bool need_restart_threads = (num_writer_threads != writer_threads);
		bool need_reinit_cache = (cache_dir_path != directory || cache_capacity != max_size_bytes);
		
		if (need_reinit_cache || need_restart_threads) {
			LogDebug(StringUtil::Format("Reinitializing cache: old_dir='%s' new_dir='%s' old_size=%llu new_size=%llu old_threads=%zu new_threads=%zu", 
			                           cache_dir_path.c_str(), directory.c_str(), cache_capacity, max_size_bytes, num_writer_threads, writer_threads));
			
			// Stop existing threads if we need to change thread count or directory
			if (num_writer_threads > 0) {
				LogDebug("Stopping existing cache writer threads for reinitialization");
				StopCacheWriterThreads();
			}
			// Clear existing cache if directory/size changed
			if (need_reinit_cache) {
				ClearCache();
			}
		}
	}
	cache_dir_path = directory;
	cache_capacity = max_size_bytes;
	cache_initialized = true;

	LogDebug(StringUtil::Format("Initializing cache: directory='%s' max_size=%llu bytes writer_threads=%zu",
	                           cache_dir_path.c_str(), cache_capacity, writer_threads));
	InitializeCacheDirectory();
	StartCacheWriterThreads(writer_threads);
}

void BlobCache::ConfigureCache(const string &directory, idx_t max_size_bytes, idx_t writer_threads) {
	std::lock_guard<std::mutex> lock(cache_mutex);
	if (cache_initialized) {
		// Cache already initialized, check what needs to be changed
		bool need_restart_threads = (num_writer_threads != writer_threads);
		bool directory_changed = (cache_dir_path != directory);
		bool size_reduced = (max_size_bytes < cache_capacity);
		bool size_changed = (cache_capacity != max_size_bytes);
		
		if (directory_changed || need_restart_threads || size_changed) {
			LogDebug(StringUtil::Format("Configuring cache: old_dir='%s' new_dir='%s' old_size=%llu new_size=%llu old_threads=%zu new_threads=%zu", 
			                           cache_dir_path.c_str(), directory.c_str(), cache_capacity, max_size_bytes, num_writer_threads, writer_threads));
			
			// Stop existing threads if we need to change thread count or directory
			if (num_writer_threads > 0 && (need_restart_threads || directory_changed)) {
				LogDebug("Stopping existing cache writer threads for reconfiguration");
				StopCacheWriterThreads();
			}
			// Clear existing cache only if directory changed
			if (directory_changed) {
				LogDebug("Directory changed, clearing cache");
				ClearCache();
				CleanCacheDirectory(); // Clean old directory before switching
				cache_dir_path = directory;
				// Check if this is the special debug directory
				InitializeCacheDirectory();
			} else {
				// Same directory, just update capacity and evict if needed
				cache_capacity = max_size_bytes;
				if (size_reduced && current_cache_size > cache_capacity) {
					LogDebug(StringUtil::Format("Cache size reduced from %llu to %llu bytes, evicting excess entries", 
					                           current_cache_size, cache_capacity));
					EvictToCapacity(0); // Evict until we're under the new capacity
				}
			}
			// Start threads if they were stopped or thread count changed
			if (need_restart_threads || directory_changed) {
				StartCacheWriterThreads(writer_threads);
			}
		} else {
			LogDebug("Cache configuration unchanged, no action needed");
		}
	} else {
		// Cache not initialized yet, delegate to InitializeCache
		LogDebug("Cache not initialized, delegating to InitializeCache");
		// Release lock before calling InitializeCache to avoid deadlock
		cache_mutex.unlock();
		InitializeCache(directory, max_size_bytes, writer_threads);
		return; // InitializeCache handles everything
	}
	LogDebug(StringUtil::Format("Cache configuration complete: directory='%s' max_size=%llu bytes writer_threads=%zu", 
	                           cache_dir_path.c_str(), cache_capacity, writer_threads));
}

vector<BlobCache::RangeInfo> BlobCache::GetCacheStatistics(idx_t max_results) const {
	std::lock_guard<std::mutex> lock(cache_mutex);
	vector<RangeInfo> range_infos;
	if (!cache_initialized) {
		return range_infos;
	}
	range_infos.reserve(max_results);

	// Collect cache entries in LRU order (least recently used first)
	CacheEntry* current = lru_tail;
	while (current && range_infos.size() < max_results) {
		// Find the cache key for this entry
		string cache_key;
		for (const auto& kv : key_cache) {
			if (kv.second.get() == current) {
				cache_key = kv.first;
				break;
			}
		}

		if (!cache_key.empty()) {
			// Lock the ranges for this entry
			std::lock_guard<std::mutex> ranges_lock(current->ranges_mutex);

			for (const auto &range_pair : current->ranges) {
				const auto &range = range_pair.second;
				RangeInfo info;
				// Extract protocol from cache key
				// Cache key format: hash-filename-protocol, search backwards for last '-'
				info.protocol = "unknown"; // Default
				info.filename = current->filename;
				size_t last_dash = cache_key.find_last_of('-');
				if (last_dash != string::npos && last_dash < cache_key.length() - 1) {
					info.protocol = cache_key.substr(last_dash + 1);
				}
				info.start = range->start;
				info.end = range->end;
				info.usage_count = range->usage_count;
				info.bytes_from_cache = range->bytes_from_cache;
				range_infos.push_back(info);

				// Early exit if we have enough results
				if (range_infos.size() >= max_results) {
					return range_infos;
				}
			}
		}
		// Move to next entry in LRU order
		current = current->lru_prev;
	}
	return range_infos;
}

void BlobCache::InitializeCacheDirectory() {
	// Create cache directory if it doesn't exist, or clean it if it does
	struct stat st;
	if (stat(cache_dir_path.c_str(), &st) == 0) {
		// Directory exists, clean it to ensure it's empty
		LogDebug(StringUtil::Format("Cache directory '%s' exists, cleaning it", cache_dir_path.c_str()));
		CleanCacheDirectory();
	} else {
		// Create directory with proper error handling
		if (mkdir(cache_dir_path.c_str(), 0755) != 0) {
			if (errno == EEXIST) {
				// Directory was created by another process, clean it to be safe
				LogDebug(StringUtil::Format("Cache directory '%s' was created concurrently, cleaning it",
				                            cache_dir_path.c_str()));
				CleanCacheDirectory();
			} else {
				LogError(StringUtil::Format("Failed to create cache directory '%s': %s", cache_dir_path.c_str(),
				                            strerror(errno)));
				return; // Don't continue if we can't create the directory
			}
		} else {
			LogDebug(StringUtil::Format("Created cache directory '%s'", cache_dir_path.c_str()));
		}
	}
	ClearCache();
}

void BlobCache::CleanCacheDirectory() {
	DIR *dir = opendir(cache_dir_path.c_str());
	if (!dir) {
		LogError(StringUtil::Format("Failed to open cache directory '%s' for cleaning: %s", 
		                           cache_dir_path.c_str(), strerror(errno)));
		return;
	}
	struct dirent *entry;
	while ((entry = readdir(dir)) != nullptr) {
		// Skip . and ..
		if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
			continue;
		}
		string subdir_path = cache_dir_path + path_separator + entry->d_name;
		
		// Remove all files in subdirectory
		DIR *subdir = opendir(subdir_path.c_str());
		if (subdir) {
			struct dirent *subentry;
			while ((subentry = readdir(subdir)) != nullptr) {
				if (strcmp(subentry->d_name, ".") == 0 || strcmp(subentry->d_name, "..") == 0) {
					continue;
				}
				string file_path = subdir_path + path_separator + subentry->d_name;
				if (unlink(file_path.c_str()) != 0) {
					LogError(StringUtil::Format("Failed to remove cache file '%s': %s", 
					                           file_path.c_str(), strerror(errno)));
				}
			}
			closedir(subdir);
		}
		// Remove subdirectory
		if (rmdir(subdir_path.c_str()) != 0) {
			LogError(StringUtil::Format("Failed to remove cache subdirectory '%s': %s", 
			                           subdir_path.c_str(), strerror(errno)));
		}
	}
	closedir(dir);
	LogDebug(StringUtil::Format("Cleaned cache directory '%s'", cache_dir_path.c_str()));
}

string BlobCache::GetCacheFilePath(const string &cache_key) {
	string subdir_name = cache_key.substr(0, 4);
	string file_name = cache_key.substr(4);  // Remove first 4 characters for filename
	return cache_dir_path + path_separator + subdir_name + path_separator + file_name;
}

void BlobCache::EnsureSubdirectoryExists(const string &cache_key) {
	string subdir_name = cache_key.substr(0, 4);
	uint16_t subdir_index = 0;
	for (int i = 0; i < 4; i++) {
		char c = subdir_name[i];
		uint16_t digit = 0;
		if (c >= '0' && c <= '9') {
			digit = c - '0';
		} else if (c >= 'A' && c <= 'F') {
			digit = c - 'A' + 10;
		}
		subdir_index = (subdir_index << 4) | digit;
	}
	if (subdirs_created[subdir_index]) {
		return;  // Already exists
	}
	// Create subdirectory
	string subdir_path = cache_dir_path + path_separator + subdir_name;
	if (mkdir(subdir_path.c_str(), 0755) == 0) {
		subdirs_created[subdir_index] = true;
		LogDebug(StringUtil::Format("Created cache subdirectory '%s'", subdir_path.c_str()));
	} else if (errno == EEXIST) {
		// Directory already exists, mark as created
		subdirs_created[subdir_index] = true;
	} else {
		LogError(StringUtil::Format("Failed to create cache subdirectory '%s': %s", 
		                           subdir_path.c_str(), strerror(errno)));
	}
}

int BlobCache::OpenCacheFile(const string &cache_key, int flags) {
	EnsureSubdirectoryExists(cache_key);
	string file_path = GetCacheFilePath(cache_key);
	if (file_path.empty()) {
		return -1;
	}
	int fd = open(file_path.c_str(), flags, 0644);
	if (fd == -1) {
		LogError(StringUtil::Format("Failed to open cache file '%s': %s",
		                           file_path.c_str(), strerror(errno)));
	}
	return fd;
}

bool BlobCache::ReadFromCacheFile(const string &cache_key, idx_t file_offset, void *buffer, idx_t length) {
	int flags = O_RDONLY;
#ifdef O_BINARY
	flags |= O_BINARY;
#endif
#ifdef O_DIRECT
	flags |= O_DIRECT;  // Use unbuffered I/O for binary data integrity
#endif
	int fd = OpenCacheFile(cache_key, flags);
	if (fd == -1) {
		return false;
	}
	if (lseek(fd, file_offset, SEEK_SET) == -1) {
		LogError(StringUtil::Format("Failed to seek in cache file '%s' to offset %zu: %s",
		                           cache_key.c_str(), file_offset, strerror(errno)));
		close(fd);
		return false;
	}
	ssize_t bytes_read = read(fd, buffer, length);
	close(fd);
	if (bytes_read != static_cast<ssize_t>(length)) {
		LogError(StringUtil::Format("Failed to read %zu bytes from cache file '%s' (read %zd): %s",
		                           length, cache_key.c_str(), bytes_read, strerror(errno)));
		return false;
	}

	return true;
}

bool BlobCache::WriteToCacheFile(const string &cache_key, const void *buffer, idx_t length, idx_t &file_offset) {
	// Open file for append to get current end position
	int flags = O_WRONLY | O_CREAT | O_APPEND;
#ifdef O_BINARY
	flags |= O_BINARY;
#endif
#ifdef O_DIRECT
	flags |= O_DIRECT;  // Use unbuffered I/O for binary data integrity
#endif
	int fd = OpenCacheFile(cache_key, flags);
	if (fd == -1) {
		return false;
	}
	// Get current position (which is end of file due to O_APPEND)
	file_offset = lseek(fd, 0, SEEK_CUR);
	if (file_offset == (idx_t)-1) {
		LogError(StringUtil::Format("Failed to get file position for cache file '%s': %s",
		                           cache_key.c_str(), strerror(errno)));
		close(fd);
		return false;
	}

	// Explicitly seek to end before write to ensure correct append position
	idx_t end_position = lseek(fd, 0, SEEK_END);
	file_offset = end_position;

	ssize_t bytes_written = write(fd, buffer, length);
	close(fd);
	if (bytes_written != static_cast<ssize_t>(length)) {
		LogError(StringUtil::Format("Failed to write %zu bytes to cache file '%s' (wrote %zd): %s",
		                           length, cache_key.c_str(), bytes_written, strerror(errno)));
		return false;
	}

	return true;
}

void BlobCache::DeleteCacheFile(const string &cache_key) {
	string file_path = GetCacheFilePath(cache_key);
	if (file_path.empty()) {
		return;
	}
	if (unlink(file_path.c_str()) != 0) {
		LogError(StringUtil::Format("Failed to delete cache file '%s': %s", 
		                           file_path.c_str(), strerror(errno)));
	} else {
		LogDebug(StringUtil::Format("Deleted cache file '%s'", file_path.c_str()));
	}
}

//===----------------------------------------------------------------------===//
// Configuration and caching policy implementation
//===----------------------------------------------------------------------===//

void BlobCache::UpdateRegexPatterns(const string &regex_patterns_str) {
	std::lock_guard<std::mutex> lock(regex_mutex);
	
	cached_regexps.clear(); // Clear existing patterns
	
	if (regex_patterns_str.empty()) {
		// Conservative mode: empty regexps
		conservative_mode = true;
		LogDebug("Updated to conservative mode (empty regex patterns)");
		return;
	}
	// Aggressive mode: parse semicolon-separated patterns
	conservative_mode = false;
	vector<string> pattern_strings = StringUtil::Split(regex_patterns_str, ';');
	
	for (const auto &pattern_str : pattern_strings) {
		if (!pattern_str.empty()) {
			try {
				cached_regexps.emplace_back(pattern_str, std::regex_constants::icase);
				LogDebug(StringUtil::Format("Compiled regex pattern: '%s'", pattern_str.c_str()));
			} catch (const std::regex_error &e) {
				LogError(StringUtil::Format("Invalid regex pattern '%s': %s", pattern_str.c_str(), e.what()));
			}
		}
	}
	LogDebug(StringUtil::Format("Updated to aggressive mode with %zu regex patterns", cached_regexps.size()));
}

void BlobCache::PurgeCacheForPatternChange(optional_ptr<FileOpener> opener) {
	std::lock_guard<std::mutex> cache_lock(cache_mutex);
	
	if (!cache_initialized) {
		return;
	}
	LogDebug("Purging cache entries that no longer match current caching patterns");
	vector<string> keys_to_evict;
	
	// Go through all cached entries and check if they should still be cached
	for (const auto &entry_pair : key_cache) {
		const string &cache_key = entry_pair.first;
		
		// Extract filename from cache_key (format: "filename:filesystem")
		size_t colon_pos = cache_key.find_last_of(':');
		if (colon_pos == string::npos) {
			continue; // Invalid key format
		}
		
		string filename = cache_key.substr(0, colon_pos);
		
		// Check if this file should still be cached using current patterns
		bool should_cache = false;
		{
			std::lock_guard<std::mutex> regex_lock(regex_mutex);
			if (conservative_mode) {
				// Conservative mode: only cache .parquet files when parquet_metadata_cache=true
				if (StringUtil::EndsWith(StringUtil::Lower(filename), ".parquet")) {
					// Check parquet_metadata_cache setting if opener is available
					if (opener) {
						Value parquet_cache_value;
						auto parquet_result = FileOpener::TryGetCurrentSetting(opener, "parquet_metadata_cache", parquet_cache_value);
						if (parquet_result) {
							should_cache = BooleanValue::Get(parquet_cache_value);
						} else {
							should_cache = true; // Default to true for parquet files
						}
					} else {
						should_cache = true; // Default to true for parquet files
					}
				}
			} else {
				// Aggressive mode: use cached compiled regex patterns
				for (const auto &compiled_pattern : cached_regexps) {
					if (std::regex_search(filename, compiled_pattern)) {
						should_cache = true;
						break;
					}
				}
			}
		}
		if (!should_cache) {
			keys_to_evict.push_back(cache_key);
		}
	}
	// Evict all non-qualifying entries
	idx_t evicted_count = 0;
	for (const auto &cache_key : keys_to_evict) {
		EvictKey(cache_key);
		evicted_count++;
	}
	LogDebug(StringUtil::Format("Purged %zu cache entries that no longer match caching patterns", evicted_count));
}

bool BlobCache::ShouldCacheFile(const string &filename, optional_ptr<FileOpener> opener) const {
	std::lock_guard<std::mutex> lock(regex_mutex);

	// Never cache file:// URLs as they are already local
	if (StringUtil::StartsWith(StringUtil::Lower(filename), "file://")) {
		return false;
	}

	// Always cache debug:// URLs for testing
	if (StringUtil::StartsWith(StringUtil::Lower(filename), "debug://")) {
		return true;
	}

	if (conservative_mode) {
		// Conservative mode: only cache .parquet files when parquet_metadata_cache=true
		if (StringUtil::EndsWith(StringUtil::Lower(filename), ".parquet") && opener) {
			Value parquet_cache_value;
			auto parquet_result = FileOpener::TryGetCurrentSetting(opener, "parquet_metadata_cache", parquet_cache_value);
			if (parquet_result) {
				return BooleanValue::Get(parquet_cache_value);
			}
		}
		return false;
	} else {
		// Aggressive mode: use cached compiled regex patterns
		for (const auto &compiled_pattern : cached_regexps) {
			if (std::regex_search(filename, compiled_pattern)) {
				return true;
			}
		}
		return false;
	}
}

string BlobCache::GenerateCacheKey(const string &filename) {
	hash_t hash_value = Hash(string_t(filename.c_str(), static_cast<uint32_t>(filename.length())));
	std::stringstream hex_stream;
	hex_stream << std::hex << std::uppercase << std::setfill('0') << std::setw(16) << hash_value;
	string hex_hash = hex_stream.str();
	string suffix = ExtractValidSuffix(filename, 15);
	string filename_lower = StringUtil::Lower(filename);
	size_t protocol_end = filename_lower.find("://");
	string protocol_suffix = (protocol_end != string::npos && protocol_end > 0) ?
		"-" + filename_lower.substr(0, protocol_end) : "";
	return hex_hash + "-" + suffix + protocol_suffix;
}

string BlobCache::ExtractValidSuffix(const string &filename, size_t max_chars) {
	string result;
	result.reserve(max_chars);
	for (size_t i = filename.length(); i > 0 && result.length() < max_chars; i--) {
		char c = filename[i - 1];
		if (std::isalnum(c) || c == '-' || c == '_' || c == '.') {
			result = c + result;
		}
	}
	return result;
}

} // namespace duckdb