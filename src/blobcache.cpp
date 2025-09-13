#include "blobcache.hpp"

namespace duckdb {

idx_t BlobCache::ReadFromCache(const string &cache_key, idx_t position, void *buffer, idx_t &nr_bytes) {
	duckdb::shared_ptr<SharedBuffer> mem_buffer = nullptr; // if set, read from temp in-memory write-buffer
	idx_t bytes_to_read = 0, offset = 0;
	{
		std::lock_guard<std::mutex> lock(cache_mutex);
		
		auto cache_it = key_cache.find(cache_key);
		if (cache_it == key_cache.end()) {
			return 0; // No cache entry
		}
		auto cache_entry = &cache_it->second;

		// Lock ranges mutex to prevent range modification during search
		std::lock_guard<std::mutex> ranges_lock(cache_entry->ranges_mutex);
		
		// Use lower_bound to efficiently find the first range that could contain position
		// lower_bound finds first range with start >= position
		auto range_it = cache_entry->ranges.lower_bound(position);
		
		// Check if previous range contains our position
		if (range_it != cache_entry->ranges.begin()) {
			auto prev_it = std::prev(range_it);
			if (prev_it->second.start <= position && prev_it->second.end > position) {
				// Previous range contains our position - cache hit!
				auto &range = prev_it->second;

				// Determine read strategy first
				bytes_to_read = std::min(nr_bytes, range.end - position);
				offset = position - range.start;
				if (range.memory_buffer && !range.disk_write_complete) {
					mem_buffer = range.memory_buffer; // acquires shared_ptr
				} else if (range.disk_write_complete) {
					offset += range.file_offset; // read from cached file
				}

				// admin stats & LRU celebrating the hit
				lru_cache.Touch(cache_key, cache_entry->filename);
				range.bytes_from_cache += bytes_to_read;
				range.usage_count++;
			}
		}
		// If no hit yet, check if current range starts ahead but limits our read
		if (bytes_to_read == 0 && range_it != cache_entry->ranges.end()) {
			if (range_it->second.start > position && range_it->second.start < position + nr_bytes) {
				// Cache range is ahead - limit filesystem read
				nr_bytes = std::min(nr_bytes, range_it->second.start - position);
			}
		}
	}
	// All locks released here
	if (mem_buffer) { // read from shared_ptr buffer (in progress write)
		std::memcpy(buffer, mem_buffer->data.get() + offset, bytes_to_read);
	} else if (bytes_to_read && !ReadFromCacheFile(cache_key, offset, buffer, bytes_to_read)) {
		bytes_to_read = 0; // read from cached file failed!
	}
	return bytes_to_read; // 0 means: read from wrapped_fs
}

void BlobCache::InvalidateCache(const string &filename) {
	if (!cache_initialized) {
		return; // Early exit if cache is not initialized - no need to invalidate
	}
	std::lock_guard<std::mutex> lock(cache_mutex);
	EvictFile(filename);
}

void BlobCache::InsertCache(const string &filename, idx_t start_pos, const void *buffer, int64_t length) {
	if (length < 0 || !cache_initialized || static_cast<idx_t>(length) > cache_capacity) {
		return;
	}
	string cache_key = GenerateCacheKey(filename);
	idx_t original_start_pos = start_pos;
	idx_t end_pos = start_pos + length;
	idx_t actual_length = 0;

	// Phase 1: Analyze cache structure and compute ranges while holding global lock
	{
		std::lock_guard<std::mutex> lock(cache_mutex);
		auto cache_it = key_cache.find(cache_key);
		if (cache_it != key_cache.end() && cache_it->second.filename != filename) {
			return;
		}
		auto &cache_entry = key_cache[cache_key];
		cache_entry.filename = filename;

		auto it = cache_entry.ranges.lower_bound(start_pos);
		if (it != cache_entry.ranges.begin()) {
			auto prev_it = std::prev(it);
			if (prev_it->second.end > start_pos) {
				start_pos = prev_it->second.end;
				if (start_pos >= end_pos) {
					return;
				}
			}
		}
		if (it != cache_entry.ranges.end() && it->second.start < end_pos) {
			end_pos = it->second.start;
			if (start_pos >= end_pos) {
				return;
			}
		}
		actual_length = end_pos - start_pos;
		if (actual_length == 0) {
			return;
		}
	}
	// Global lock released here
	
	auto shared_buffer = duckdb::make_shared_ptr<SharedBuffer>(actual_length);
	std::memcpy(shared_buffer->data.get(), static_cast<const char*>(buffer) + (start_pos - original_start_pos), actual_length);
	{
		std::lock_guard<std::mutex> lock(cache_mutex);
		auto recheck_it = key_cache.find(cache_key);
		if (recheck_it == key_cache.end() || recheck_it->second.filename != filename) {
			return;
		}
		auto &verified_cache_entry = recheck_it->second;
		{
			std::lock_guard<std::mutex> ranges_lock(verified_cache_entry.ranges_mutex);
			verified_cache_entry.ranges[start_pos] = CacheRange(start_pos, end_pos, shared_buffer);
			verified_cache_entry.cached_file_size += actual_length;
		}
		if (EvictToCapacity(actual_length, cache_key)) {
			auto job = make_uniq<CacheWriteJob>();
			job->cache_key = cache_key;
			job->filename = filename;
			job->start_pos = start_pos;
			job->end_pos = end_pos;
			job->buffer = shared_buffer;
			job->buffer_size = actual_length;
			QueueCacheWrite(std::move(job));

			current_cache_size += actual_length;
			lru_cache.Touch(cache_key, filename);
		}
	}
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

void BlobCache::ChangeWriterThreadCount(idx_t new_thread_count) {
	std::lock_guard<std::mutex> lock(cache_mutex);
	if (new_thread_count == num_writer_threads) {
		LogDebug(StringUtil::Format("Thread count already at %zu, no change needed", new_thread_count));
		return;
	}
	if (new_thread_count > MAX_WRITER_THREADS) {
		new_thread_count = MAX_WRITER_THREADS;
		LogDebug(StringUtil::Format("Limiting thread count to maximum allowed: %zu", MAX_WRITER_THREADS));
	}
	LogDebug(StringUtil::Format("Changing writer thread count from %zu to %zu", num_writer_threads, new_thread_count));
	
	// Collect all pending jobs before stopping threads
	vector<unique_ptr<CacheWriteJob>> pending_jobs;
	if (num_writer_threads > 0) {
		// Stop existing threads (they will finish their current jobs gracefully)
		StopCacheWriterThreads();
		
		// Collect all remaining jobs from all queues
		for (idx_t i = 0; i < MAX_WRITER_THREADS; i++) {
			std::lock_guard<std::mutex> queue_lock(write_queue_mutexes[i]);
			while (!write_job_queues[i].empty()) {
				pending_jobs.push_back(std::move(write_job_queues[i].front()));
				write_job_queues[i].pop();
			}
		}
		if (!pending_jobs.empty()) {
			LogDebug(StringUtil::Format("Collected %zu pending jobs for redistribution", pending_jobs.size()));
		}
	}
	// Start new threads with the desired count
	StartCacheWriterThreads(new_thread_count);
		
	// Redistribute the pending jobs to the correct partitions based on new thread count
	if (!pending_jobs.empty()) {
		for (auto &job : pending_jobs) {
			// Re-queue the job to the correct partition with new thread count
			// Note: GetPartitionForKey will now use the new num_writer_threads value
			QueueCacheWrite(std::move(job));
		}
		LogDebug(StringUtil::Format("Redistributed %zu jobs to new thread partitions", pending_jobs.size()));
	}
	LogDebug(StringUtil::Format("Successfully changed to %zu writer threads", new_thread_count));
}

void BlobCache::CacheWriterThreadLoop(idx_t thread_id) {
	LogDebug(StringUtil::Format("Cache writer thread %zu started", thread_id));
	
	while (!shutdown_writer_threads) {
		unique_ptr<CacheWriteJob> job;
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
			}
		}
		if (!job) {
			LogError(StringUtil::Format("Background writer failed to write cache for key '%s'", job->cache_key.c_str()));
			continue;
		}
		// Process the cache write job
		if (WriteToCacheFile(job->cache_key, job->buffer->data.get(), job->buffer_size, job->file_offset)) {
			// Successfully wrote to cache file, now add the range to the cache entry
			std::lock_guard<std::mutex> cache_lock(cache_mutex);
			auto cache_it = key_cache.find(job->cache_key);
			if (cache_it != key_cache.end()) {
				// Lock the specific cache entry's ranges
				std::lock_guard<std::mutex> ranges_lock(cache_it->second.ranges_mutex);

				// Find the existing range and mark it as disk-complete
				auto range_it = cache_it->second.ranges.find(job->start_pos);
				if (range_it != cache_it->second.ranges.end()) {
					auto &existing_range = range_it->second;
					existing_range.file_offset = job->file_offset;
					existing_range.disk_write_complete = true;
				}
				LogDebug(StringUtil::Format("Background writer completed range [%lld, %lld) for file '%s', key '%s', size %lld bytes",
										   job->start_pos, job->end_pos, job->filename.c_str(), job->cache_key.c_str(), job->buffer_size));
			}
		}
	}
	// Only log thread shutdown if not during database shutdown to avoid access to destroyed instance
	if (!database_shutting_down.load()) {
		LogDebug(StringUtil::Format("Cache writer thread %zu stopped", thread_id));
	}
}

void BlobCache::QueueCacheWrite(unique_ptr<CacheWriteJob> job) {
	idx_t partition = GetPartitionForKey(job->cache_key);
	{
		std::lock_guard<std::mutex> lock(write_queue_mutexes[partition]);
		write_job_queues[partition].push(std::move(job));
	}
	write_queue_cvs[partition].notify_one();
}


bool BlobCache::HasUnfinishedWrites(const string &cache_key) {
	auto cache_it = key_cache.find(cache_key);
	if (cache_it == key_cache.end()) return false;
	std::lock_guard<std::mutex> ranges_lock(cache_it->second.ranges_mutex);
	for (const auto &range_pair : cache_it->second.ranges) {
		if (range_pair.second.memory_buffer && !range_pair.second.disk_write_complete) {
			return true;
		}
	}
	return false;
}

bool BlobCache::EvictToCapacity(idx_t required_space, const string &exclude_key) {
	// Try to evict entries to make space, returns true if successful
	// Skip entries with unfinished writes and the exclude_key
	// Note: This is called with cache_mutex already held
	idx_t attempts = 0;
	const idx_t max_attempts = lru_cache.Empty() ? 0 : key_cache.size() * 2; // Prevent infinite loops
	
	while (current_cache_size + required_space > cache_capacity && !lru_cache.Empty() && attempts < max_attempts) {
		attempts++;
		string cache_key_to_evict = lru_cache.GetLRU();
		// Skip if this is the key we're trying to insert
		if (!exclude_key.empty() && cache_key_to_evict == exclude_key) {
			LogDebug(StringUtil::Format("Skipping eviction of current insert key '%s'", cache_key_to_evict.c_str()));
			// Move it to front of LRU so we try something else
			lru_cache.Touch(cache_key_to_evict, lru_cache.GetLRUFilename());
			continue;
		}
		// Skip if this entry has unfinished writes
		if (HasUnfinishedWrites(cache_key_to_evict)) {
			LogDebug(StringUtil::Format("Skipping eviction of key '%s' with unfinished writes", cache_key_to_evict.c_str()));
			// Move it to front of LRU so we try something else
			lru_cache.Touch(cache_key_to_evict, lru_cache.GetLRUFilename());
			continue;
		}
		// Safe to evict this entry
		string filename_to_evict = lru_cache.GetLRUFilename();
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
	if (cache_it != key_cache.end()) key_cache.erase(cache_it);
	DeleteCacheFile(cache_key);
	if (lru_cache.Contains(cache_key)) {
		current_cache_size -= file_bytes;
		lru_cache.Remove(cache_key);
	}
}

idx_t BlobCache::CalculateFileBytes(const string &cache_key) {
	auto cache_it = key_cache.find(cache_key);
	return cache_it == key_cache.end() ? 0 : cache_it->second.cached_file_size;
}

//===----------------------------------------------------------------------===//
// Cache key generation methods
//===----------------------------------------------------------------------===//
string BlobCache::GenerateCacheKey(const string &filename) {
	hash_t hash_value = Hash(string_t(filename.c_str(), filename.length()));
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
	for (int i = static_cast<int>(filename.length()) - 1; i >= 0 && result.length() < max_chars; i--) {
		char c = filename[i];
		if (std::isalnum(c) || c == '-' || c == '_' || c == '.') {
			result = c + result;
		}
	}
	return result;
}

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
				cache_dir_path = directory;
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
	
	// Get cache keys in LRU order (least recently used first)
	auto lru_keys = lru_cache.GetKeysInLRUOrder();
	
	// Collect cache entries in LRU order
	for (const auto &cache_key : lru_keys) {
		auto cache_it = key_cache.find(cache_key);
		if (cache_it == key_cache.end()) {
			continue; // Skip if key no longer exists
		}
		const auto &cache_entry = cache_it->second;
		
		// Lock the ranges for this entry
		std::lock_guard<std::mutex> ranges_lock(cache_entry.ranges_mutex);
		
		for (const auto &range_pair : cache_entry.ranges) {
			const auto &range = range_pair.second;
			RangeInfo info;
			// Extract protocol from cache key
			// Cache key format: hash-filename-protocol, search backwards for last '-'
			info.protocol = "unknown"; // Default
			info.filename = cache_entry.filename;
			size_t last_dash = cache_key.find_last_of('-');
			if (last_dash != string::npos && last_dash < cache_key.length() - 1) {
				info.protocol = cache_key.substr(last_dash + 1);
			}
			info.start = range.start;
			info.end = range.end;
			info.usage_count = range.usage_count;
			info.bytes_from_cache = range.bytes_from_cache;
			range_infos.push_back(info);
			
			// Early exit if we have enough results
			if (range_infos.size() >= max_results) {
				return range_infos;
			}
		}
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
	int fd = OpenCacheFile(cache_key, O_RDONLY);
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
	int fd = OpenCacheFile(cache_key, O_WRONLY | O_CREAT | O_APPEND);
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
	ssize_t bytes_written = write(fd, buffer, length);
	close(fd);
	if (bytes_written != static_cast<ssize_t>(length)) {
		LogError(StringUtil::Format("Failed to write %zu bytes to cache file '%s' (wrote %zd): %s", 
		                           length, cache_key.c_str(), bytes_written, strerror(errno)));
		return false;
	}
	return true; // File size is now tracked in CacheEntry.cached_file_size
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

} // namespace duckdb