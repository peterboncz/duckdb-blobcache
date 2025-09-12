#include "blobcache.hpp"

namespace duckdb {

idx_t BlobCache::ReadFromCache(const string &cache_key, idx_t position, void *buffer, idx_t &nr_bytes) {
	std::lock_guard<std::mutex> lock(cache_mutex);

	auto cache_it = key_cache.find(cache_key);
	if (cache_it == key_cache.end()) {
		return 0; // No cache entry
	}

	auto &cache_entry = cache_it->second;
	std::lock_guard<std::mutex> ranges_lock(cache_entry.ranges_mutex);

	idx_t end_pos = position + nr_bytes;

	// Find first overlapping range
	for (const auto &range_pair : cache_entry.ranges) {
		const auto &range = range_pair.second;

		if (range.start < end_pos && range.end > position) {
			// Found overlap - check if we can read from start position
			if (range.start <= position && range.end > position) {
				// Cache hit at start position
				idx_t bytes_to_read = std::min(nr_bytes, range.end - position);

				bool read_successful = false;

				// Read from memory buffer or disk
				if (range.memory_buffer && !range.disk_write_complete) {
					idx_t buffer_offset = position - range.start;
					std::memcpy(buffer, range.memory_buffer.get() + buffer_offset, bytes_to_read);
					read_successful = true;
				} else if (range.disk_write_complete) {
					idx_t file_offset = range.file_offset + (position - range.start);
					read_successful = ReadFromCacheFile(cache_key, file_offset, buffer, bytes_to_read);
				}

				if (read_successful) {
					lru_cache.Touch(cache_key, cache_entry.filename);
					auto &mutable_range = const_cast<CacheRange&>(range);
					mutable_range.usage_count++;
					mutable_range.bytes_from_cache += bytes_to_read;
					return bytes_to_read;
				}
			} else if (range.start > position) {
				// Cache range is ahead - limit filesystem read
				nr_bytes = std::min(nr_bytes, range.start - position);
			}
		}
	}
	return 0; // Cache miss
}

void BlobCache::InvalidateCache(const string &filename) {
	// Early exit if cache is not initialized - no need to invalidate
	if (!cache_initialized) {
		return;
	}
	
	std::lock_guard<std::mutex> lock(cache_mutex);
	EvictFile(filename);
}

void BlobCache::InsertCache(const string &filename, idx_t start_pos, const void *buffer, int64_t length) {
	std::lock_guard<std::mutex> lock(cache_mutex);
	if (length < 0 || !cache_initialized) {
		return;
	}

	// Refuse to cache ranges larger than cache capacity
	if (static_cast<idx_t>(length) > cache_capacity) {
		LogDebug(StringUtil::Format("Refusing to cache range of size %lld bytes (exceeds capacity %lld bytes)",
		                            length, cache_capacity));
		return;
	}

	// Generate cache key and check for collisions
	string cache_key = GenerateCacheKey(filename);
	auto cache_it = key_cache.find(cache_key);
	if (cache_it != key_cache.end()) {
		// Key exists, check if filename matches (collision detection)
		if (cache_it->second.filename != filename) {
			// Collision detected - different filename maps to same key
			LogDebug(StringUtil::Format("Refusing to cache file '%s' due to cache key collision with '%s' (key '%s')",
			                            filename.c_str(), cache_it->second.filename.c_str(), cache_key.c_str()));
			return;
		}
	}

	// Calculate initial end position
	idx_t end_pos = start_pos + length;

	// Get or create cache entry
	bool is_new_entry = (cache_it == key_cache.end());
	auto &cache_entry = key_cache[cache_key];

	if (is_new_entry) {
		// Initialize new cache entry
		cache_entry.filename = filename;
		cache_entry.cached_file_size = 0;
	}

	// Insert the range maintaining non-overlapping property
	// Shorten the new range to avoid overlapping with existing cached ranges

	idx_t original_start_pos = start_pos;  // Remember original start for buffer offset calculation

	// Find the first existing range that starts at or after our start_pos
	auto it = cache_entry.ranges.lower_bound(start_pos);

	// Check if there's a range before our start that might overlap
	if (it != cache_entry.ranges.begin()) {
		auto prev_it = std::prev(it);
		if (prev_it->second.end > start_pos) {
			// Previous range overlaps - move our start to after that range
			start_pos = prev_it->second.end;
			if (start_pos >= end_pos) {
				// New range is completely covered by existing cache
				LogDebug(StringUtil::Format("New range [%lld, %lld) completely covered by existing cache",
				                            original_start_pos, end_pos));
				return;
			}
		}
	}

	// Check if there are ranges ahead that would limit our end position
	if (it != cache_entry.ranges.end() && it->second.start < end_pos) {
		// There's a cached range starting before our end - truncate our range
		end_pos = it->second.start;
		if (start_pos >= end_pos) {
			// New range is completely covered by existing cache
			LogDebug(StringUtil::Format("New range truncated to empty by existing cache at %lld", it->second.start));
			return;
		}
	}

	// Recalculate length and range with potentially shortened boundaries
	idx_t actual_length = end_pos - start_pos;
	if (actual_length == 0) {
		// Nothing to cache
		return;
	}

	if (actual_length != static_cast<idx_t>(length)) {
		LogDebug(StringUtil::Format("Shortened new range from [%lld, %lld) to [%lld, %lld) to avoid overlap",
		                            original_start_pos, original_start_pos + length, start_pos, end_pos));
	}

	// Evict to make space before caching (evict before writing requirement)
	EvictToCapacity(actual_length);

	// Create buffer copies - one for the background write job, one for memory cache
	// Calculate the offset into the buffer for the shortened range
	idx_t buffer_offset = start_pos - original_start_pos;
	const char* shortened_buffer = static_cast<const char*>(buffer) + buffer_offset;

	// Allocate buffer copy for the background write job
	auto write_buffer_copy = unique_ptr<char[]>(new char[actual_length]);
	std::memcpy(write_buffer_copy.get(), shortened_buffer, actual_length);

	// Allocate buffer copy for the memory cache
	auto memory_buffer_copy = unique_ptr<char[]>(new char[actual_length]);
	std::memcpy(memory_buffer_copy.get(), shortened_buffer, actual_length);

	// Create and queue the write job
	auto job = make_uniq<CacheWriteJob>();
	job->cache_key = cache_key;
	job->filename = filename;
	job->start_pos = start_pos;
	job->end_pos = end_pos;
	job->buffer = std::move(write_buffer_copy);
	job->buffer_size = actual_length;

	// Add range immediately to memory cache with in-memory buffer
	// The background thread will update the file_offset when it completes the write
	{
		std::lock_guard<std::mutex> ranges_lock(cache_entry.ranges_mutex);
		CacheRange range(start_pos, end_pos, std::move(memory_buffer_copy));
		cache_entry.ranges[start_pos] = std::move(range);
		cache_entry.cached_file_size += actual_length;
	}

	QueueCacheWrite(std::move(job));

	// Update LRU tracking and cache size
	lru_cache.Touch(cache_key, filename);
	current_cache_size += actual_length;
	LogDebug(StringUtil::Format("Add range to%scache entry for file '%s', key '%s', size %lld bytes (total cache: %lld/%lld bytes)",
	                            is_new_entry ? " new ": " ",
	                            filename.c_str(), cache_key.c_str(), actual_length, current_cache_size, cache_capacity));
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
	
	// Reset thread count
	num_writer_threads = 0;
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
	
	// Stop existing threads (they will finish their current jobs gracefully)
	if (num_writer_threads > 0) {
		StopCacheWriterThreads();
		
		// Clear any remaining jobs in queues (they'll be in wrong partitions anyway)
		for (idx_t i = 0; i < MAX_WRITER_THREADS; i++) {
			std::lock_guard<std::mutex> queue_lock(write_queue_mutexes[i]);
			std::queue<unique_ptr<CacheWriteJob>> empty_queue;
			write_job_queues[i].swap(empty_queue);  // Clear the queue
		}
		
		LogDebug("Cleared job queues after thread shutdown");
	}
	
	// Start new threads with the desired count
	if (new_thread_count > 0) {
		StartCacheWriterThreads(new_thread_count);
		LogDebug(StringUtil::Format("Successfully changed to %zu writer threads", new_thread_count));
	} else {
		num_writer_threads = 0;
		LogDebug("All writer threads stopped");
	}
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
		if (WriteToCacheFile(job->cache_key, job->buffer.get(), job->buffer_size, job->file_offset)) {
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
					// Free the memory buffer since we now have disk storage
					existing_range.memory_buffer.reset();
				}
				// Note: we don't increment current_cache_size here because it was already counted in InsertCache

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
	// Determine which partition/thread should handle this job
	idx_t partition = GetPartitionForKey(job->cache_key);
	{
		std::lock_guard<std::mutex> lock(write_queue_mutexes[partition]);
		write_job_queues[partition].push(std::move(job));
	}
	write_queue_cvs[partition].notify_one();
}


void BlobCache::EvictToCapacity(idx_t required_space) {
	while (current_cache_size + required_space > cache_capacity && !lru_cache.Empty()) {
		string cache_key_to_evict = lru_cache.GetLRU();
		string filename_to_evict = lru_cache.GetLRUFilename();
		idx_t file_bytes = CalculateFileBytes(cache_key_to_evict);
		LogDebug(StringUtil::Format("Evicting file '%s' (key=%s, size=%lld bytes) to make space", 
		                           filename_to_evict.c_str(), cache_key_to_evict.c_str(), file_bytes));
		EvictKey(cache_key_to_evict);
	}
}

void BlobCache::EvictFile(const string &filename) {
	// Generate cache key from filename
	string cache_key = GenerateCacheKey(filename);
	EvictKey(cache_key);
}

void BlobCache::EvictKey(const string &cache_key) {
	// Calculate bytes to subtract before removing from cache
	idx_t file_bytes = CalculateFileBytes(cache_key);
	
	// Remove from key cache
	auto cache_it = key_cache.find(cache_key);
	if (cache_it != key_cache.end()) {
		key_cache.erase(cache_it);
	}
	
	// Delete the disk cache file
	DeleteCacheFile(cache_key);
	
	// Remove from LRU tracking
	if (lru_cache.Contains(cache_key)) {
		current_cache_size -= file_bytes;
		lru_cache.Remove(cache_key);
	}
}


idx_t BlobCache::CalculateFileBytes(const string &cache_key) {
	auto cache_it = key_cache.find(cache_key);
	return (cache_it == key_cache.end()) ? 0 : cache_it->second.cached_file_size;
}

//===----------------------------------------------------------------------===//
// Cache key generation methods
//===----------------------------------------------------------------------===//
string BlobCache::GenerateCacheKey(const string &filename) {
	// Generate 16-byte hex hash using DuckDB's string hash
	hash_t hash_value = Hash(string_t(filename.c_str(), filename.length()));
	
	// Convert to 16-byte hex string (hash_t is typically 8 bytes, so we'll use it as-is)
	std::stringstream hex_stream;
	hex_stream << std::hex << std::uppercase << std::setfill('0') << std::setw(16) << hash_value;
	string hex_hash = hex_stream.str();
	
	// Extract last 15 valid characters from filename
	string suffix = ExtractValidSuffix(filename, 15);
	
	// Determine protocol suffix using full protocol name
	string protocol_suffix;
	string filename_lower = StringUtil::Lower(filename);
	
	// Extract protocol part (everything before "://")
	size_t protocol_end = filename_lower.find("://");
	if (protocol_end != string::npos && protocol_end > 0) {
		string protocol = filename_lower.substr(0, protocol_end);
		protocol_suffix = "-" + protocol;
	}
	
	// Combine: 16-byte-hex + "-" + suffix + protocol_suffix
	return hex_hash + "-" + suffix + protocol_suffix;
}

string BlobCache::ExtractValidSuffix(const string &filename, size_t max_chars) {
	string result;
	result.reserve(max_chars);
	
	// Start from the end and work backwards, collecting valid characters
	for (int i = static_cast<int>(filename.length()) - 1; i >= 0 && result.length() < max_chars; i--) {
		char c = filename[i];
		// Valid characters: alphanumeric, dash, underscore, dot
		if (std::isalnum(c) || c == '-' || c == '_' || c == '.') {
			result = c + result; // Prepend to maintain original order
		}
	}
	
	return result;
}

idx_t BlobCache::GetPartitionForKey(const string &cache_key) const {
	// Use the first byte (2 hex characters) of the cache key for partitioning
	// Cache keys are hex strings starting with the hash, so we can extract directly
	// This gives us values 0-255, which is sufficient for up to 256 threads
	if (cache_key.length() < 2 || num_writer_threads == 0) {
		return 0;  // Fallback for malformed keys or no threads
	}
	
	// Extract first 2 hex characters and convert to partition number
	// Since we know cache keys always start with hex, no try-catch needed
	string hex_prefix = cache_key.substr(0, 2);
	idx_t partition_hash = std::stoul(hex_prefix, nullptr, 16);
	
	// Use modulo to map to available writer threads
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
				key_cache.clear();
				lru_cache.Clear();
				current_cache_size = 0;
				subdirs_created.reset();
			}
		}
	}
	
	cache_dir_path = directory;
	cache_capacity = max_size_bytes;
	cache_initialized = true;
	
	LogDebug(StringUtil::Format("Initializing cache: directory='%s' max_size=%llu bytes writer_threads=%zu", 
	                           cache_dir_path.c_str(), cache_capacity, writer_threads));
	
	InitializeCacheDirectory();
	
	// Start the cache writer threads with the specified count
	StartCacheWriterThreads(writer_threads);
}

void BlobCache::PopulateCacheStatistics(DataChunk &output, idx_t max_results) const {
	std::lock_guard<std::mutex> lock(cache_mutex);
	
	if (!cache_initialized) {
		output.SetCardinality(0);
		return;
	}
	
	// Temporary struct for range information
	struct RangeInfo {
		string protocol;
		string filename;
		idx_t start, end;
		idx_t usage_count;
		idx_t bytes_from_cache;
	};
	
	vector<RangeInfo> range_infos;
	
	// Get cache keys in LRU order (least recently used first)
	auto lru_keys = lru_cache.GetKeysInLRUOrder();
	
	// Collect cache entries in LRU order
	for (const auto &cache_key : lru_keys) {
		auto cache_it = key_cache.find(cache_key);
		if (cache_it == key_cache.end()) {
			continue; // Skip if key no longer exists
		}
		
		const auto &cache_entry = cache_it->second;
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
				break;
			}
		}
		
		// Early exit if we have enough results
		if (range_infos.size() >= max_results) {
			break;
		}
	}
	
	if (range_infos.empty()) {
		output.SetCardinality(0);
		return;
	}
	
	// Limit output to avoid excessive results
	idx_t result_count = std::min(static_cast<idx_t>(range_infos.size()), max_results);
	output.SetCardinality(result_count);
	
	// Populate the output DataChunk
	for (idx_t i = 0; i < result_count; i++) {
		const auto &info = range_infos[i];
		output.data[0].SetValue(i, Value(info.protocol));
		output.data[1].SetValue(i, Value(info.filename));
		output.data[2].SetValue(i, Value::BIGINT(static_cast<int64_t>(info.start)));
		output.data[3].SetValue(i, Value::BIGINT(static_cast<int64_t>(info.end)));
		output.data[4].SetValue(i, Value::BIGINT(static_cast<int64_t>(info.usage_count)));
		output.data[5].SetValue(i, Value::BIGINT(static_cast<int64_t>(info.bytes_from_cache)));
	}
}

void BlobCache::PopulateCacheStatistics(vector<RangeInfo> &range_infos, idx_t max_results) const {
	std::lock_guard<std::mutex> lock(cache_mutex);
	
	if (!cache_initialized) {
		return;
	}
	
	range_infos.clear();
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
			
			range_infos.push_back(std::move(info));
			
			// Early exit if we have enough results
			if (range_infos.size() >= max_results) {
				return;
			}
		}
		
		// Early exit if we have enough results
		if (range_infos.size() >= max_results) {
			return;
		}
	}
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
				LogDebug(StringUtil::Format("Cache directory '%s' was created concurrently, cleaning it", cache_dir_path.c_str()));
				CleanCacheDirectory();
			} else {
				LogError(StringUtil::Format("Failed to create cache directory '%s': %s", 
				                           cache_dir_path.c_str(), strerror(errno)));
				return; // Don't continue if we can't create the directory
			}
		} else {
			LogDebug(StringUtil::Format("Created cache directory '%s'", cache_dir_path.c_str()));
		}
	}
	
	// Clear all in-memory cache state
	key_cache.clear();
	current_cache_size = 0;
	lru_cache.Clear();
	
	// Initialize subdirectory bitmap to all zeros (reset all bits)
	subdirs_created.reset();
	
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
	// Extract first 4 characters as subdirectory name
	if (cache_key.length() < 4) {
		return "";  // Invalid key
	}
	
	string subdir_name = cache_key.substr(0, 4);
	string file_name = cache_key.substr(4);  // Remove first 4 characters for filename
	
	return cache_dir_path + path_separator + subdir_name + path_separator + file_name;
}

void BlobCache::EnsureSubdirectoryExists(const string &cache_key) {
	if (cache_key.length() < 4) {
		return;  // Invalid key
	}
	
	string subdir_name = cache_key.substr(0, 4);
	
	// Convert first 4 hex characters to index (0-65535)
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
	
	// Check if subdirectory already created
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
	
	// File size is now tracked in CacheEntry.cached_file_size
	
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
	
	// File size is now tracked in CacheEntry.cached_file_size
}


} // namespace duckdb