#include "blobcache.hpp"

namespace duckdb {

idx_t BlobCache::ReadFromCache(const string &cache_key, idx_t position, void *buffer, idx_t &nr_bytes) {
	// Gather all needed information while holding locks, then perform I/O safely
	CacheEntry* cache_entry_ptr = nullptr;
	string filename_copy;
	idx_t bytes_to_read = 0;
	idx_t buffer_offset = 0;
	idx_t file_offset = 0;
	bool use_memory_buffer = false;
	bool use_disk = false;
	duckdb::shared_ptr<SharedBuffer> memory_buffer_copy; // Safe copy of memory buffer
	idx_t range_start = 0, range_end = 0; // For validation later
	
	// Phase 1: Find range and copy necessary data while holding locks
	{
		std::lock_guard<std::mutex> lock(cache_mutex);
		
		auto cache_it = key_cache.find(cache_key);
		if (cache_it == key_cache.end()) {
			return 0; // No cache entry
		}
		
		cache_entry_ptr = &cache_it->second;
		filename_copy = cache_entry_ptr->filename;
		idx_t end_pos = position + nr_bytes;
		
		// Lock ranges mutex to prevent range modification during search
		std::lock_guard<std::mutex> ranges_lock(cache_entry_ptr->ranges_mutex);
		
		// Find first overlapping range
		for (auto &range_pair : cache_entry_ptr->ranges) {
			auto &range = range_pair.second;
			
			if (range.start < end_pos && range.end > position) {
				// Found overlap - check if we can read from start position
				if (range.start <= position && range.end > position) {
					// Cache hit at start position
					bytes_to_read = std::min(nr_bytes, range.end - position);
					buffer_offset = position - range.start;
					range_start = range.start;
					range_end = range.end;
					
					// Determine read strategy and copy memory buffer safely
					if (range.memory_buffer && !range.disk_write_complete) {
						use_memory_buffer = true;
						memory_buffer_copy = range.memory_buffer; // Safe shared_ptr copy
					} else if (range.disk_write_complete) {
						use_disk = true;
						file_offset = range.file_offset + buffer_offset;
					}
					break;
				} else if (range.start > position) {
					// Cache range is ahead - limit filesystem read
					nr_bytes = std::min(nr_bytes, range.start - position);
				}
			}
		}
	}
	// All locks released here
	
	// No suitable range found
	if (bytes_to_read == 0) {
		return 0; // Cache miss
	}
	
	// Phase 2: Perform I/O without holding any locks
	bool read_successful = false;
	
	if (use_memory_buffer && memory_buffer_copy) {
		// Safe to access memory buffer - we have a valid shared_ptr copy
		// The shared_ptr prevents the buffer from being freed
		char* buffer_ptr = memory_buffer_copy->data.get();
		std::memcpy(buffer, buffer_ptr + buffer_offset, bytes_to_read);
		read_successful = true;
	} else if (use_disk) {
		// Disk I/O with immutable file_offset - safe without locks
		read_successful = ReadFromCacheFile(cache_key, file_offset, buffer, bytes_to_read);
	}
	
	// Phase 3: Update statistics if read was successful
	if (read_successful) {
		std::lock_guard<std::mutex> lock(cache_mutex);
		
		// Verify cache entry still exists
		auto recheck_it = key_cache.find(cache_key);
		if (recheck_it == key_cache.end() || recheck_it->second.filename != filename_copy) {
			return bytes_to_read; // Cache was invalidated but read succeeded
		}
		
		CacheEntry& recheck_cache_entry = recheck_it->second;
		std::lock_guard<std::mutex> ranges_lock(recheck_cache_entry.ranges_mutex);
		
		// Find the range again to update statistics
		for (auto &range_pair : recheck_cache_entry.ranges) {
			auto &range = range_pair.second;
			if (range.start == range_start && range.end == range_end) {
				// Found the same range - update atomic statistics
				range.usage_count.fetch_add(1);
				range.bytes_from_cache.fetch_add(bytes_to_read);
				break;
			}
		}
		
		// Update LRU (already holding cache_mutex)
		lru_cache.Touch(cache_key, filename_copy);
		
		return bytes_to_read;
	}
	
	return 0; // Read failed
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
	if (length < 0 || !cache_initialized) {
		return;
	}

	// Refuse to cache ranges larger than cache capacity
	if (static_cast<idx_t>(length) > cache_capacity) {
		LogDebug(StringUtil::Format("Refusing to cache range of size %lld bytes (exceeds capacity %lld bytes)",
		                            length, cache_capacity));
		return;
	}

	string cache_key = GenerateCacheKey(filename);
	bool is_new_entry = false;
	idx_t original_start_pos = start_pos;
	idx_t end_pos = start_pos + length;
	idx_t actual_length = 0;
	const char* shortened_buffer = nullptr;

	// Phase 1: Analyze cache structure and compute ranges while holding global lock
	{
		std::lock_guard<std::mutex> lock(cache_mutex);
		
		// Generate cache key and check for collisions
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
		
		// Get or create cache entry
		is_new_entry = (cache_it == key_cache.end());
		auto &cache_entry = key_cache[cache_key];
		
		if (is_new_entry) {
			// Initialize new cache entry
			cache_entry.filename = filename;
			cache_entry.cached_file_size = 0;
		}
		
		// Insert the range maintaining non-overlapping property
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
		actual_length = end_pos - start_pos;
		if (actual_length == 0) {
			// Nothing to cache
			return;
		}
		
		if (actual_length != static_cast<idx_t>(length)) {
			LogDebug(StringUtil::Format("Shortened new range from [%lld, %lld) to [%lld, %lld) to avoid overlap",
			                            original_start_pos, original_start_pos + length, start_pos, end_pos));
		}
		
		// Try to evict to make space before caching, but exclude our own key
		// If eviction fails, we skip the caching opportunity
		if (!EvictToCapacity(actual_length, cache_key)) {
			LogDebug(StringUtil::Format("Cannot evict enough space for range [%lld, %lld) in file '%s', skipping cache opportunity",
			                            start_pos, end_pos, filename.c_str()));
			return;
		}
		
		// Calculate the offset into the buffer for the shortened range
		idx_t buffer_offset = start_pos - original_start_pos;
		shortened_buffer = static_cast<const char*>(buffer) + buffer_offset;
	}
	// Global lock released here
	
	// Phase 2: Perform single memory copy without holding global lock
	// Create a shared buffer that can be used by both CacheRange and CacheWriteJob
	auto shared_buffer = duckdb::make_shared_ptr<SharedBuffer>(actual_length);
	std::memcpy(shared_buffer->data.get(), shortened_buffer, actual_length);
	
	// Phase 3: Re-acquire global lock and update cache structures
	{
		std::lock_guard<std::mutex> lock(cache_mutex);
		
		// Verify cache entry still exists and is valid after memory copy
		auto recheck_it = key_cache.find(cache_key);
		if (recheck_it == key_cache.end() || recheck_it->second.filename != filename) {
			// Cache entry was invalidated or changed while we were copying - abort
			LogDebug(StringUtil::Format("Cache entry invalidated during buffer copy for file '%s', key '%s'", 
			                            filename.c_str(), cache_key.c_str()));
			return;
		}
		auto &verified_cache_entry = recheck_it->second;
		
		// Create and queue the write job with the shared buffer
		auto job = make_uniq<CacheWriteJob>();
		job->cache_key = cache_key;
		job->filename = filename;
		job->start_pos = start_pos;
		job->end_pos = end_pos;
		job->buffer = shared_buffer;  // Share the same buffer
		job->buffer_size = actual_length;
		
		// Add range immediately to memory cache with the same shared buffer
		// The background thread will update the file_offset when it completes the write
		{
			std::lock_guard<std::mutex> ranges_lock(verified_cache_entry.ranges_mutex);
			CacheRange range(start_pos, end_pos, shared_buffer);  // Share the same buffer
			verified_cache_entry.ranges[start_pos] = std::move(range);
			verified_cache_entry.cached_file_size += actual_length;
		}
		
		QueueCacheWrite(std::move(job));
		
		// Update LRU tracking and cache size
		lru_cache.Touch(cache_key, filename);
		current_cache_size += actual_length;
		LogDebug(StringUtil::Format("Add range to%scache entry for file '%s', key '%s', size %lld bytes (total cache: %lld/%lld bytes)",
		                            is_new_entry ? " new ": " ",
		                            filename.c_str(), cache_key.c_str(), actual_length, current_cache_size, cache_capacity));
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
	if (new_thread_count > 0) {
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
	} else {
		// If new_thread_count is 0, we need to handle pending jobs differently
		// We should either process them synchronously or mark the ranges as failed
		if (!pending_jobs.empty()) {
			LogDebug(StringUtil::Format("Dropping %zu pending jobs as no writer threads are active", pending_jobs.size()));
			
			// Clean up the cache entries for these dropped jobs
			// We need to remove the ranges and adjust cache size
			for (const auto &job : pending_jobs) {
				auto cache_it = key_cache.find(job->cache_key);
				if (cache_it != key_cache.end()) {
					std::lock_guard<std::mutex> ranges_lock(cache_it->second.ranges_mutex);
					
					// Find and remove the range that corresponds to this job
					auto range_it = cache_it->second.ranges.find(job->start_pos);
					if (range_it != cache_it->second.ranges.end()) {
						// This range was counted in cache size but never written
						// We need to subtract it to avoid capacity leak
						current_cache_size -= job->buffer_size;
						cache_it->second.cached_file_size -= job->buffer_size;
						cache_it->second.ranges.erase(range_it);
						
						LogDebug(StringUtil::Format("Removed unwritten range [%lld, %lld) for key '%s', freed %lld bytes",
						                           job->start_pos, job->end_pos, job->cache_key.c_str(), job->buffer_size));
					}
				}
			}
		}
		
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


bool BlobCache::HasUnfinishedWrites(const string &cache_key) {
	// Check if a cache entry has any ranges with unfinished disk writes
	// Note: This is called with cache_mutex already held
	auto cache_it = key_cache.find(cache_key);
	if (cache_it == key_cache.end()) {
		return false;
	}
	
	// Lock the ranges and check for unfinished writes
	std::lock_guard<std::mutex> ranges_lock(cache_it->second.ranges_mutex);
	for (const auto &range_pair : cache_it->second.ranges) {
		const auto &range = range_pair.second;
		// If there's a memory buffer but disk write is not complete, write is in progress
		if (range.memory_buffer && !range.disk_write_complete) {
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
	
	// Return true if we successfully made enough space
	return (current_cache_size + required_space <= cache_capacity);
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
				key_cache.clear();
				lru_cache.Clear();
				current_cache_size = 0;
				subdirs_created.reset();
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

//===----------------------------------------------------------------------===//
// Configuration and caching policy implementation
//===----------------------------------------------------------------------===//

void BlobCache::UpdateRegexPatterns(const string &regex_patterns_str) {
	std::lock_guard<std::mutex> lock(regex_mutex);
	
	// Clear existing patterns
	cached_regexps.clear();
	
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
		if (!StringUtil::EndsWith(StringUtil::Lower(filename), ".parquet")) {
			return false;
		}
		
		// Check parquet_metadata_cache setting if opener is available
		if (opener) {
			Value parquet_cache_value;
			auto parquet_result = FileOpener::TryGetCurrentSetting(opener, "parquet_metadata_cache", parquet_cache_value);
			if (parquet_result) {
				return BooleanValue::Get(parquet_cache_value);
			}
		}
		
		// Default to true for parquet files in conservative mode
		return true;
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