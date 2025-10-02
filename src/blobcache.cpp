#include "blobcache.hpp"

namespace duckdb {

void RemoveDirectory(DatabaseInstance *db, const string &dir_path, string &path_separator) {
	// Remove all files in the directory
	DIR *dir = opendir(dir_path.c_str());
	if (!dir) {
		return; // Directory doesn't exist or can't be opened
	}
	struct dirent *entry;
	while ((entry = readdir(dir)) != nullptr) {
		if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
			continue; // Skip . and ..
		}
		string file_path = dir_path + path_separator + entry->d_name;
		unlink(file_path.c_str()); // Remove file
	}
	closedir(dir);

	// Remove the directory itself
	if (rmdir(dir_path.c_str()) == 0) {
		DUCKDB_LOG_ERROR(*db, StringUtil::Format("Removed cache directory '%s'", dir_path.c_str()));
	} else if (errno != ENOENT) {
		DUCKDB_LOG_ERROR(*db, StringUtil::Format("Could not remove cache directory '%s': %s",
		                                    dir_path.c_str(), strerror(errno)));
	}
}

bool ReadFromCacheFile(DatabaseInstance *db, string &filepath, idx_t file_offset, void *buffer, idx_t length) {
	int fd = open(filepath.c_str(), O_RDONLY);
	if (fd == -1) {
		DUCKDB_LOG_ERROR(*db, StringUtil::Format("Failed to open partition file '%s' for reading: %s", filepath.c_str(), strerror(errno)));
		return false;
	}
	if (lseek(fd, file_offset, SEEK_SET) == -1) {
		DUCKDB_LOG_ERROR(*db, StringUtil::Format("Failed to seek in partition file '%s' to offset %zu: %s",
		                                    filepath.c_str(), file_offset, strerror(errno)));
		close(fd);
		return false;
	}
	ssize_t bytes_read = read(fd, buffer, length);
	close(fd);
	if (bytes_read != static_cast<ssize_t>(length)) {
		DUCKDB_LOG_ERROR(*db, StringUtil::Format("Failed to read %zu bytes from partition file '%s' (read %zd): %s",
		                                    length, filepath.c_str(), bytes_read, strerror(errno)));
		return false;
	}
	return true;
}

bool WriteToCacheFile(DatabaseInstance *db, string &filepath, const void *buffer, idx_t length, idx_t &file_offset) {
	// Open file for append
	int fd = open(filepath.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
	if (fd == -1) {
		DUCKDB_LOG_ERROR(*db, StringUtil::Format("Failed to open partition file '%s' for writing: %s", filepath.c_str(), strerror(errno)));
		return false;
	}
	// Get current end position
	file_offset = lseek(fd, 0, SEEK_END);
	if (file_offset == (idx_t)-1) {
		DUCKDB_LOG_ERROR(*db, StringUtil::Format("Failed to get file position for partition file '%s': %s", filepath.c_str(), strerror(errno)));
		close(fd);
		return false;
	}
	ssize_t bytes_written = write(fd, buffer, length);
	close(fd);
	if (bytes_written != static_cast<ssize_t>(length)) {
		DUCKDB_LOG_ERROR(*db, StringUtil::Format("Failed to write %zu bytes to partition file '%s' (wrote %zd): %s",
		                                    length, filepath.c_str(), bytes_written, strerror(errno)));
		return false;
	}
	return true;
}

static shared_ptr<CacheRange> AnalyzeRange(CacheEntry *cache_entry, idx_t position, idx_t &max_nr_bytes) {
	auto it = cache_entry->ranges.upper_bound(position);
	shared_ptr<CacheRange> hit_range;

	if (it != cache_entry->ranges.begin()) { // is there a range that starts <= start_pos?
		auto prev_range = std::prev(it)->second;
		if (prev_range->end > position &&  // it covers the start of this range
		    prev_range->memory_buffer) { // range is only ready if this is filled
			hit_range = prev_range; // so it is a hit
		}
	}
	if (it != cache_entry->ranges.end() && it->second->start < position + max_nr_bytes) {
		max_nr_bytes = it->second->start - position; // cut short our range because the end is already cached now
	}
	return hit_range;
}

idx_t BlobCache::ReadFromCache(const string &filename, idx_t position, void *buffer, idx_t &max_nr_bytes) {
	idx_t partition_id = GetPartitionForFilename(filename);
	auto& partition = (max_nr_bytes > SMALL_RANGE_THRESHOLD) ? small_partitions[partition_id] : large_partitions[partition_id];

	duckdb::shared_ptr<SharedBuffer> mem_buffer = nullptr;
	duckdb::shared_ptr<CacheRange> hit_range = nullptr;
	idx_t offset = 0, hit_size = 0;
	string filepath;
	{
		std::lock_guard<std::mutex> lock(cache_mutex);

		auto cache_it = partition.file_cache.find(filename);
		if (cache_it == partition.file_cache.end()) {
			return 0; // No cache entry
		}
		CacheEntry* cache_entry = cache_it->second.get();
		hit_range = AnalyzeRange(cache_entry, position, max_nr_bytes);
		if (hit_range) { // hit
			hit_size = std::min(max_nr_bytes, hit_range->end - position);
			partition.TouchLRU(cache_entry);

			hit_range->bytes_from_cache += hit_size;
			hit_range->usage_count++;
			offset = position - hit_range->start;
			if (hit_range->memory_buffer) {
				mem_buffer = hit_range->memory_buffer;
			}
		}
	}
	// All locks released here
	if (hit_range) {
		if (mem_buffer) {
			std::memcpy(buffer, mem_buffer->data.get() + offset, hit_size);
		} else if (!ReadFromCacheFile(db, partition.file1, hit_range->file_offset + offset, buffer, hit_size)) {
			hit_size = 0; // note that read can fail if eviction has rotated this blobcache file
		}
	}
	return hit_size;
}

void BlobCache::InsertCache(const string &filename, idx_t position, const void *buffer, idx_t req_bytes) {
	idx_t partition_id = GetPartitionForFilename(filename);
	auto is_small = (req_bytes < SMALL_RANGE_THRESHOLD);
	auto& partition =  is_small ? small_partitions[partition_id] : large_partitions[partition_id];

	if (!cache_initialized || req_bytes == 0 || req_bytes*4 > cache_capacity) {
		return; // refuse to cache request
	}

	// look up cache entry and create a new one if needed
	CacheEntry *cache_entry = nullptr;
	auto cache_it = partition.file_cache.find(filename);
	if (cache_it == partition.file_cache.end()) {
		auto new_entry = make_uniq<CacheEntry>();
		new_entry->filename = filename;
		new_entry->total_size = 0;
		cache_entry = new_entry.get();
		partition.file_cache[filename] = std::move(new_entry);
		partition.AddToLRUFront(cache_entry);
	} else {
		cache_entry = cache_it->second.get();
		partition.TouchLRU(cache_entry);
	}

	// Determine exactly which range to cache (in the meantime, some pieces may have gotten cached)
	idx_t nr_bytes = req_bytes, actual_bytes = 0, start_pos = position;
	auto hit_range = AnalyzeRange(cache_entry, start_pos, nr_bytes);
	auto end_pos = start_pos + nr_bytes;
	idx_t offset = 0;
	if (hit_range) {
		offset = hit_range->end - start_pos;
		start_pos = hit_range->end;
	}
	if (end_pos > start_pos) { // this could lead to a small range getting cached in the large cache, but OK
		actual_bytes = end_pos - start_pos;
	}

	// finally, time to make a new CacheRange
	duckdb::shared_ptr<CacheRange> new_range = duckdb::make_shared_ptr<CacheRange>(start_pos, end_pos, partition_id);
	{
		std::lock_guard<std::mutex> lock(cache_mutex);
		if (actual_bytes == 0 || partition.current_size + actual_bytes > GetPartitionCapacity(partition_id, is_small)) {
			return; // back off
		}
		// administer the effective new cached range in all stats under lock
		partition.current_size += actual_bytes;
		current_cache_ranges++;
		current_cache_size += actual_bytes;

		// add the new range to the cache entry
		cache_entry->ranges[start_pos] = new_range;
		cache_entry->total_size += actual_bytes;
	}

	// epilogue: allocate and copy data into tmp_buffer outside lock
	auto mem_buffer = duckdb::make_shared_ptr<SharedBuffer>(actual_bytes);
	std::memcpy(mem_buffer->data.get(), static_cast<const char *>(buffer) + offset, actual_bytes);
	new_range->memory_buffer = mem_buffer;
	QueueCacheWrite(partition_id, filename, new_range); 	// Queue for writing to disk
}

bool BlobCache::EvictPartition(idx_t partition_id, bool is_small) {
	// this executes unlocked, but the eviction_in_progress ensures only one eviction runs at-a-time
	// Calculate partition capacity based on size category
	auto& partition = is_small ? small_partitions[partition_id] : large_partitions[partition_id];
	idx_t target_size = GetPartitionCapacity(partition_id, is_small) * 3 / 4;
	LogDebug(StringUtil::Format("Starting partition %llu eviction: current=%llu, target=%llu, using file %s",
	                            partition_id, partition.current_size, target_size, partition.file1));

	// This prevents writer threads from continuing to write to files we're about to replace
	idx_t old_range_count = 0;
	for (auto& file_pair : partition.file_cache) {
		old_range_count += file_pair.second->ranges.size();
	}

	// Create/truncate the new file
	int new_fd = open(partition.file2.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
	if (new_fd < 0) {
		LogError(StringUtil::Format("Failed to create eviction file '%s': %s",
		                            partition.file2.c_str(), strerror(errno)));
		eviction_in_progress = false;
		return false;
	}

	// Open old file for reading
	int old_fd = open(partition.file1.c_str(), O_RDONLY);
	if (old_fd < 0) {
		close(new_fd);
		LogError(StringUtil::Format("Failed to open old file '%s': %s",
		                            partition.file1.c_str(), strerror(errno)));
		eviction_in_progress = false;
		return false;
	}

	// Create new cache map that will replace the old one
	unordered_map<string, unique_ptr<CacheEntry>> new_file_cache;
	CacheEntry* new_lru_head = nullptr;
	CacheEntry* new_lru_tail = nullptr;
	idx_t new_partition_size = 0;
	idx_t new_range_count = 0;
	idx_t file_offset = 0;

	// Single loop over LRU cache entries (most recent to least recent)
	CacheEntry* current = partition.lru_head;
	while (current && new_partition_size < target_size) {
		// Create new entry for the new map
		auto new_entry = make_uniq<CacheEntry>();
		new_entry->filename = current->filename;
		new_entry->total_size = 0;

		// Copy ranges that fit within target size
		for (auto& range_pair : current->ranges) {
			auto& range = range_pair.second;
			idx_t range_size = range->end - range->start;
			if (new_partition_size + range_size <= target_size) {
				unique_ptr<char[]> buffer(new char[range_size]);

				if (lseek(old_fd, range->file_offset, SEEK_SET) != range->file_offset) {
					LogError(StringUtil::Format("Failed to seek in file '%s' to %ulld",
					                            partition.file1.c_str(), range->file_offset, strerror(errno)));
					close(old_fd);
					close(new_fd);
					eviction_in_progress = false;
					return false;
				}
				ssize_t bytes_read = read(old_fd, buffer.get(), range_size);
				if (bytes_read != static_cast<ssize_t>(range_size)) {
					LogError(StringUtil::Format("Failed to read  file '%s' at range %ulld-%ulld: %s",
					                            partition.file1.c_str(), range->file_offset,
													range_size, strerror(errno)));
					close(old_fd);
					close(new_fd);
					eviction_in_progress = false;
					return false;
				}
				ssize_t bytes_written = write(new_fd, buffer.get(), range_size);
				if (bytes_written != static_cast<ssize_t>(range_size)) {
					LogError(StringUtil::Format("Failed to write file '%s' with %ulld bytes: %s",
												partition.file2.c_str(), range_size, strerror(errno)));
					close(old_fd);
					close(new_fd);
					eviction_in_progress = false;
					return false;
				}

				// Create new range with updated file offset
				auto new_range = make_shared_ptr<CacheRange>(range->start, range->end, partition_id);
				new_range->file_offset = file_offset;
				new_range->usage_count = range->usage_count;
				new_range->bytes_from_cache = range->bytes_from_cache;

				// Add to new entry
				new_entry->ranges[range->start] = new_range;
				new_entry->total_size += range_size;
				new_partition_size += range_size;
				new_range_count++;
				file_offset += range_size;
			}
		}

		// Add new entry to new map if it has ranges
		if (!new_entry->ranges.empty()) {
			CacheEntry* entry_ptr = new_entry.get();
			new_file_cache[current->filename] = std::move(new_entry);

			// Add to new LRU list (maintain LRU order)
			entry_ptr->lru_prev = nullptr;
			entry_ptr->lru_next = new_lru_head;
			if (new_lru_head) {
				new_lru_head->lru_prev = entry_ptr;
			}
			new_lru_head = entry_ptr;
			if (!new_lru_tail) {
				new_lru_tail = entry_ptr;
			}
		}
		current = current->lru_next;
	}

	// Close file descriptors
	close(old_fd);
	close(new_fd);

	// Update global statistics and partition state atomically
	idx_t evicted_size = partition.current_size - new_partition_size;
	idx_t evicted_ranges = old_range_count - new_range_count;
	{
		std::lock_guard<std::mutex> global_lock(cache_mutex);
		// Update global statistics
		current_cache_ranges -= evicted_ranges;
		current_cache_size -= evicted_size;

		// Replace old map with new map and update partition state
		partition.file_cache = std::move(new_file_cache);
		partition.lru_head = new_lru_head;
		partition.lru_tail = new_lru_tail;
		partition.current_size = new_partition_size;

		// rotate the files
		auto swap = partition.file1;
		partition.file1 = partition.file2;
		partition.file2 = swap;
	}
	unlink(partition.file2.c_str()); // Delete the old file
	LogDebug(StringUtil::Format("Partition %llu eviction complete: new size=%llu, now using file %s",
	                            partition_id, partition.current_size, partition.file1));
	eviction_in_progress = false;
	return partition.current_size <= target_size;
}


//===----------------------------------------------------------------------===//
// Multi-threaded background cache writer implementation
//===----------------------------------------------------------------------===//


void BlobCache::CacheWriterThreadLoop(idx_t partition_id) {
	LogDebug(StringUtil::Format("Cache writer thread %zu started", partition_id));
	
	while (!database_shutting_down) {
		pair<string, duckdb::shared_ptr<CacheRange>> job;
		bool has_job = false;
		// Wait for a job or shutdown signal for this thread's queue
		{
			std::unique_lock<std::mutex> lock(write_queue_mutexes[partition_id]);
			write_queue_cvs[partition_id].wait(lock, [this, partition_id] {
				return !write_job_queues[partition_id].empty() || database_shutting_down;
			});

			if (database_shutting_down && write_job_queues[partition_id].empty()) {
				break;
			}
			if (!write_job_queues[partition_id].empty()) {
				job = std::move(write_job_queues[partition_id].front());
				write_job_queues[partition_id].pop();
				has_job = true;
			}
		}
		if (!has_job) {
			// if there are no more jobs
			EvictPartition(partition_id, false);
			EvictPartition(partition_id, true);
			continue;
		}
		// Process the cache write job
		const string &filename = job.first;
		auto range = job.second;

		idx_t data_size = range->end - range->start;
		bool write_success = WriteToCacheFile(db, partition_id, range->memory_buffer->data.get(), data_size, range->file_offset);

		// Try to mark as completed using atomic compare-and-swap
		if (write_success) {
			LogDebug(StringUtil::Format("Background writer completed range [%lld, %lld) for file '%s' in partition %llu",
			                            range->start, range->end, filename.c_str(), partition_id));
			// Clear memory buffer since data is now on disk
			range->memory_buffer.reset();
		} else {
			LogError(StringUtil::Format("Failed to write range [%lld, %lld) for file '%s' to disk",
			                            range->start, range->end, filename.c_str()));
		}
	}
}

//===----------------------------------------------------------------------===//
// Disk cache helper methods
//===----------------------------------------------------------------------===//
void BlobCache::ConfigureCache(const string &directory, idx_t max_size_bytes) {
	// Note: Assumes cache_mutex is already held by caller
	RemoveDirectory(db, cache_dir_path, path_separator); // Remove old directory and all its contents
	cache_dir_path = directory;
	cache_capacity = max_size_bytes;
	for (idx_t i = 0; i < NUM_PARTITIONS*2; i++) {
		small_partitions[i].file_cache.clear();
		large_partitions[i].file_cache.clear();
		small_partitions[i].lru_head = nullptr;
		large_partitions[i].lru_head = nullptr;
		small_partitions[i].lru_tail = nullptr;
		large_partitions[i].lru_tail = nullptr;
		small_partitions[i].file1 = StringUtil::Format("%02da-small.blobcache", i);
		large_partitions[i].file1 = StringUtil::Format("%02da-large.blobcache", i);
		small_partitions[i].file2 = StringUtil::Format("%02db-small.blobcache", i);
		large_partitions[i].file2 = StringUtil::Format("%02db-large.blobcache", i);
	}
	current_cache_ranges = 0;
	current_cache_size = 0;
	LogDebug(StringUtil::Format("Cache configuration complete: directory='%s' max_size=%llu bytes",
	                           cache_dir_path.c_str(), cache_capacity));
}

vector<BlobCache::RangeInfo> BlobCache::GetCacheStatistics() const {
	vector<RangeInfo> range_infos;
	if (!cache_initialized) {
		return range_infos;
	}

	std::lock_guard<std::mutex> lock(cache_mutex);
	range_infos.reserve(current_cache_ranges);

	// Collect from all partitions (both small and large)
	for (idx_t i = 0; i < NUM_PARTITIONS*2; i++) {
		auto& partition = (i > NUM_PARTITIONS) ? small_partitions[i-NUM_PARTITIONS] : large_partitions[i];

		// Collect cache entries in LRU order (least recently used first)
		CacheEntry* current = partition.lru_tail;
		while (current) {
			RangeInfo info;
			info.protocol = "unknown";
			info.filename = current->filename;
			auto pos = info.filename.find_first_of("://");
			if (pos < info.filename.length()) {
				info.protocol = info.filename.substr(0, pos);
				info.filename = info.filename.substr(pos + 3, info.filename.length() - (pos + 3));
			}
			for (const auto &range_pair : current->ranges) {
				info.file_offset = range_pair.second->file_offset;
				info.start = range_pair.second->start;
				info.end = range_pair.second->end;
				info.usage_count = range_pair.second->usage_count;
				info.bytes_from_cache = range_pair.second->bytes_from_cache;
				range_infos.push_back(info);
			}
			current = current->lru_prev;
		}
	}
	return range_infos;
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
	if (!cache_initialized) {
		return;
	}
	LogDebug("Reordering LRU for cache entries that no longer match current caching patterns");

	// Go through all partitions (both small and large)
	std::lock_guard<std::mutex> lock(cache_mutex);
	for (idx_t i = 0; i < TOTAL_PARTITIONS; i++) {
		auto& partition = partitions[i];

		vector<CacheEntry*> entries_to_move_to_tail;

		// Go through all cached entries in this partition
		for (const auto &entry_pair : partition.file_cache) {
			const string &filename = entry_pair.first;
			CacheEntry* entry = entry_pair.second.get();
			if (!ShouldCacheFile(filename, opener)) {
				entries_to_move_to_tail.push_back(entry);
			}
		}

		// Move non-qualifying entries to the tail (least recently used) of LRU list
		// This makes them first candidates for eviction
		for (CacheEntry* entry : entries_to_move_to_tail) {
			// Remove from current LRU position
			partition.RemoveFromLRU(entry);
			// Add to tail (least recently used position)
			partition.AddToLRUTail(entry);
		}

		if (!entries_to_move_to_tail.empty()) {
			LogDebug(StringUtil::Format("Moved %zu non-qualifying entries to LRU tail in partition %llu",
			                           entries_to_move_to_tail.size(), i));
		}
	}
	LogDebug("Completed LRU reordering for pattern change");
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


} // namespace duckdb