#pragma once

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/common/opener_file_system.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/string_type.hpp"
#include <iostream>
#include <memory>
#include <mutex>
#include <map>
#include <sstream>
#include <iomanip>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <dirent.h>
#include <cstring>
#include <bitset>
#include <map>
#include <thread>
#include <queue>
#include <condition_variable>
#include <atomic>
#include <regex>

namespace duckdb {

//===----------------------------------------------------------------------===//
// Cache data structures
//===----------------------------------------------------------------------===//
// Wrapper to manage shared buffer allocation
struct SharedBuffer {
	unique_ptr<char[]> data;
	explicit SharedBuffer(idx_t size) : data(new char[size]) {}
};

struct CacheRange {
	idx_t start = 0, end = 0; // range in remote blob file
	idx_t file_offset = 0;  // Offset in the cache local file where this range is stored
	idx_t usage_count = 0, bytes_from_cache = 0;  // statistics
	duckdb::shared_ptr<SharedBuffer> memory_buffer;  // Temporary in-memory buffer until disk write completes
	bool disk_write_complete = false;  // Whether the background disk write has completed (lock-free)
	CacheRange(idx_t start, idx_t end) : start(start), end(end) {}
};

struct CacheEntry {
	string filename; // full URL
	map<idx_t, duckdb::shared_ptr<CacheRange>> ranges;  // Map of start position to shared CacheRange
	idx_t cached_file_size = 0;  // Total bytes cached for this file

	// LRU linked list pointers
	CacheEntry* lru_prev = nullptr;
	CacheEntry* lru_next = nullptr;
};

//===----------------------------------------------------------------------===//
// BlobCache - Main cache implementation
//===----------------------------------------------------------------------===//
class BlobCache {
private:
	static constexpr idx_t DEFAULT_CACHE_CAPACITY = 1024ULL * 1024 * 1024; // 1GB default
	
	DatabaseInstance *db_instance;
	string path_separator; // make this work on windoze as well
	string cache_dir_path; // local directory where the cached files are written
	idx_t cache_capacity, current_cache_size, current_cache_ranges;
	bool cache_initialized = false;
	
	// Cache state
	unordered_map<string, unique_ptr<CacheEntry>> key_cache; // Maps cache key to CacheEntry
	CacheEntry* lru_head = nullptr; // Most recently used
	CacheEntry* lru_tail = nullptr; // Least recently used
	mutable std::mutex cache_mutex; // Protects key_cache, LRU list, current_cache_size
	
	// Cached regex patterns for file filtering (updated via callback)
	mutable std::mutex regex_mutex; // Protects cached regex state
	vector<std::regex> cached_regexps; // Compiled regex patterns
	bool conservative_mode; // True if blobcache_regexps is empty
	
	// Directory creation optimization
	std::bitset<4095> subdirs_created; // Track which subdirectories have been created (12-bit prefix = 4095 possibilities)
	
	// Multi-threaded background cache writer system
	static constexpr idx_t MAX_WRITER_THREADS = 256;
	std::array<std::thread, MAX_WRITER_THREADS> cache_writer_threads;
	std::array<std::queue<pair<string, duckdb::shared_ptr<CacheRange>>>, MAX_WRITER_THREADS> write_job_queues;
	std::array<std::mutex, MAX_WRITER_THREADS> write_queue_mutexes;
	std::array<std::condition_variable, MAX_WRITER_THREADS> write_queue_cvs;
	std::atomic<bool> shutdown_writer_threads;
	std::atomic<bool> database_shutting_down; // Flag to indicate database shutdown in progress
	idx_t num_writer_threads;
	
	void CacheWriterThreadLoop(idx_t thread_id);
	void QueueCacheWrite(const string &cache_key, duckdb::shared_ptr<CacheRange> range);
	idx_t GetPartitionForKey(const string &cache_key) const;
	
	// Cache management helper methods
	bool EvictToCapacity(idx_t required_space, const string &exclude_key = "");
	void EvictFile(const string &filename);
	void EvictKey(const string &cache_key);
	idx_t CalculateFileBytes(const string &cache_key);
	bool HasUnfinishedWrites(const string &cache_key);

	// LRU list management and cache key generation methods - implemented inline at end of class
	
	// Disk cache helper methods
	void InitializeCacheDirectory();
	void CleanCacheDirectory();
	void EnsureSubdirectoryExists(const string &cache_key);
	int OpenCacheFile(const string &cache_key, int flags);
	string GetCacheFilePath(const string &cache_key);
	bool WriteToCacheFile(const string &cache_key, const void *data, idx_t size, idx_t &file_offset);
	bool ReadFromCacheFile(const string &cache_key, idx_t file_offset, void *buffer, idx_t length);
	void DeleteCacheFile(const string &cache_key);

	friend class BlobFilesystemWrapper;

public:
	// Constructor/Destructor
	explicit BlobCache(DatabaseInstance *db_instance = nullptr)
	    : db_instance(db_instance), path_separator("/"), cache_capacity(DEFAULT_CACHE_CAPACITY),
	      current_cache_size(0), current_cache_ranges(0), cache_initialized(false), conservative_mode(true),
	      shutdown_writer_threads(false), database_shutting_down(false), num_writer_threads(1) {
		// Don't start the cache writer threads in constructor to avoid potential deadlocks
		// They will be started when needed in InitializeCache
	}
	
	~BlobCache() {
		// Set shutdown flag to prevent further logging attempts  
		database_shutting_down = true;
		StopCacheWriterThreads();
	}
	
	// Thread management
	void StartCacheWriterThreads(idx_t thread_count);
	void StopCacheWriterThreads();
	
	// Logging methods with shutdown-safe null guards
	void LogDebug(const string &message) const {
		if (!database_shutting_down.load() && db_instance) {
			DUCKDB_LOG_DEBUG(*db_instance, "[BlobCache] %s", message.c_str());
		}
	}
	void LogError(const string &message) const {
		if (!database_shutting_down.load() && db_instance) {
			DUCKDB_LOG_ERROR(*db_instance, "[BlobCache] %s", message.c_str());
		}
	}
	
	// Core cache operations
	void InsertCache(const string &cache_key, const string &filename, idx_t start_pos, const void *buffer, idx_t length);
	// Combined cache lookup and read - returns bytes read from cache, adjusts nr_bytes if needed
	idx_t ReadFromCache(const string &cache_key, idx_t position, void *buffer, idx_t &nr_bytes);

	// Range merging
	void TryMergeWithPrevious(const string &cache_key, duckdb::shared_ptr<CacheRange> range);
	
	// Cache management
	void ClearCache() {
		key_cache.clear();
		lru_head = nullptr;
		lru_tail = nullptr;
		current_cache_size = 0;
		current_cache_ranges = 0;
		subdirs_created.reset();
	}
	void InvalidateCache(const string &filename);
	void InitializeCache(const string &directory, idx_t max_size_bytes, idx_t writer_threads = 1);
	void ConfigureCache(const string &directory, idx_t max_size_bytes, idx_t writer_threads = 1);
	bool IsCacheInitialized() const { return cache_initialized; }
	
	// Cache status getters
	string GetCachePath() const { 
		return cache_dir_path;
	}
	idx_t GetMaxSizeBytes() const { 
		return cache_capacity;
	}
	idx_t GetCurrentSizeBytes() const { 
		return current_cache_size;
	}
	idx_t GetWriterThreadCount() const { 
		return num_writer_threads;
	}
	
	// Statistics structure
	struct RangeInfo {
		string protocol;
		string filename;
		idx_t file_offset;
		idx_t start, end;
		idx_t usage_count;
		idx_t bytes_from_cache;
	};

	// Statistics
	vector<RangeInfo> GetCacheStatistics() const;
	
	// Configuration and caching policy
	bool ShouldCacheFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) const;
	
	// Regex pattern management
	void UpdateRegexPatterns(const string &regex_patterns_str);
	void PurgeCacheForPatternChange(optional_ptr<FileOpener> opener = nullptr);

	// Cache key generation
	string GenerateCacheKey(const string &filename);

	//===----------------------------------------------------------------------===//
	// Inline implementations for trivial methods
	//===----------------------------------------------------------------------===//

	// LRU list management methods
	void TouchLRU(CacheEntry* entry) {
		// Move entry to front of LRU list (most recently used)
		// Note: Must be called with cache_mutex held
		if (entry == lru_head) {
			return; // Already at front
		}

		// Remove from current position
		RemoveFromLRU(entry);

		// Add to front
		AddToLRUFront(entry);
	}

	void RemoveFromLRU(CacheEntry* entry) {
		// Remove entry from LRU list
		// Note: Must be called with cache_mutex held
		if (entry->lru_prev) {
			entry->lru_prev->lru_next = entry->lru_next;
		} else {
			lru_head = entry->lru_next;
		}

		if (entry->lru_next) {
			entry->lru_next->lru_prev = entry->lru_prev;
		} else {
			lru_tail = entry->lru_prev;
		}

		entry->lru_prev = nullptr;
		entry->lru_next = nullptr;
	}

	void AddToLRUFront(CacheEntry* entry) {
		// Add entry to front of LRU list (most recently used)
		// Note: Must be called with cache_mutex held
		entry->lru_prev = nullptr;
		entry->lru_next = lru_head;

		if (lru_head) {
			lru_head->lru_prev = entry;
		}
		lru_head = entry;

		if (!lru_tail) {
			lru_tail = entry;
		}
	}

private:
	// Helper method for cache key generation
	string ExtractValidSuffix(const string &filename, size_t max_chars);
};

} // namespace duckdb
