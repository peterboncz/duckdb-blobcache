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
	idx_t file_offset = 0;  // Offset in the partition file where this range is stored
	idx_t usage_count = 0, bytes_from_cache = 0;  // statistics
	duckdb::shared_ptr<SharedBuffer> memory_buffer;  // Temporary in-memory buffer until disk write completes
	CacheRange(idx_t start, idx_t end, idx_t partition_id) : start(start), end(end) {}
};

struct CacheEntry {
	string filename; // full URL
	map<idx_t, duckdb::shared_ptr<CacheRange>> ranges;  // Map of start position to shared CacheRange
	idx_t total_size = 0;  // Total disk space used by this entry (includes invalidated ranges)

	// LRU linked list pointers
	CacheEntry* lru_prev = nullptr;
	CacheEntry* lru_next = nullptr;
};

//===----------------------------------------------------------------------===//
// BlobCache - Main cache implementation
//===----------------------------------------------------------------------===//
class BlobCache {
private:
	DatabaseInstance *db;
	string path_separator; // make this work on windoze as well
	string cache_dir_path; // local directory where the cached files are written
	idx_t cache_capacity;
	bool cache_initialized = false;
	
	static constexpr idx_t NUM_PARTITIONS = 16;
	static constexpr idx_t SMALL_RANGE_THRESHOLD = 512; // Ranges <= 512 bytes go to small cache

	struct Partition {
		unordered_map<string, duckdb::unique_ptr<CacheEntry>> file_cache; // Maps filename to CacheEntry
		CacheEntry* lru_head = nullptr; // Most recently used in this partition
		CacheEntry* lru_tail = nullptr; // Least recently used in this partition
		idx_t current_size = 0; // Current bytes in this partition
		string file1, file2;

		// LRU list management methods (per-partition)
		void TouchLRU(CacheEntry* entry) {
			// Move entry to front of partition's LRU list
			// Note: Must be called with partition mutex held
			if (entry == lru_head) {
				return; // Already at front
			}
			RemoveFromLRU(entry);
			AddToLRUFront(entry);
		}

		void RemoveFromLRU(CacheEntry* entry) {
			// Remove entry from partition's LRU list
			// Note: Must be called with partition mutex held
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
			// Add entry to front of partition's LRU list
			// Note: Must be called with partition mutex held
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

		void AddToLRUTail(CacheEntry* entry) {
			// Add entry to tail of partition's LRU list (least recently used position)
			// Note: Must be called with partition mutex held
			entry->lru_next = nullptr;
			entry->lru_prev = lru_tail;
			if (lru_tail) {
				lru_tail->lru_next = entry;
			}
			lru_tail = entry;
			if (!lru_head) {
				lru_head = entry;
			}
		}

		void InvalidateCachedFile(const string &filename) {
			auto cache_it = file_cache.find(filename);
			if (cache_it != file_cache.end()) {
				CacheEntry *entry = cache_it->second.get();
				entry->ranges.clear(); // Clear all ranges from the entry but keep total_size unchanged
				RemoveFromLRU(entry);
				AddToLRUTail(entry); // Move to tail of LRU
			}
		}

		void Purge(optional_ptr<FileOpener> opener) {
			for (const auto &entry_pair : file_cache) {
				const string &filename = entry_pair.first;
				CacheEntry* entry = entry_pair.second.get();
				if (!ShouldCacheFile(filename, opener)) {
					entry->ranges.clear();
					RemoveFromLRU(entry);
					AddToLRUTail(entry);
				}
			}
		}
	};

	std::array<Partition, NUM_PARTITIONS> small_partitions, large_partitions;
	std::atomic<bool> eviction_in_progress{false}; // Global eviction flag
	
	// Cached regex patterns for file filtering (updated via callback)
	mutable std::mutex regex_mutex; // Protects cached regex state
	vector<std::regex> cached_regexps; // Compiled regex patterns
	bool conservative_mode = true; // True if blobcache_regexps is empty
	
	// Global cache statistics
	idx_t current_cache_ranges = 0; // Total ranges across all partitions (for statistics reserve)
	idx_t current_cache_size = 0; // Current size of  cache
	mutable std::mutex cache_mutex; // Protects global statistics
	
	// Multi-threaded background cache writer system
	std::array<std::thread, NUM_PARTITIONS> cache_writer_threads;
	std::array<std::queue<pair<string, duckdb::shared_ptr<CacheRange>>>, NUM_PARTITIONS> write_job_queues;
	std::array<std::mutex, NUM_PARTITIONS> write_queue_mutexes;
	std::array<std::condition_variable, NUM_PARTITIONS> write_queue_cvs;
	std::atomic<bool> database_shutting_down; // Flag to indicate database shutdown in progress

	void CacheWriterThreadLoop(idx_t partition_id);
	void QueueCacheWrite(idx_t partition_id, const string &filename, duckdb::shared_ptr<CacheRange> range) {
		{
			std::lock_guard<std::mutex> lock(write_queue_mutexes[partition_id]);
			write_job_queues[partition_id].emplace(filename, range);
		}
		write_queue_cvs[partition_id].notify_one();
	}

	idx_t GetPartitionForFilename(const string &filename) const {
		// Use hash of filename to determine partition for writer threads, considering size category
		hash_t hash_value = Hash(string_t(filename.c_str(), static_cast<uint32_t>(filename.length())));
		return hash_value % NUM_PARTITIONS;
	}

	idx_t GetPartitionCapacity(idx_t partition_id, bool is_small) const {
		idx_t partition_capacity = cache_capacity / (NUM_PARTITIONS+1);
		return is_small ?  partition_capacity - large_partitions[partition_id].current_size : 0.9 * partition_capacity;
	}

	// Cache management helper methods
	bool EvictPartition(idx_t partition_id, bool is_small);

	// Internal configuration helper method (assumes cache_mutex is held)
	void ConfigureCache(const string &directory, idx_t max_size_bytes);

	void InvalidateCachedFile(const string &filename) {
		auto partition_id = GetPartitionForFilename(filename);
		std::lock_guard<std::mutex> lock(cache_mutex);
		small_partitions[partition_id].InvalidateCachedFile(filename);
		large_partitions[partition_id].InvalidateCachedFile(filename);
	}

	friend class BlobFilesystemWrapper;

public:
	// Constructor/Destructor
	explicit BlobCache(DatabaseInstance *db = nullptr)
	    : db(db), path_separator("/") {
		for (idx_t i = 0; i < NUM_PARTITIONS; i++) {
			cache_writer_threads[i] = std::thread([this, i] { CacheWriterThreadLoop(i); });
		}
	}

	~BlobCache() {
		// Set shutdown flag to prevent further logging attempts  
		database_shutting_down = true;
	}
	
	// Logging methods with shutdown-safe null guards
	void LogDebug(const string &message) const {
		if (!database_shutting_down.load() && db) {
			DUCKDB_LOG_DEBUG(*db, "[BlobCache] %s", message.c_str());
		}
	}
	void LogError(const string &message) const {
		if (!database_shutting_down.load() && db) {
			DUCKDB_LOG_ERROR(*db, "[BlobCache] %s", message.c_str());
		}
	}
	
	// Core cache operations
	void InsertCache(const string &filename, idx_t start_pos, const void *buffer, idx_t length);
	// Combined cache lookup and read - returns bytes read from cache, adjusts nr_bytes if needed
	idx_t ReadFromCache(const string &filename, idx_t position, void *buffer, idx_t &nr_bytes);

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
};

} // namespace duckdb
