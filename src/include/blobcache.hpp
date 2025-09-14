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

//===----------------------------------------------------------------------====//
// LRU Cache for managing file eviction
//===----------------------------------------------------------------------===//
class LRUCache {
private:
	struct LRUNode {
		string key;
		string filename;
		LRUNode* prev;
		unique_ptr<LRUNode> next;
		
		LRUNode(const string &k, const string &f) : key(k), filename(f), prev(nullptr), next(nullptr) {}
	};
	
	unordered_map<string, LRUNode*> cache_map;
	unique_ptr<LRUNode> head;
	LRUNode* tail;
	
	void AddToFront(unique_ptr<LRUNode> node) {
		if (!head) {
			head = std::move(node);
			tail = head.get();
		} else {
			node->next = std::move(head);
			if (node->next) {
				node->next->prev = node.get();
			}
			head = std::move(node);
		}
	}
	
	void MoveToFront(LRUNode* node) {
		if (node == head.get()) {
			return; // Already at front
		}
		
		// Remove from current position
		if (node->prev) {
			node->prev->next = std::move(node->next);
		}
		if (node->next) {
			node->next->prev = node->prev;
		}
		if (node == tail) {
			tail = node->prev;
		}
		
		// Move to front
		unique_ptr<LRUNode> node_ptr;
		if (node->prev) {
			node_ptr = std::move(node->prev->next);
		} else {
			// This should never happen since we checked node != head
			throw InternalException("Invalid LRU state");
		}
		
		node_ptr->prev = nullptr;
		node_ptr->next = std::move(head);
		if (head) {
			head->prev = node_ptr.get();
		}
		head = std::move(node_ptr);
	}

public:
	LRUCache() : head(nullptr), tail(nullptr) {}
	
	~LRUCache() {
		Clear();
	}
	
	void Touch(const string &key, const string &filename) {
		auto it = cache_map.find(key);
		if (it != cache_map.end()) {
			// Move existing node to front
			MoveToFront(it->second);
		} else {
			// Add new node to front
			auto node = make_uniq<LRUNode>(key, filename);
			cache_map[key] = node.get();
			AddToFront(std::move(node));
		}
	}
	
	bool Contains(const string &key) const {
		return cache_map.find(key) != cache_map.end();
	}
	
	string GetLRU() const {
		if (!tail) {
			return "";
		}
		return tail->key;
	}
	
	string GetLRUFilename() const {
		if (!tail) {
			return "";
		}
		return tail->filename;
	}
	
	void Remove(const string &key) {
		auto it = cache_map.find(key);
		if (it == cache_map.end()) {
			return;
		}
		
		LRUNode* node = it->second;
		
		// Remove from linked list
		if (node->prev) {
			node->prev->next = std::move(node->next);
		} else {
			head = std::move(node->next);
		}
		
		if (node->next) {
			node->next->prev = node->prev;
		} else {
			tail = node->prev;
		}
		
		// Remove from map
		cache_map.erase(it);
		
		// Node will be automatically deleted when unique_ptr goes out of scope
	}
	
	bool Empty() const {
		return cache_map.empty();
	}
	
	void Clear() {
		cache_map.clear();
		head = nullptr;
		tail = nullptr;
	}
	
	// Get all keys in LRU order (least recently used first, i.e., from tail to head)
	vector<string> GetKeysInLRUOrder() const {
		vector<string> keys;
		LRUNode* current = tail;
		while (current) {
			keys.push_back(current->key);
			current = current->prev;
		}
		return keys;
	}
};

//===----------------------------------------------------------------------===//
// Cache data structures
//===----------------------------------------------------------------------===//
// Wrapper to manage shared buffer allocation
struct SharedBuffer {
	unique_ptr<char[]> data;
	explicit SharedBuffer(idx_t size) : data(new char[size]) {}
};

struct CacheRange {
	idx_t start = 0;
	idx_t end = 0;
	idx_t file_offset = 0;  // Offset in the cache file where this range is stored
	idx_t usage_count = 0;  // statistics - protected by ranges_mutex
	idx_t bytes_from_cache = 0;  // statistics - protected by ranges_mutex
	duckdb::shared_ptr<SharedBuffer> memory_buffer;  // Temporary in-memory buffer until disk write completes
	bool disk_write_complete = false;  // Whether the background disk write has completed

	// Default constructor
	CacheRange() = default;

	// Constructor for disk-backed range
	CacheRange(idx_t start, idx_t end, idx_t file_offset)
		: start(start), end(end), file_offset(file_offset), disk_write_complete(true) {}

	// Constructor for memory-backed range
	CacheRange(idx_t start, idx_t end, duckdb::shared_ptr<SharedBuffer> buffer)
		: start(start), end(end), file_offset(0), memory_buffer(std::move(buffer)), disk_write_complete(false) {}
};

struct CacheEntry {
	string filename;
	map<idx_t, duckdb::shared_ptr<CacheRange>> ranges;  // Map of start position to shared CacheRange
	mutable std::mutex ranges_mutex;  // Protects the ranges map and individual range statistics
	idx_t cached_file_size = 0;  // Total bytes cached for this file
};

//===----------------------------------------------------------------------===//
// BlobCache - Main cache implementation
//===----------------------------------------------------------------------===//
class BlobCache {
private:
	static constexpr idx_t DEFAULT_CACHE_CAPACITY = 1024ULL * 1024 * 1024; // 1GB default
	
	DatabaseInstance *db_instance;
	string path_separator;
	string cache_dir_path;
	idx_t cache_capacity;
	idx_t current_cache_size;
	bool cache_initialized;
	
	// Cache state
	unordered_map<string, CacheEntry> key_cache; // Maps cache key to CacheEntry
	LRUCache lru_cache;
	mutable std::mutex cache_mutex; // Protects key_cache, lru_cache, current_cache_size
	
	// Cached regex patterns for file filtering (updated via callback)
	mutable std::mutex regex_mutex; // Protects cached regex state
	vector<std::regex> cached_regexps; // Compiled regex patterns
	bool conservative_mode; // True if blobcache_regexps is empty
	
	// Directory creation optimization
	std::bitset<65536> subdirs_created; // Track which subdirectories have been created (16-bit hex = 65536 possibilities)
	
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
	void QueueCacheWrite(const string &filename, duckdb::shared_ptr<CacheRange> range);
	idx_t GetPartitionForKey(const string &cache_key) const;
	
	// Cache management helper methods
	bool EvictToCapacity(idx_t required_space, const string &exclude_key = "");
	void EvictFile(const string &filename);
	void EvictKey(const string &cache_key);
	idx_t CalculateFileBytes(const string &cache_key);
	bool HasUnfinishedWrites(const string &cache_key);
	
	// Cache key generation methods
	string GenerateCacheKey(const string &filename);
	string ExtractValidSuffix(const string &filename, size_t max_chars);
	
	// Disk cache helper methods
	void InitializeCacheDirectory();
	void CleanCacheDirectory();
	void EnsureSubdirectoryExists(const string &cache_key);
	int OpenCacheFile(const string &cache_key, int flags);
	bool CreateCacheSubdirectory(const string &cache_key);
	string GetCacheFilePath(const string &cache_key);
	bool WriteToCacheFile(const string &cache_key, const void *data, idx_t size, idx_t &file_offset);
	bool ReadFromCacheFile(const string &cache_key, idx_t file_offset, void *buffer, idx_t length);
	void DeleteCacheFile(const string &cache_key);
	string CalculateFileHash(const string &filename);
	
	friend class BlobFilesystemWrapper;

public:
	// Constructor/Destructor
	explicit BlobCache(DatabaseInstance *db_instance = nullptr)
	    : db_instance(db_instance), path_separator("/"), cache_capacity(DEFAULT_CACHE_CAPACITY), 
	      current_cache_size(0), cache_initialized(false), conservative_mode(true),
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
		// Skip logging if database is shutting down to prevent access to destroyed instance
		if (database_shutting_down.load()) {
			return;
		}
		if (db_instance) {
			DUCKDB_LOG_DEBUG(*db_instance, "[BlobCache] %s", message.c_str());
		}
	}
	void LogError(const string &message) const {
		// Skip logging if database is shutting down to prevent access to destroyed instance
		if (database_shutting_down.load()) {
			return;
		}
		if (db_instance) {
			DUCKDB_LOG_ERROR(*db_instance, "[BlobCache] %s", message.c_str());
		}
	}

	
	// Core cache operations
	void InsertCache(const string &filename, idx_t start_pos, const void *buffer, int64_t length);
	// Combined cache lookup and read - returns bytes read from cache, adjusts nr_bytes if needed
	idx_t ReadFromCache(const string &cache_key, idx_t position, void *buffer, idx_t &nr_bytes);
	
	// Cache management
	void ClearCache() {
		key_cache.clear();
		lru_cache.Clear();
		current_cache_size = 0;
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
		idx_t start, end;
		idx_t usage_count;
		idx_t bytes_from_cache;
	};
	
	// Statistics
	vector<RangeInfo> GetCacheStatistics(idx_t max_results = 10000) const;
	
	// Configuration and caching policy
	bool ShouldCacheFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) const;
	
	// Regex pattern management
	void UpdateRegexPatterns(const string &regex_patterns_str);
	void PurgeCacheForPatternChange(optional_ptr<FileOpener> opener = nullptr);
};

} // namespace duckdb
