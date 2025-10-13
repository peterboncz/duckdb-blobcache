#pragma once

// Undefine Windows macros BEFORE any includes
#ifdef WIN32
#undef CreateDirectory
#undef MoveFile
#undef RemoveDirectory
#endif

#include "duckdb.hpp"
#include "duckdb/common/file_opener.hpp"
#include <regex>
#include <iomanip>
#include <thread>

namespace duckdb {

//===----------------------------------------------------------------------===//
// Cache data structures
//===----------------------------------------------------------------------===//

// Cache type for dual-cache system
enum class CacheType : uint8_t {
	SMALL_RANGE = 0, // Small ranges (default <= 2047 bytes)
	LARGE_RANGE = 1  // Large ranges (> threshold)
};

// Statistics structure
struct CacheRangeInfo {
	string protocol;
	string filename;
	idx_t file_offset;
	idx_t start, end;
	idx_t usage_count;
	idx_t bytes_from_cache;
};

// CacheFileBuffer - shared buffer with backpointer for background write completion
struct CacheFileBuffer {
	unique_ptr<char[]> data;
	idx_t size;                          // Size of data in buffer
	std::atomic<idx_t> *file_offset_ptr; // Backpointer to CacheRange::file_offset for write completion

	explicit CacheFileBuffer(idx_t buffer_size)
	    : data(new char[buffer_size]), size(buffer_size), file_offset_ptr(nullptr) {
	}
};

//===----------------------------------------------------------------------===//
// CacheEntry caches a file (URL) in used ranges as a ordered map of CacheRange
//===----------------------------------------------------------------------===//
struct CacheRange {
	static constexpr idx_t WRITE_NOT_COMPLETED_YET = static_cast<idx_t>(-1);

	idx_t start = 0, end = 0;                          // range in remote blob file
	std::atomic<idx_t> file_offset;                    // Offset in cache file (-1 until write completes)
	idx_t usage_count = 0, bytes_from_cache = 0;       // statistics
	duckdb::shared_ptr<CacheFileBuffer> memory_buffer; // Temporary in-memory buffer until disk write completes

	CacheRange(idx_t start, idx_t end) : start(start), end(end), file_offset(WRITE_NOT_COMPLETED_YET) {
	}
};

struct CacheEntry {
	string filename;                                     // full URL
	map<idx_t, unique_ptr<CacheRange>> ranges;           // Map of start position to unique CacheRange
	idx_t cached_file_size = 0;                          // Total bytes cached for this file
	idx_t entry_id = 0;                                  // unique entry-id ensures every filename is unique
	CacheEntry *lru_prev = nullptr, *lru_next = nullptr; // LRU linked list pointers
};

//===----------------------------------------------------------------------===//
// CacheConfig - shared configuration for cache
//===----------------------------------------------------------------------===//
struct CacheConfig {
	static constexpr idx_t DEFAULT_CACHE_CAPACITY = 1024ULL * 1024 * 1024; // 1GB default
	static constexpr idx_t DEFAULT_SMALL_RANGE_THRESHOLD = 2047;           // Default threshold for small ranges
	static constexpr idx_t FILENAME_SUFFIX_LEN = 15;

	shared_ptr<DatabaseInstance> db_instance;
	FileSystem *file_system = nullptr; // Portable file system interface
	bool cache_initialized = false;
	bool database_shutting_down = false; // Flag to indicate database shutdown in progress
	string path_sep;                     // normally "/" ,but  "\" on windows
	string cache_dir;                    // where we store data temporarilu
	idx_t total_cache_capacity = DEFAULT_CACHE_CAPACITY;
	idx_t small_range_threshold = DEFAULT_SMALL_RANGE_THRESHOLD;
	std::bitset<4095> subdirs_created; // Subdirectory tracking (shared by both caches)

	// Logging methods
	void LogDebug(const string &message) const {
		if (db_instance && !database_shutting_down) {
			DUCKDB_LOG_DEBUG(*db_instance, "[BlobCache] %s", message.c_str());
		}
	}
	void LogError(const string &message) const {
		if (db_instance && !database_shutting_down) {
			DUCKDB_LOG_ERROR(*db_instance, "[BlobCache] %s", message.c_str());
		}
	}
	// If the directory does not exist, create it, otherwise empty it
	bool InitCacheDir() {
		if (!file_system) {
			return false;
		}
		if (!file_system->DirectoryExists(cache_dir)) {
			try {
				file_system->CreateDirectory(cache_dir);
				return true;
			} catch (const std::exception &e) {
				LogError("Failed to create cache directory: " + string(e.what()));
				return false;
			}
		}
		return CleanCacheDir();
	}

	// inline to demonstrate path structure: /cache_dir/cache_key[0..3]/cache_key[4..15](s|l)entry_id:cache_key[16..]
	string GenCacheFilePath(idx_t entry_id, const string &cache_key, CacheType type) const {
		std::ostringstream oss;
		oss << std::setw(19) << std::setfill('0') << entry_id;
		return cache_dir + cache_key.substr(0, 4) + path_sep + cache_key.substr(4, 12) +
		       ((type == CacheType::SMALL_RANGE) ? 's' : 'l') + oss.str() + cache_key.substr(16);
	}

	// cache_key(filename) = hex_hash64 ':' filename_suffix16 ':' protocol
	// goals: (i) 'unique' id, (ii) high character diversity at start of name, (iii) descriptive to aid debugging
	string GenCacheKey(const string &filename) const {
		const idx_t len = filename.length();
		const idx_t suffix = (len > FILENAME_SUFFIX_LEN) ? len - FILENAME_SUFFIX_LEN : 0;
		const idx_t slash = filename.find_last_of(path_sep); // will shorten the filename to suffix or last slash
		const idx_t protocol = filename.find("://");
		hash_t hash_value = Hash(string_t(filename.c_str(), static_cast<uint32_t>(len)));
		std::stringstream hex_stream;
		hex_stream << std::hex << std::uppercase << std::setfill('0') << std::setw(16) << hash_value;
		return hex_stream.str() + ":" +
		       filename.substr(std::max<idx_t>((slash != string::npos) * (slash + 1), suffix)) + ":" +
		       ((protocol != string::npos) ? StringUtil::Lower(filename.substr(0, protocol)) : "unknown");
	}

	bool CleanCacheDir(); // empty cache directory (remove all files and subdirs)
};

//===----------------------------------------------------------------------===//
// CacheMap - cache consists of Map(cache_key,CacheEntry) and a CacheEntry-LRU
//===----------------------------------------------------------------------===//
struct CacheMap {
	CacheConfig &config; // Reference to central configuration in BlobCache
	unique_ptr<unordered_map<string, unique_ptr<CacheEntry>>> key_cache; // KV store
	CacheEntry *lru_head = nullptr, *lru_tail = nullptr;                 // LRU
	idx_t current_size = 0, num_ranges = 0, current_entry_id = 0;

	explicit CacheMap(CacheConfig &cfg)
	    : config(cfg), key_cache(make_uniq<unordered_map<string, unique_ptr<CacheEntry>>>()) {
	}

	// Cache state management
	void Clear() {
		key_cache->clear();
		lru_head = nullptr;
		lru_tail = nullptr;
		current_size = 0;
		num_ranges = 0;
	}

	// Cache management operations
	CacheEntry *FindCacheEntry(const string &cache_key, const string &filename) {
		auto it = key_cache->find(cache_key);
		return (it != key_cache->end() && it->second->filename == filename) ? it->second.get() : nullptr;
	}
	CacheEntry *UpsertCacheEntry(const string &cache_key, const string &filename) {
		CacheEntry *cache_entry = nullptr;
		auto it = key_cache->find(cache_key);
		if (it == key_cache->end()) {
			auto new_entry = make_uniq<CacheEntry>();
			new_entry->filename = filename;
			new_entry->entry_id = ++current_entry_id; // unique id (filename must be unique each time we cache a file)
			config.LogDebug("Insert '" + cache_key + "' with entry_id " + std::to_string(new_entry->entry_id));
			cache_entry = new_entry.get();
			(*key_cache)[cache_key] = std::move(new_entry);
			AddToLRUFront(cache_entry);
		} else if (it->second->filename == filename) { // collision if not equal
			cache_entry = it->second.get();
			TouchLRU(cache_entry);
		}
		return cache_entry; // nullptr if there is a collision and we cannot insert
	}
	size_t EvictCacheEntry(const string &cache_key, CacheType cache_type);
	bool EvictToCapacity(idx_t required_space, CacheType cache_type, const string &exclude_filename = "");
	void PurgeCacheForPatternChange(const vector<std::regex> &regexps, optional_ptr<FileOpener> opener,
	                                CacheType cache_type);

	// LRU list management
	void TouchLRU(CacheEntry *entry);
	void RemoveFromLRU(CacheEntry *entry);
	void AddToLRUFront(CacheEntry *entry);

	// File operations (operate on cache_filepath which includes s/l prefix)
	void EnsureSubdirectoryExists(const string &cache_filepath);
	bool WriteToCacheFile(const string &cache_filepath, const void *buffer, idx_t length, idx_t &file_offset);
	bool ReadFromCacheFile(const string &cache_filepath, idx_t file_offset, void *buffer, idx_t length);
	bool DeleteCacheFile(const string &cache_filepath);

	vector<CacheRangeInfo> GetStatistics() const; // for blobcache_stats() table function
};

//===----------------------------------------------------------------------===//
// BlobCache - Main cache implementation
//===----------------------------------------------------------------------===//

struct BlobCache {
	static constexpr idx_t MAX_WRITER_THREADS = 256;

	CacheConfig config; // owns cache configuration settings

	// Cache maps for small and large ranges
	mutable std::mutex cache_mutex; // Protects both caches, LRU lists, sizes
	unique_ptr<CacheMap> small_cache, large_cache;

	// Cached regex patterns for file filtering (updated via callback)
	mutable std::mutex regex_mutex;    // Protects cached regex state
	vector<std::regex> cached_regexps; // Compiled regex patterns

	// Multi-threaded background cache writer system
	std::array<std::thread, MAX_WRITER_THREADS> cache_writer_threads;
	std::array<std::queue<pair<string, duckdb::shared_ptr<CacheFileBuffer>>>, MAX_WRITER_THREADS> write_job_queues;
	std::array<std::mutex, MAX_WRITER_THREADS> write_queue_mutexes;
	std::array<std::condition_variable, MAX_WRITER_THREADS> write_queue_cvs;
	std::atomic<bool> shutdown_writer_threads;
	idx_t num_writer_threads;

	// Helper methods for accessing cache and capacity by type
	CacheMap &GetCacheMap(CacheType type) {
		return type == CacheType::SMALL_RANGE ? *small_cache : *large_cache;
	}
	idx_t GetCacheCapacity(CacheType type) const {
		return (type == CacheType::LARGE_RANGE)
		           ? static_cast<idx_t>(config.total_cache_capacity * 0.9)    // Large cache gets 90% of total capacity
		           : config.total_cache_capacity - large_cache->current_size; // small cache gets the rest
	}

	friend class BlobFilesystemWrapper;
	friend struct CacheMap;

	// Constructor/Destructor
	explicit BlobCache(DatabaseInstance *db_instance = nullptr)
	    : small_cache(make_uniq<CacheMap>(config)), large_cache(make_uniq<CacheMap>(config)),
	      shutdown_writer_threads(false), num_writer_threads(1) {
		if (db_instance) {
			config.db_instance = db_instance->shared_from_this();
		}
	}
	~BlobCache() { // Set shutdown flag to prevent further logging attempts
		config.database_shutting_down = true;
		StopCacheWriterThreads();
	}

	// Thread management
	void CacheWriterThreadLoop(idx_t thread_id);
	void QueueCacheWrite(const string &filepath, idx_t partition, duckdb::shared_ptr<CacheFileBuffer> buffer);
	void StartCacheWriterThreads(idx_t thread_count);
	void StopCacheWriterThreads();

	// Core cache operations
	void InsertCache(const string &cache_key, const string &filename, idx_t start_pos, void *buf, idx_t len);
	// Combined cache lookup and read - returns bytes read from cache, adjusts nr_bytes if needed
	idx_t ReadFromCache(const string &cache_key, const string &filename, idx_t position, void *buf, idx_t &len);
	bool EvictToCapacity(idx_t extra_bytes = 0, const string &exclude_filename = "");

	void ConfigureCache(const string &directory, idx_t max_size_bytes = CacheConfig::DEFAULT_CACHE_CAPACITY,
	                    idx_t writer_threads = 1, idx_t small_threshold = CacheConfig::DEFAULT_SMALL_RANGE_THRESHOLD);

	// Configuration and caching policy
	bool ShouldCacheFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) const;
	void UpdateRegexPatterns(const string &regex_patterns_str);

	// helpers that delegate to CacheMap on both smaller and larger
	void ClearCache() {
		small_cache->Clear();
		large_cache->Clear();
	}
	void PurgeCacheForPatternChange(optional_ptr<FileOpener> opener = nullptr) {
		if (!config.cache_initialized)
			return;
		std::lock_guard<std::mutex> cache_lock(cache_mutex);
		small_cache->PurgeCacheForPatternChange(cached_regexps, opener, CacheType::SMALL_RANGE);
		large_cache->PurgeCacheForPatternChange(cached_regexps, opener, CacheType::LARGE_RANGE);
	}
	void EvictFile(const string &filename) { // Evict from both caches
		if (!config.cache_initialized)
			return;
		std::lock_guard<std::mutex> lock(cache_mutex);
		string cache_key = config.GenCacheKey(filename);
		small_cache->EvictCacheEntry(cache_key, CacheType::SMALL_RANGE);
		large_cache->EvictCacheEntry(cache_key, CacheType::LARGE_RANGE);
	}
};

} // namespace duckdb
