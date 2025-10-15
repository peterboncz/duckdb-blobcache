#pragma once

// Undefine Windows macros BEFORE any includes
#ifdef WIN32
#undef CreateDirectory
#undef MoveFile
#undef RemoveDirectory
#endif

#include "duckdb.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/external_file_cache.hpp"
#include <regex>
#include <iomanip>
#include <thread>

namespace duckdb {

// Forward declarations
struct BlobCache;

//===----------------------------------------------------------------------===//
// Cache data structures
//===----------------------------------------------------------------------===//

// Cache type for dual-cache system
enum class BlobCacheType : uint8_t {
	SMALL_RANGE = 0, // Small ranges (default <= 2047 bytes)
	LARGE_RANGE = 1  // Large ranges (> threshold)
};

// Statistics structure
struct BlobCacheRangeInfo {
	string protocol;
	string filename;
	idx_t blobcache_range_start; // Offset in cache file where this range starts
	idx_t range_start;           // Start position in remote file
	idx_t range_size;            // Size of range (end - start in remote file)
	idx_t usage_count;
	idx_t bytes_from_cache;
	idx_t bytes_from_mem;
};

// BlobCacheFileBuffer - shared buffer for background write
struct BlobCacheFileBuffer {
	BufferHandle buffer_handle;
	idx_t size;                                  // Size of data in buffer
	std::atomic<bool> *disk_write_completed_ptr; // Backpointer to BlobCacheFileRange::disk_write_completed
	string cache_key;                            // Cache key for eviction on write failure
	string filename;                             // Original filename for eviction on write failure

	explicit BlobCacheFileBuffer(BufferHandle handle, idx_t buffer_size, string key = "", string fname = "")
	    : buffer_handle(std::move(handle)), size(buffer_size), disk_write_completed_ptr(nullptr),
	      cache_key(std::move(key)), filename(std::move(fname)) {
	}

	// Unpin the buffer after write completes to allow buffer manager to evict if needed
	void Unpin() {
		buffer_handle.Destroy();
	}
};

//===----------------------------------------------------------------------===//
// BlobCacheFile caches a URL in used ranges as a ordered map of BlobCacheFileRange
//===----------------------------------------------------------------------===//
struct BlobCacheFileRange {
	idx_t range_start = 0, range_end = 0;                            // Range in remote blob file
	idx_t blobcache_range_start;                                     // Offset in cache file (pre-computed for memcache)
	std::atomic<bool> disk_write_completed;                          // True when background write to disk completes
	idx_t usage_count = 0, bytes_from_cache = 0, bytes_from_mem = 0; // Statistics
	duckdb::shared_ptr<BlobCacheFileBuffer> memcache_buffer; // In-memory buffer (references ExternalFileCache buffer)

	BlobCacheFileRange(idx_t start, idx_t end)
	    : range_start(start), range_end(end), blobcache_range_start(0), disk_write_completed(false) {
	}
};

struct BlobCacheFile {
	string filename;                                        // full URL
	map<idx_t, unique_ptr<BlobCacheFileRange>> ranges;      // Map of start position to unique BlobCacheFileRange
	idx_t cached_file_size = 0;                             // Total bytes cached for this file
	idx_t current_blobcache_file_offset = 0;                // Current offset for next append write
	idx_t file_id = 0;                                      // unique file-id ensures every filename is unique
	BlobCacheFile *lru_prev = nullptr, *lru_next = nullptr; // LRU linked list pointers
};

//===----------------------------------------------------------------------===//
// BlobCacheConfig - shared configuration for cache
//===----------------------------------------------------------------------===//
struct BlobCacheConfig {
	static constexpr idx_t DEFAULT_CACHE_CAPACITY = 1024ULL * 1024 * 1024; // 1GB default
	static constexpr idx_t DEFAULT_SMALL_RANGE_THRESHOLD = 2047;           // Default threshold for small ranges
	static constexpr idx_t FILENAME_SUFFIX_LEN = 15;

	shared_ptr<DatabaseInstance> db_instance;
	BlobCache *parent_cache = nullptr; // Reference to parent BlobCache for accessing blobfile_memcache
	bool blobcache_initialized = false;
	bool blobcache_shutting_down = false; // Flag to indicate database shutdown in progress
	string path_sep;                      // normally "/", but  "\" on windows
	string blobcache_dir;                 // where we store data temporarilu
	idx_t total_cache_capacity = DEFAULT_CACHE_CAPACITY;
	idx_t small_range_threshold = DEFAULT_SMALL_RANGE_THRESHOLD;
	std::bitset<4095> subdirs_created; // Subdirectory tracking (shared by both caches)

	// Get FileSystem reference from database instance
	FileSystem &GetFileSystem() const {
		return FileSystem::GetFileSystem(*db_instance);
	}

	// Logging methods
	void LogDebug(const string &message) const {
		if (db_instance && !blobcache_shutting_down) {
			DUCKDB_LOG_DEBUG(*db_instance, "[BlobCache] %s", message.c_str());
		}
	}
	void LogError(const string &message) const {
		if (db_instance && !blobcache_shutting_down) {
			DUCKDB_LOG_ERROR(*db_instance, "[BlobCache] %s", message.c_str());
		}
	}
	// If the directory does not exist, create it, otherwise empty it
	bool InitCacheDir() {
		if (!db_instance)
			return false;
		auto &fs = GetFileSystem();
		if (!fs.DirectoryExists(blobcache_dir)) {
			try {
				fs.CreateDirectory(blobcache_dir);
				return true;
			} catch (const std::exception &e) {
				LogError("Failed to create cache directory: " + string(e.what()));
				return false;
			}
		}
		return CleanCacheDir();
	}

	// inline to demonstrate path structure: /blobcache_dir/cache_key[0..3]/cache_key[4..15](s|l)file_id:cache_key[16..]
	string GenCacheFilePath(idx_t file_id, const string &cache_key, BlobCacheType type) const {
		std::ostringstream oss;
		oss << std::setw(19) << std::setfill('0') << file_id;
		return blobcache_dir + cache_key.substr(0, 4) + path_sep + cache_key.substr(4, 12) +
		       ((type == BlobCacheType::SMALL_RANGE) ? 's' : 'l') + oss.str() + cache_key.substr(16);
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
// BlobCacheMap - cache consists of Map(cache_key,BlobCacheFile) and a BlobCacheFile-LRU
//===----------------------------------------------------------------------===//
struct BlobCacheMap {
	BlobCacheConfig &config; // Reference to central configuration in BlobCache (includes parent_cache)
	unique_ptr<unordered_map<string, unique_ptr<BlobCacheFile>>> key_cache; // KV store
	BlobCacheFile *lru_head = nullptr, *lru_tail = nullptr;                 // LRU
	idx_t current_size = 0, num_ranges = 0, current_file_id = 0;

	explicit BlobCacheMap(BlobCacheConfig &cfg)
	    : config(cfg), key_cache(make_uniq<unordered_map<string, unique_ptr<BlobCacheFile>>>()) {
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
	BlobCacheFile *FindFile(const string &cache_key, const string &filename) {
		auto it = key_cache->find(cache_key);
		return (it != key_cache->end() && it->second->filename == filename) ? it->second.get() : nullptr;
	}
	BlobCacheFile *UpsertFile(const string &cache_key, const string &filename) {
		BlobCacheFile *cache_file = nullptr;
		auto it = key_cache->find(cache_key);
		if (it == key_cache->end()) {
			auto new_file = make_uniq<BlobCacheFile>();
			new_file->filename = filename;
			new_file->file_id = ++current_file_id; // unique id (filename must be unique each time we cache a file)
			config.LogDebug("Insert '" + cache_key + "' with file_id " + std::to_string(new_file->file_id));
			cache_file = new_file.get();
			(*key_cache)[cache_key] = std::move(new_file);
			AddToLRUFront(cache_file);
		} else if (it->second->filename == filename) { // collision if not equal
			cache_file = it->second.get();
			TouchLRU(cache_file);
		}
		return cache_file; // nullptr if there is a collision and we cannot insert
	}
	size_t EvictCacheKey(const string &cache_key, BlobCacheType cache_type);
	bool EvictToCapacity(idx_t required_space, BlobCacheType cache_type, const string &exclude_filename = "");
	void PurgeCacheForPatternChange(const vector<std::regex> &regexps, optional_ptr<FileOpener> opener,
	                                BlobCacheType cache_type);

	// LRU list management
	void TouchLRU(BlobCacheFile *cache_file);
	void RemoveFromLRU(BlobCacheFile *cache_file);
	void AddToLRUFront(BlobCacheFile *cache_file);

	// File operations (operate on cache_filepath which includes s/l prefix)
	void EnsureSubdirectoryExists(const string &cache_filepath);
	unique_ptr<FileHandle> TryOpenCacheFile(const string &cache_filepath);
	bool WriteToCacheFile(const string &blobcache_filepath, const void *buffer, idx_t length,
	                      idx_t &blobcache_range_start);
	bool ReadFromCacheFile(const string &blobcache_filepath, idx_t blobcache_range_start, void *buffer, idx_t &length,
	                       idx_t &out_bytes_from_mem); // read length may be reduced
	bool DeleteCacheFile(const string &cache_filepath);

	vector<BlobCacheRangeInfo> GetStatistics() const; // for blobcache_stats() table function
};

//===----------------------------------------------------------------------===//
// BlobCache - Main cache implementation
//===----------------------------------------------------------------------===//

struct BlobCache {
	static constexpr idx_t MAX_IO_THREADS = 256;

	BlobCacheConfig config; // owns cache configuration settings
	idx_t file_handle_id = 0;

	// Cache maps for small and large ranges
	mutable std::mutex blobcache_mutex; // Protects both caches, LRU lists, sizes
	unique_ptr<BlobCacheMap> smallrange_blobcache, largerange_blobcache;

	// Memory cache for disk-cached files (our own ExternalFileCache instance)
	unique_ptr<ExternalFileCache> blobfile_memcache;

	// Cached regex patterns for file filtering (updated via callback)
	mutable std::mutex regex_mutex;    // Protects cached regex state
	vector<std::regex> cached_regexps; // Compiled regex patterns

	// Multi-threaded background cache writer system
	std::array<std::thread, MAX_IO_THREADS> io_threads;
	std::array<std::queue<pair<string, duckdb::shared_ptr<BlobCacheFileBuffer>>>, MAX_IO_THREADS> io_queues;
	std::array<std::mutex, MAX_IO_THREADS> io_mutexes;
	std::array<std::condition_variable, MAX_IO_THREADS> io_cvs;
	std::atomic<bool> shutdown_io_threads;
	idx_t num_io_threads;

	// Helper methods for accessing cache and capacity by type
	BlobCacheMap &GetCacheMap(BlobCacheType type) {
		return type == BlobCacheType::SMALL_RANGE ? *smallrange_blobcache : *largerange_blobcache;
	}
	idx_t GetCacheCapacity(BlobCacheType type) const {
		return (type == BlobCacheType::LARGE_RANGE)
		           ? static_cast<idx_t>(config.total_cache_capacity * 0.9) // Large cache gets 90% of total capacity
		           : config.total_cache_capacity - largerange_blobcache->current_size; // small cache gets the rest
	}

	friend class BlobFilesystemWrapper;
	friend struct BlobCacheMap;

	// Constructor/Destructor
	explicit BlobCache(DatabaseInstance *db_instance = nullptr)
	    : smallrange_blobcache(make_uniq<BlobCacheMap>(config)), largerange_blobcache(make_uniq<BlobCacheMap>(config)),
	      shutdown_io_threads(false), num_io_threads(1) {
		if (db_instance) {
			config.db_instance = db_instance->shared_from_this();
		}
		config.parent_cache = this; // Set parent reference in config for BlobCacheMap to access
	}
	~BlobCache() { // Set shutdown flag to prevent further logging attempts
		config.blobcache_shutting_down = true;
		StopIOThreads();
	}

	// Thread management
	void MainIOThreadLoop(idx_t thread_id);
	void QueueIOWrite(const string &filepath, idx_t partition, duckdb::shared_ptr<BlobCacheFileBuffer> buffer);
	void StartIOThreads(idx_t thread_count);
	void StopIOThreads();

	// Memory cache helpers
	void InsertRangeIntoMemcache(const string &blobcache_filepath, idx_t blobcache_range_start,
	                             BufferHandle &buffer_handle, idx_t len);
	bool TryReadFromMemcache(const string &blobcache_filepath, idx_t blobcache_range_start, void *buffer, idx_t &len);
	bool AllocateInMemCache(BufferHandle &buffer_handle, idx_t length) {
		try {
			buffer_handle = blobfile_memcache->GetBufferManager().Allocate(MemoryTag::EXTERNAL_FILE_CACHE, length);
			return true;
		} catch (const std::exception &e) {
			config.LogError("AllocateInMemCache: failed for '" + to_string(length) + " bytes: " + string(e.what()));
			return false;
		}
	}

	// Core cache operations
	void InsertCache(const string &cache_key, const string &filename, idx_t start_pos, void *buf, idx_t len);
	// Combined cache lookup and read - returns bytes read from cache, adjusts nr_bytes if needed
	idx_t ReadFromCache(const string &cache_key, const string &filename, idx_t position, void *buf, idx_t &len);

	bool EvictToCapacity(idx_t extra_bytes = 0, const string &exclude_filename = "");

	void ConfigureCache(const string &directory, idx_t max_size_bytes = BlobCacheConfig::DEFAULT_CACHE_CAPACITY,
	                    idx_t writer_threads = 1,
	                    idx_t small_threshold = BlobCacheConfig::DEFAULT_SMALL_RANGE_THRESHOLD);

	// Configuration and caching policy
	bool ShouldCacheFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) const;
	void UpdateRegexPatterns(const string &regex_patterns_str);

	// helpers that delegate to BlobCacheMap on both smaller and larger
	void Clear() {
		smallrange_blobcache->Clear();
		largerange_blobcache->Clear();
	}
	void PurgeCacheForPatternChange(optional_ptr<FileOpener> opener = nullptr) {
		if (!config.blobcache_initialized)
			return;
		std::lock_guard<std::mutex> cache_lock(blobcache_mutex);
		smallrange_blobcache->PurgeCacheForPatternChange(cached_regexps, opener, BlobCacheType::SMALL_RANGE);
		largerange_blobcache->PurgeCacheForPatternChange(cached_regexps, opener, BlobCacheType::LARGE_RANGE);
	}
	void EvictFile(const string &filename) { // Evict from both caches
		if (!config.blobcache_initialized)
			return;
		std::lock_guard<std::mutex> lock(blobcache_mutex);
		string cache_key = config.GenCacheKey(filename);
		smallrange_blobcache->EvictCacheKey(cache_key, BlobCacheType::SMALL_RANGE);
		largerange_blobcache->EvictCacheKey(cache_key, BlobCacheType::LARGE_RANGE);
	}

	// Internal helpers
	void InsertRangeInternal(BlobCacheType cache_type, BlobCacheFile *cache_file, const string &cache_key,
	                         const string &filename, idx_t range_start, idx_t range_end, const void *buffer);
	idx_t ReadFromCacheInternal(BlobCacheType cache_type, const string &cache_key, const string &filename,
	                            idx_t position, void *buffer, idx_t &max_nr_bytes);
};

} // namespace duckdb
