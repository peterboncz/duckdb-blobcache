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
	string protocol;             // e.g., s3
	string filename;             // original URL
	idx_t blobcache_range_start; // Offset in cache file where this range starts
	idx_t range_start;           // Start position in remote file
	idx_t range_size;            // Size of range (end - start in remote file)
	idx_t usage_count;           // how often it was read from the cache
	idx_t bytes_from_cache;      // disk bytes read from CacheFile
	idx_t bytes_from_mem;        // memory bytes read from this cached range
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
	void Unpin() {
		buffer_handle.Destroy();
	}
};

// BlobCacheWriteJob - async write job for disk persistence
struct BlobCacheWriteJob {
	string filepath;                                // Cache file path to write to
	duckdb::shared_ptr<BlobCacheFileBuffer> buffer; // Buffer containing data to write

	BlobCacheWriteJob() {
	}
	BlobCacheWriteJob(string path, duckdb::shared_ptr<BlobCacheFileBuffer> buf)
	    : filepath(std::move(path)), buffer(std::move(buf)) {
	}
};

// BlobCacheReadJob - async read job for prefetching
struct BlobCacheReadJob {
	string filename;   // File to read from
	string cache_key;  // Cache key for insertion
	idx_t range_start; // Start position in file
	idx_t range_size;  // Bytes to read

	BlobCacheReadJob() : range_start(0), range_size(0) {
	}
	BlobCacheReadJob(string fname, string key, idx_t start, idx_t size)
	    : filename(std::move(fname)), cache_key(std::move(key)), range_start(start), range_size(size) {
	}
};

//===----------------------------------------------------------------------===//
// BlobCacheFile - represents a physical cache file on disk
//===----------------------------------------------------------------------===//
struct BlobCacheFile {
	string filepath;                   // Physical cache file path
	idx_t current_size = 0;            // Current size of this cache file
	idx_t file_id = 0;                 // Unique file ID
	std::atomic<bool> deleted;         // True when evicted (for lazy deletion)
	BlobCacheFile *lru_prev = nullptr; // LRU linked list pointers
	BlobCacheFile *lru_next = nullptr;
	string cache_key; // Cache key of primary CacheEntry (for exclude_filename logic)

	BlobCacheFile(string path, idx_t id, string key = "")
	    : filepath(std::move(path)), file_id(id), deleted(false), cache_key(std::move(key)) {
	}
};

//===----------------------------------------------------------------------===//
// BlobCacheEntry caches a URL in used ranges as a ordered map of BlobCacheFileRange
//===----------------------------------------------------------------------===//
struct BlobCacheFileRange {
	idx_t smallrange_id;                    // ID. For largerange: the smallrange read that preceded it
	idx_t range_start = 0, range_end = 0;   // Range in remote blob file
	idx_t blobcache_range_start;            // Offset in cache file (pre-computed for memcache)
	std::atomic<bool> disk_write_completed; // True when background write to disk completes
	idx_t usage_count = 0, bytes_from_cache = 0, bytes_from_mem = 0;
	duckdb::shared_ptr<BlobCacheFileBuffer> memcache_buffer; // In-memory buffer (references ExternalFileCache buffer)
	string cache_filepath;                                   // Path to cache file (for lookup in filepath_cache)

	BlobCacheFileRange(idx_t start, idx_t end)
	    : range_start(start), range_end(end), blobcache_range_start(0), disk_write_completed(false) {
	}
};

struct BlobCacheEntry {
	string filename;                                   // full URL
	map<idx_t, unique_ptr<BlobCacheFileRange>> ranges; // Map of start position to unique BlobCacheFileRange
	idx_t cached_file_size = 0;                        // Total bytes cached for this file
};

//===----------------------------------------------------------------------===//
// BlobCacheConfig - shared configuration for cache
//===----------------------------------------------------------------------===//
struct BlobCacheConfig {
	static constexpr idx_t DEFAULT_CACHE_CAPACITY = 1024ULL * 1024 * 1024; // 1GB default
	static constexpr idx_t DEFAULT_SMALL_RANGE_THRESHOLD = 16384;          // Default threshold for small ranges (16KB)
	static constexpr idx_t FILENAME_SUFFIX_LEN = 15;

	shared_ptr<DatabaseInstance> db_instance;
	BlobCache *parent_cache = nullptr; // Reference to parent BlobCache for accessing blobfile_memcache
	bool blobcache_initialized = false;
	bool blobcache_shutting_down = false; // Flag to indicate database shutdown in progress
	string path_sep;                      // normally "/", but  "\" on windows
	string blobcache_dir;                 // where we store data temporarilu
	idx_t total_cache_capacity = DEFAULT_CACHE_CAPACITY;
	idx_t small_range_threshold = DEFAULT_SMALL_RANGE_THRESHOLD;
	idx_t small_range_current_id = 0;
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

	bool CleanCacheDir(); // empty cache directory (remove all files and subdirs)
	bool InitCacheDir() { // If the directory does not exist, create it, otherwise empty it
		if (!db_instance) {
			return false;
		}
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
	// Naming scheme:
	// Small files:  /blobcache_dir/{id%4095 in 4-char hex}/{id}
	// Large files:  /blobcache_dir/{cache_key[0..3]}/{cache_key[4..15]}{range_start}_{file_id}{cache_key[16..]}
	string GenCacheFilePath(idx_t file_id, const string &cache_key, BlobCacheType type, idx_t range_start = 0) const {
		std::ostringstream oss;

		if (type == BlobCacheType::SMALL_RANGE) {
			// Small files: hash on ID only, not filename
			// Subdir: file_id % 4095 in hex (4 chars)
			idx_t subdir_hash = file_id % 4095;
			oss << std::setw(4) << std::setfill('0') << std::hex << std::uppercase << subdir_hash;
			string subdir = oss.str();
			return blobcache_dir + subdir + path_sep + to_string(file_id);
		} else {
			// Large range: use cache_key-based naming
			oss << range_start << "_" << file_id;
			return blobcache_dir + cache_key.substr(0, 4) + path_sep + cache_key.substr(4, 12) + oss.str() +
			       cache_key.substr(16);
		}
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
};

//===----------------------------------------------------------------------===//
// BlobCacheMap - cache consists of Map(cache_key,BlobCacheEntry) and a BlobCacheEntry-LRU
//===----------------------------------------------------------------------===//
struct BlobCacheMap {
	BlobCacheConfig &config; // Reference to central configuration in BlobCache (includes parent_cache)
	unique_ptr<unordered_map<string, unique_ptr<BlobCacheEntry>>> key_cache;     // KV store: cache_key -> CacheEntry
	unique_ptr<unordered_map<string, unique_ptr<BlobCacheFile>>> filepath_cache; // KV store: filepath -> CacheFile
	BlobCacheFile *lru_head = nullptr, *lru_tail = nullptr;                      // LRU for CacheFiles
	idx_t current_size = 0, num_ranges = 0, current_file_id = 10000000;

	explicit BlobCacheMap(BlobCacheConfig &cfg)
	    : config(cfg), key_cache(make_uniq<unordered_map<string, unique_ptr<BlobCacheEntry>>>()),
	      filepath_cache(make_uniq<unordered_map<string, unique_ptr<BlobCacheFile>>>()) {
	}

	// Cache state management
	void Clear() {
		key_cache->clear();
		filepath_cache->clear();
		lru_head = lru_tail = nullptr;
		current_size = num_ranges = 0;
	}

	// Cache management operations
	BlobCacheEntry *FindFile(const string &cache_key, const string &filename) {
		auto it = key_cache->find(cache_key);
		return (it != key_cache->end() && it->second->filename == filename) ? it->second.get() : nullptr;
	}
	BlobCacheEntry *UpsertFile(const string &cache_key, const string &filename) {
		BlobCacheEntry *cache_entry = nullptr;
		auto it = key_cache->find(cache_key);
		if (it == key_cache->end()) {
			auto new_entry = make_uniq<BlobCacheEntry>();
			new_entry->filename = filename;
			config.LogDebug("Insert cache_key '" + cache_key + "'");
			cache_entry = new_entry.get();
			(*key_cache)[cache_key] = std::move(new_entry);
		} else if (it->second->filename == filename) { // collision if not equal
			cache_entry = it->second.get();
		}
		return cache_entry; // nullptr if there is a collision and we cannot insert
	}
	void EvictFile(const string &filename) {
		string cache_key = config.GenCacheKey(filename);
		auto it = key_cache->find(cache_key);
		if (it != key_cache->end() && it->second->filename == filename) {
			key_cache->erase(it); // remove the orig filename from the map. LRU will eventually clean up the CacheFile
		}
	}
	bool EvictToCapacity(idx_t required_space, const string &exclude_cache_key = "");
	BlobCacheFile *GetOrCreateCacheFile(BlobCacheEntry *cache_entry, const string &cache_key, BlobCacheType cache_type,
	                                    idx_t range_start, idx_t range_size);

	// LRU is based on a doubly-linked list. Note: blobcache mutex must be held!
	void TouchLRU(BlobCacheFile *cache_file) {
		if (cache_file != lru_head) {  // if not already at front
			RemoveFromLRU(cache_file); // Remove from current position
			AddToLRUFront(cache_file); // Add to front
		}
	}
	void RemoveFromLRU(BlobCacheFile *cache_file) {
		if (cache_file->lru_prev) {
			cache_file->lru_prev->lru_next = cache_file->lru_next;
		} else {
			lru_head = cache_file->lru_next;
		}
		if (cache_file->lru_next) {
			cache_file->lru_next->lru_prev = cache_file->lru_prev;
		} else {
			lru_tail = cache_file->lru_prev;
		}
		cache_file->lru_prev = cache_file->lru_next = nullptr;
	}
	void AddToLRUFront(BlobCacheFile *cache_file) {
		cache_file->lru_prev = nullptr;
		cache_file->lru_next = lru_head;
		if (lru_head) {
			lru_head->lru_prev = cache_file;
		}
		if (!lru_tail) {
			lru_tail = cache_file;
		}
		lru_head = cache_file; // Add cache_file to front of LRU list (most recently used)
	}

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

	// Multi-threaded background I/O system (writes + reads)
	std::array<std::thread, MAX_IO_THREADS> io_threads;
	std::array<std::queue<BlobCacheWriteJob>, MAX_IO_THREADS> write_queues;
	std::array<std::queue<BlobCacheReadJob>, MAX_IO_THREADS> read_queues;
	std::array<std::mutex, MAX_IO_THREADS> io_mutexes;
	std::array<std::condition_variable, MAX_IO_THREADS> io_cvs;
	std::atomic<bool> shutdown_io_threads;
	std::atomic<idx_t> read_job_counter; // For round-robin read job assignment
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
	      shutdown_io_threads(false), read_job_counter(0), num_io_threads(1) {
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
	void ProcessWriteJob(const BlobCacheWriteJob &job);
	void ProcessReadJob(const BlobCacheReadJob &job);
	void QueueIOWrite(const string &filepath, idx_t partition, duckdb::shared_ptr<BlobCacheFileBuffer> buffer);
	void QueueIORead(const string &filename, const string &cache_key, idx_t range_start, idx_t range_size);
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

	bool EvictToCapacity(BlobCacheType cache_type = BlobCacheType::LARGE_RANGE, idx_t new_range_size = 0,
	                     const string &exclude_cache_key = "");

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
	void EvictFile(const string &filename) { // Evict from both caches
		if (!config.blobcache_initialized) {
			return;
		}
		std::lock_guard<std::mutex> lock(blobcache_mutex);
		smallrange_blobcache->EvictFile(filename);
		largerange_blobcache->EvictFile(filename);
	}

	// Internal helpers
	void InsertRangeInternal(BlobCacheType cache_type, BlobCacheEntry *cache_entry, const string &cache_key,
	                         const string &filename, idx_t range_start, idx_t range_end, const void *buffer);
	idx_t ReadFromCacheInternal(BlobCacheType cache_type, const string &cache_key, const string &filename,
	                            idx_t position, void *buffer, idx_t &max_nr_bytes);
};

} // namespace duckdb
