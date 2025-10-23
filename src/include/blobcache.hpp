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

// Cache type for dual-cache system
enum class BlobCacheType : uint8_t {
	SMALL_RANGE = 0, // Small ranges (default <= 2047 bytes)
	LARGE_RANGE = 1  // Large ranges (> threshold)
};

// Statistics structure
struct BlobCacheRangeInfo {
	string protocol;        // e.g., s3
	string uri;             // Blob that we have cached a range of
	string file;            // Disk file where this range is stored in the cache
	idx_t file_range_start; // Offset in cache file where this range starts
	idx_t uri_range_start;  // Start position in blob of this range
	idx_t uri_range_size;   // Size of range (end - start in remote file)
	idx_t usage_count;      // how often it was read from the cache
	idx_t bytes_from_cache; // disk bytes read from CacheFile
	idx_t bytes_from_mem;   // memory bytes read from this cached range
};

// BlobCacheFileBuffer - shared buffer for background write
struct BlobCacheFileBuffer {
	BufferHandle handle;                         // DuckDB buffer manager handle
	string uri, key;                             // Cache uri and derived key of the blob that gets cached
	idx_t size;                                  // Size of data in buffer
	std::atomic<bool> *disk_write_completed_ptr; // Backpointer to BlobCacheFileRange::disk_write_completed

	explicit BlobCacheFileBuffer(BufferHandle handle, idx_t buffer_size, string key = "", string uri = "")
	    : handle(std::move(handle)), uri(std::move(uri)), key(std::move(key)), size(buffer_size) {
	}
	void Unpin() {
		handle.Destroy();
	}
};

// BlobCacheWriteJob - async write job for disk persistence
struct BlobCacheWriteJob {
	string file;                                 // Cache file path to write to
	duckdb::shared_ptr<BlobCacheFileBuffer> buf; // Buffer containing data to write

	BlobCacheWriteJob() {
	}
	BlobCacheWriteJob(string file, duckdb::shared_ptr<BlobCacheFileBuffer> buf)
	    : file(std::move(file)), buf(std::move(buf)) {
	}
};

// BlobCacheReadJob - async read job for prefetching
struct BlobCacheReadJob {
	string uri, key;   // Cache uri and derived key of the blob that gets cached
	idx_t range_start; // Start position in file
	idx_t range_size;  // Bytes to read

	BlobCacheReadJob() : range_start(0), range_size(0) {
	}
	BlobCacheReadJob(string fname, string key, idx_t start, idx_t size)
	    : uri(std::move(fname)), key(std::move(key)), range_start(start), range_size(size) {
	}
};

//===----------------------------------------------------------------------===//
// BlobCacheFile - represents a physical cache file on disk
//===----------------------------------------------------------------------===//
struct BlobCacheFile {
	string file;                       // Physical cache file path
	idx_t file_size = 0;               // Current size of this cache file
	idx_t file_id = 0;                 // Unique file ID
	std::atomic<bool> deleted;         // True when evicted (for lazy deletion)
	BlobCacheFile *lru_prev = nullptr; // LRU linked list pointers
	BlobCacheFile *lru_next = nullptr;

	BlobCacheFile(string file, idx_t id) : file(std::move(file)), file_id(id), deleted(false) {
	}
};

//===----------------------------------------------------------------------===//
// BlobCacheEntry caches a URL in used ranges as a ordered map of BlobCacheFileRange
//===----------------------------------------------------------------------===//
struct BlobCacheFileRange {
	idx_t uri_range_start = 0, uri_range_end = 0; // Range in remote blob file (uri)
	idx_t file_range_start;                       // Offset in cache file (pre-computed for memcache)
	std::atomic<bool> disk_write_completed;       // True when background write to disk completes
	idx_t usage_count = 0, bytes_from_cache = 0, bytes_from_mem = 0;
	duckdb::shared_ptr<BlobCacheFileBuffer> memcache_buffer; // In-memory buffer (references ExternalFileCache buffer)
	string file;                                             // Path to cache file (for lookup in file_cache)

	BlobCacheFileRange(idx_t start, idx_t end)
	    : uri_range_start(start), uri_range_end(end), file_range_start(0), disk_write_completed(false) {
	}
};

struct BlobCacheEntry {
	string uri;                                        // full URL of the blob
	idx_t total_cached_size = 0;                       // Total bytes cached for this blob
	map<idx_t, unique_ptr<BlobCacheFileRange>> ranges; // Map of start position to BlobCacheFileRanges
};

//===----------------------------------------------------------------------===//
// BlobCacheState - shared configuration for cache
//===----------------------------------------------------------------------===//
struct BlobCacheState {
	static constexpr idx_t SMALL_RANGE_THRESHOLD = 8192; // Default threshold for small ranges (16KB)
	static constexpr idx_t URI_SUFFIX_LEN = 15;

	shared_ptr<DatabaseInstance> db_instance;
	bool blobcache_initialized = false;
	bool blobcache_shutting_down = false; // Flag to indicate database shutdown in progress
	string path_sep;                      // normally "/", but  "\" on windows
	string blobcache_dir;                 // where we store data temporarilu
	idx_t total_cache_capacity = 0;

	// Memory cache for disk-cached files (our own ExternalFileCache instance)
	unique_ptr<ExternalFileCache> blobfile_memcache;

	// Directory creation tracking (65536 = 256*256 for XX/YY directories)
	mutable std::mutex subdir_mutex; // Protects subdir_created bitset
	std::bitset<65536>
	    subdir_created; // Track which XX/YY subdirectories exist
	                    // Directory management: ensures XX/ and XX/YY/ directories exist for a given key and type
	void EnsureDirectoryExists(const string &key, BlobCacheType type);

	// Memory cache helpers
	void InsertRangeIntoMemcache(const string &file_path, idx_t file_range_start, BufferHandle &handle, idx_t len);
	bool TryReadFromMemcache(const string &file_path, idx_t file_range_start, void *buffer, idx_t &len);
	bool AllocateInMemCache(BufferHandle &handle, idx_t length) {
		try {
			handle = blobfile_memcache->GetBufferManager().Allocate(MemoryTag::EXTERNAL_FILE_CACHE, length);
			return true;
		} catch (const std::exception &e) {
			LogError("AllocateInMemCache: failed for '" + to_string(length) + " bytes: " + string(e.what()));
			return false;
		}
	}

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
	bool InitCacheDir();  // If the directory does not exist, create it, otherwise empty it
	// Naming scheme:
	// Small files:  /blobcache_dir/XX/smallFILE_ID
	// Large files:  /blobcache_dir/XX/YY/key_suffix
	string GenCacheFilePath(idx_t file_id, const string &key, BlobCacheType type, idx_t range_start = 0) const {
		std::ostringstream oss;

		if (type == BlobCacheType::SMALL_RANGE) {
			// Small files: XX/smallFILE_ID
			// Use first 2 hex chars of key for XX subdirectory
			string xx = key.substr(0, 2);
			return blobcache_dir + xx + path_sep + "small" + to_string(file_id);
		} else {
			// Large range: XX/YY/key_suffix
			// Use first 4 hex chars of key for XX/YY subdirectories
			string xx = key.substr(0, 2);
			string yy = key.substr(2, 2);
			oss << range_start << "_" << file_id;
			return blobcache_dir + xx + path_sep + yy + path_sep + key.substr(4, 12) + oss.str() + key.substr(16);
		}
	}

	// key(uri) = hex_hash64 ':' uri_suffix15 ':' protocol
	// goals: (i) 'unique' id, (ii) high character diversity at start of name, (iii) descriptive to aid debugging
	string GenCacheKey(const string &uri) const {
		const idx_t len = uri.length();
		const idx_t suffix = (len > URI_SUFFIX_LEN) ? len - URI_SUFFIX_LEN : 0;
		const idx_t slash = uri.find_last_of(path_sep); // will shorten the uri to suffix or last slash
		const idx_t protocol = uri.find("://");
		hash_t hash_value = Hash(string_t(uri.c_str(), static_cast<uint32_t>(len)));
		std::stringstream hex_stream;
		hex_stream << std::hex << std::uppercase << std::setfill('0') << std::setw(16) << hash_value;
		return hex_stream.str() + ":" + uri.substr(std::max<idx_t>((slash != string::npos) * (slash + 1), suffix)) +
		       ":" + ((protocol != string::npos) ? StringUtil::Lower(uri.substr(0, protocol)) : "unknown");
	}
};

//===----------------------------------------------------------------------===//
// BlobCacheMap: Map(key,BlobCacheEntry), Map(file,BlobCacheFile) + BlobCacheFile-LRU
//===----------------------------------------------------------------------===//
struct BlobCacheMap {
	BlobCacheState &state;                                                   // Reference toshared state of BlobCache
	unique_ptr<unordered_map<string, unique_ptr<BlobCacheEntry>>> key_cache; // KV store: key -> BlobCacheEntry
	unique_ptr<unordered_map<string, unique_ptr<BlobCacheFile>>> file_cache; // KV store: file -> BlobCacheFile
	BlobCacheFile *lru_head = nullptr, *lru_tail = nullptr;                  // LRU for CacheFiles
	idx_t current_cache_size = 0, nr_ranges = 0, current_file_id = 10000000;

	explicit BlobCacheMap(BlobCacheState &state)
	    : state(state), key_cache(make_uniq<unordered_map<string, unique_ptr<BlobCacheEntry>>>()),
	      file_cache(make_uniq<unordered_map<string, unique_ptr<BlobCacheFile>>>()) {
	}

	// Cache state management
	void Clear() {
		key_cache->clear();
		file_cache->clear();
		lru_head = lru_tail = nullptr;
		current_cache_size = nr_ranges = 0;
	}

	BlobCacheEntry *FindFile(const string &key, const string &uri) {
		auto it = key_cache->find(key);
		return (it != key_cache->end() && it->second->uri == uri) ? it->second.get() : nullptr;
	}
	BlobCacheEntry *UpsertFile(const string &key, const string &uri) {
		BlobCacheEntry *cache_entry = nullptr;
		auto it = key_cache->find(key);
		if (it == key_cache->end()) {
			auto new_entry = make_uniq<BlobCacheEntry>();
			new_entry->uri = uri;
			state.LogDebug("Insert key '" + key + "'");
			cache_entry = new_entry.get();
			(*key_cache)[key] = std::move(new_entry);
		} else if (it->second->uri == uri) { // collision if not equal
			cache_entry = it->second.get();
		}
		return cache_entry; // nullptr if there is a collision and we cannot insert
	}
	void EvictFile(const string &uri) {
		string key = state.GenCacheKey(uri);
		auto it = key_cache->find(key);
		if (it != key_cache->end() && it->second->uri == uri) {
			key_cache->erase(it); // remove the orig uri from the map. LRU will eventually clean up the CacheFile
		}
	}

	bool EvictToCapacity(idx_t required_space);
	BlobCacheFile *GetOrCreateCacheFile(BlobCacheEntry *cache_entry, const string &key, BlobCacheType cache_type,
	                                    idx_t range_size);

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
		lru_head = cache_file; // Add file to front of LRU list (most recently used)
	}

	// File operations (operate on file which includes s/l prefix)
	unique_ptr<FileHandle> TryOpenCacheFile(const string &file_path);
	bool WriteToCacheFile(const string &file_path, const void *buffer, idx_t length, idx_t &file_range_start);
	bool ReadFromCacheFile(const string &file_path, idx_t file_range_start, void *buffer, idx_t &length,
	                       idx_t &out_bytes_from_mem); // read length may be reduced
	bool DeleteCacheFile(const string &file_path);

	vector<BlobCacheRangeInfo> GetStatistics() const; // for blobcache_stats() table function
};

//===----------------------------------------------------------------------===//
// BlobCache - Main cache implementation
//===----------------------------------------------------------------------===//

struct BlobCache {
	static constexpr idx_t MAX_IO_THREADS = 256;

	BlobCacheState state; // owns cache configuration settings

	// Cache maps for small and large ranges
	mutable std::mutex blobcache_mutex; // Protects both caches, LRU lists, sizes
	unique_ptr<BlobCacheMap> smallrange_blobcache, largerange_blobcache;

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
	idx_t nr_io_threads;

	// Helper methods for accessing cache and capacity by type
	BlobCacheMap &GetCacheMap(BlobCacheType type) {
		return type == BlobCacheType::SMALL_RANGE ? *smallrange_blobcache : *largerange_blobcache;
	}
	idx_t GetCacheCapacity(BlobCacheType type) const {
		return (type == BlobCacheType::LARGE_RANGE)
		           ? static_cast<idx_t>(state.total_cache_capacity * 0.9) // Large cache gets 90% of total capacity
		           : state.total_cache_capacity - largerange_blobcache->current_cache_size; // small cache gets rest
	}

	friend class BlobFilesystemWrapper;
	friend struct BlobCacheMap;

	// Constructor/Destructor
	explicit BlobCache(DatabaseInstance *db_instance = nullptr)
	    : smallrange_blobcache(make_uniq<BlobCacheMap>(state)), largerange_blobcache(make_uniq<BlobCacheMap>(state)),
	      shutdown_io_threads(false), read_job_counter(0), nr_io_threads(1) {
		if (db_instance) {
			state.db_instance = db_instance->shared_from_this();
		}
	}
	~BlobCache() { // Set shutdown flag to prevent further logging attempts
		state.blobcache_shutting_down = true;
		StopIOThreads();
	}

	// Thread management
	void MainIOThreadLoop(idx_t thread_id);
	void ProcessWriteJob(const BlobCacheWriteJob &job);
	void ProcessReadJob(const BlobCacheReadJob &job);
	void QueueIOWrite(const string &filepath, idx_t partition, duckdb::shared_ptr<BlobCacheFileBuffer> buffer);
	void QueueIORead(const string &uri, const string &key, idx_t range_start, idx_t range_size);
	void StartIOThreads(idx_t thread_count);
	void StopIOThreads();

	// Core cache operations
	void InsertCache(const string &key, const string &uri, idx_t pos, idx_t len, void *buf);
	// Combined cache lookup and read - returns bytes read from cache, adjusts nr_bytes if needed, updates
	// last_smallrange
	idx_t ReadFromCache(const string &key, const string &uri, idx_t pos, idx_t &len, void *buf);

	bool EvictToCapacity(BlobCacheType cache_type = BlobCacheType::LARGE_RANGE, idx_t new_range_size = 0);

	void ConfigureCache(const string &directory, idx_t max_size_bytes, idx_t writer_threads);

	// Configuration and caching policy
	bool ShouldCacheFile(const string &uri, optional_ptr<FileOpener> opener = nullptr) const;
	void UpdateRegexPatterns(const string &regex_patterns_str);

	// helpers that delegate to BlobCacheMap on both smaller and larger
	void EvictFile(const string &uri) { // Evict from both caches
		if (!state.blobcache_initialized) {
			return;
		}
		std::lock_guard<std::mutex> lock(blobcache_mutex);
		smallrange_blobcache->EvictFile(uri);
		largerange_blobcache->EvictFile(uri);
	}
};

} // namespace duckdb
