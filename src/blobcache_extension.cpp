#define DUCKDB_EXTENSION_MAIN

#include "blobcache_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/storage/object_cache.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// BlobCacheEntry - ObjectCache wrapper for BlobCache
//===----------------------------------------------------------------------===//
class BlobCacheEntry : public ObjectCacheEntry {
public:
	shared_ptr<BlobCache> cache;
	
	explicit BlobCacheEntry(shared_ptr<BlobCache> cache_p) : cache(std::move(cache_p)) {}
	
	string GetObjectType() override {
		return "BlobCache";
	}
	
	static string ObjectType() {
		return "BlobCache";
	}
};

//===----------------------------------------------------------------------===//
// Cache Management using DatabaseInstance ObjectCache
//===----------------------------------------------------------------------===//
static shared_ptr<BlobCache> GetOrCreateBlobCache(DatabaseInstance &instance) {
	auto &object_cache = instance.GetObjectCache();
	
	// Try to get existing cache
	auto cached_entry = object_cache.Get<BlobCacheEntry>("blobcache_instance");
	if (cached_entry) {
		DUCKDB_LOG_DEBUG(instance, "[BlobCache] Retrieved existing BlobCache from ObjectCache");
		return cached_entry->cache;
	}
	
	// Create new cache and store in ObjectCache
	auto new_cache = make_shared_ptr<BlobCache>(&instance);
	auto cache_entry = make_shared_ptr<BlobCacheEntry>(new_cache);
	object_cache.Put("blobcache_instance", cache_entry);
	DUCKDB_LOG_DEBUG(instance, "[BlobCache] Created and stored new BlobCache in ObjectCache");
	
	return new_cache;
}

static void WrapExistingFilesystems(DatabaseInstance &instance) {
	auto &db_fs = FileSystem::GetFileSystem(instance);
	DUCKDB_LOG_DEBUG(instance, "[BlobCache] Filesystem type: %s", db_fs.GetName().c_str());

	// Get VirtualFileSystem
	auto vfs = dynamic_cast<VirtualFileSystem*>(&db_fs);
	if (!vfs) {
		auto *opener_fs = dynamic_cast<OpenerFileSystem*>(&db_fs);
		if (opener_fs) {
			DUCKDB_LOG_DEBUG(instance, "[BlobCache] Found OpenerFileSystem, getting wrapped VFS");
			vfs = dynamic_cast<VirtualFileSystem*>(&opener_fs->GetFileSystem());
		}
	}
	if (!vfs) {
		DUCKDB_LOG_DEBUG(instance, "[BlobCache] Cannot find VirtualFileSystem - skipping filesystem wrapping");
		return;
	}

	// Get the shared cache instance - only proceed if cache is initialized
	auto shared_cache = GetOrCreateBlobCache(instance);
	if (!shared_cache->IsCacheInitialized()) {
		DUCKDB_LOG_DEBUG(instance, "[BlobCache] Cache not initialized yet, skipping filesystem wrapping");
		return;
	}
	// Try to wrap each blob storage filesystem
	auto subsystems = vfs->ListSubSystems();
	DUCKDB_LOG_DEBUG(instance, "[BlobCache] Found %zu registered subsystems", subsystems.size());
	
	for (const auto &name : subsystems) {
		DUCKDB_LOG_DEBUG(instance, "[BlobCache] Processing subsystem: '%s'", name.c_str());
		
		// Skip if already wrapped (starts with "BlobCache:")
		if (name.find("BlobCache:") == 0) {
			DUCKDB_LOG_DEBUG(instance, "[BlobCache] Skipping already wrapped subsystem: '%s'", name.c_str());
			continue;
		}
		
		auto extracted_fs = vfs->ExtractSubSystem(name);
		if (extracted_fs) {
			DUCKDB_LOG_DEBUG(instance, "[BlobCache] Successfully extracted subsystem: '%s' (GetName returns: '%s')", 
			                  name.c_str(), extracted_fs->GetName().c_str());
			auto wrapped_fs = make_uniq<BlobFilesystemWrapper>(std::move(extracted_fs), &instance, shared_cache);
			DUCKDB_LOG_DEBUG(instance, "[BlobCache] Created wrapper with name: '%s'", wrapped_fs->GetName().c_str());
			vfs->RegisterSubSystem(std::move(wrapped_fs));
			DUCKDB_LOG_DEBUG(instance, "[BlobCache] Successfully registered wrapped subsystem for '%s'", name.c_str());
		} else {
			DUCKDB_LOG_ERROR(instance, "[BlobCache] Failed to extract '%s' - subsystem not wrapped", name.c_str());
		}
	}
	
	// Log final subsystem list
	auto final_subsystems = vfs->ListSubSystems();
	DUCKDB_LOG_DEBUG(instance, "[BlobCache] After wrapping, have %zu subsystems", final_subsystems.size());
	for (const auto &name : final_subsystems) {
		DUCKDB_LOG_DEBUG(instance, "[BlobCache] - %s", name.c_str());
	}
}

//===----------------------------------------------------------------------===//
// Table Functions
//===----------------------------------------------------------------------===//

// Bind data for blobcache_init function
struct BlobCacheInitBindData : public FunctionData {
	string directory;
	idx_t max_size_mb;
	idx_t writer_threads;
	
	BlobCacheInitBindData(string dir, idx_t size, idx_t threads) : directory(std::move(dir)), max_size_mb(size), writer_threads(threads) {}
	
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<BlobCacheInitBindData>(directory, max_size_mb, writer_threads);
	}
	
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<BlobCacheInitBindData>();
		return directory == other.directory && max_size_mb == other.max_size_mb && writer_threads == other.writer_threads;
	}
};

// Bind data for blobcache_numthreads function
struct BlobCacheNumThreadsBindData : public FunctionData {
	idx_t new_thread_count;
	
	BlobCacheNumThreadsBindData(idx_t threads) : new_thread_count(threads) {}
	
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<BlobCacheNumThreadsBindData>(new_thread_count);
	}
	
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<BlobCacheNumThreadsBindData>();
		return new_thread_count == other.new_thread_count;
	}
};

// Global state for both table functions
struct BlobCacheGlobalState : public GlobalTableFunctionState {
	idx_t tuples_processed = 0;
	vector<vector<Value>> cached_rows; // Store all rows to output in chunks (used by stats)
	vector<BlobCache::RangeInfo> cached_range_infos; // More efficient storage for range info
	bool data_loaded = false; // For stats function
	
	idx_t MaxThreads() const override {
		return 1; // Single threaded for simplicity
	}
};


// Bind function for blobcache_init
static unique_ptr<FunctionData> BlobCacheInitBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	// Setup return schema - returns a single boolean indicating success/failure
	return_types.push_back(LogicalType::BOOLEAN);
	names.push_back("success");
	
	string directory = ".blobcache"; // Default
	idx_t max_size_mb = 256; // Default 256MB
	idx_t writer_threads = 1; // Default 1 thread
	
	// Parse arguments if provided
	if (input.inputs.size() >= 1) {
		if (input.inputs[0].IsNull()) {
			throw BinderException("blobcache_init: directory cannot be NULL");
		}
		directory = StringValue::Get(input.inputs[0]);
	}
	
	if (input.inputs.size() >= 2) {
		if (input.inputs[1].IsNull()) {
			throw BinderException("blobcache_init: max_size_mb cannot be NULL");
		}
		auto size_val = input.inputs[1];
		if (size_val.type().id() != LogicalTypeId::BIGINT && size_val.type().id() != LogicalTypeId::INTEGER) {
			throw BinderException("blobcache_init: max_size_mb must be an integer");
		}
		max_size_mb = size_val.GetValue<idx_t>();
		if (max_size_mb <= 0) {
			throw BinderException("blobcache_init: max_size_mb must be positive");
		}
	}
	
	if (input.inputs.size() >= 3) {
		if (input.inputs[2].IsNull()) {
			throw BinderException("blobcache_init: writer_threads cannot be NULL");
		}
		auto threads_val = input.inputs[2];
		if (threads_val.type().id() != LogicalTypeId::BIGINT && threads_val.type().id() != LogicalTypeId::INTEGER) {
			throw BinderException("blobcache_init: writer_threads must be an integer");
		}
		writer_threads = threads_val.GetValue<idx_t>();
		if (writer_threads <= 0) {
			throw BinderException("blobcache_init: writer_threads must be positive");
		}
		if (writer_threads > 256) {
			throw BinderException("blobcache_init: writer_threads cannot exceed 256");
		}
	}
	return make_uniq<BlobCacheInitBindData>(std::move(directory), max_size_mb, writer_threads);
}

// Init function for blobcache_init global state
static unique_ptr<GlobalTableFunctionState> BlobCacheInitInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<BlobCacheGlobalState>();
}

// Init function for blobcache_stats global state (same as init)
static unique_ptr<GlobalTableFunctionState> BlobCacheStatsInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<BlobCacheGlobalState>();
}

// Init function for blobcache_numthreads global state (same as init)
static unique_ptr<GlobalTableFunctionState> BlobCacheNumThreadsInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<BlobCacheGlobalState>();
}

// Bind function for blobcache_stats
static unique_ptr<FunctionData> BlobCacheStatsBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	// Setup return schema - returns cache statistics with 6 columns
	return_types.push_back(LogicalType::VARCHAR);   // protocol
	return_types.push_back(LogicalType::VARCHAR);   // filename
	return_types.push_back(LogicalType::BIGINT);    // start
	return_types.push_back(LogicalType::BIGINT);    // end
	return_types.push_back(LogicalType::BIGINT);    // usage_count
	return_types.push_back(LogicalType::BIGINT);    // bytes_from_cache
	
	names.push_back("protocol");
	names.push_back("filename");
	names.push_back("start");
	names.push_back("end");
	names.push_back("usage_count");
	names.push_back("bytes_from_cache");
	
	return nullptr; // No bind data needed for stats function
}

// blobcache_init(directory, max_size_mb) - Initialize the blob cache
static void BlobCacheInitFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &global_state = data_p.global_state->Cast<BlobCacheGlobalState>();
	
	// Return nothing if we've already processed our single tuple
	if (global_state.tuples_processed >= 1) {
		output.SetCardinality(0);
		return;
	}
	
	DUCKDB_LOG_DEBUG(*context.db, "[BlobCache] blobcache_init called");
	
	// Process the single initialization tuple
	auto shared_cache = GetOrCreateBlobCache(*context.db);
	bool success = false;
	if (data_p.bind_data && shared_cache) {
		auto &bind_data = data_p.bind_data->Cast<BlobCacheInitBindData>();
		DUCKDB_LOG_DEBUG(*context.db, "[BlobCache] Initializing cache: directory='%s', max_size=%zu MB, writer_threads=%zu",
						  bind_data.directory.c_str(), bind_data.max_size_mb, bind_data.writer_threads);

		shared_cache->InitializeCache(bind_data.directory, bind_data.max_size_mb * 1024 * 1024, bind_data.writer_threads);
		success = true;
		// Now that cache is initialized, wrap any existing filesystems
		WrapExistingFilesystems(*context.db);
	}
	output.SetCardinality(1);
	output.data[0].SetValue(0, Value::BOOLEAN(success));
	global_state.tuples_processed = 1;
}


// blobcache_stats() - Return cache statistics in LRU order with chunking
static void BlobCacheStatsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &global_state = data.global_state->Cast<BlobCacheGlobalState>();
	
	// Load data on first call using the efficient RangeInfo vector
	if (!global_state.data_loaded) {
		auto shared_cache = GetOrCreateBlobCache(*context.db);
		if (shared_cache && shared_cache->IsCacheInitialized()) {
			// Populate the cached_range_infos vector directly under mutex in PopulateCacheStatistics
			shared_cache->PopulateCacheStatistics(global_state.cached_range_infos, 10000);
		}
		global_state.data_loaded = true;
	}
	
	// Calculate how many rows to output in this chunk
	idx_t total_rows = global_state.cached_range_infos.size();
	idx_t start_idx = global_state.tuples_processed;
	
	// If we've already output all rows, return empty
	if (start_idx >= total_rows) {
		output.SetCardinality(0);
		return;
	}
	
	// Determine chunk size (up to STANDARD_VECTOR_SIZE which is 2048)
	idx_t chunk_size = std::min(static_cast<idx_t>(STANDARD_VECTOR_SIZE), total_rows - start_idx);
	output.SetCardinality(chunk_size);
	
	// Fill the output chunk directly from cached_range_infos
	for (idx_t i = 0; i < chunk_size; i++) {
		idx_t row_idx = start_idx + i;
		const auto &info = global_state.cached_range_infos[row_idx];
		
		output.data[0].SetValue(i, Value(info.protocol));
		output.data[1].SetValue(i, Value(info.filename));
		output.data[2].SetValue(i, Value::BIGINT(static_cast<int64_t>(info.start)));
		output.data[3].SetValue(i, Value::BIGINT(static_cast<int64_t>(info.end)));
		output.data[4].SetValue(i, Value::BIGINT(static_cast<int64_t>(info.usage_count)));
		output.data[5].SetValue(i, Value::BIGINT(static_cast<int64_t>(info.bytes_from_cache)));
	}
	
	// Update the processed count
	global_state.tuples_processed += chunk_size;
}

//===----------------------------------------------------------------------===//
// blobcache_numthreads table function
//===----------------------------------------------------------------------===//

// Bind function for blobcache_numthreads
static unique_ptr<FunctionData> BlobCacheNumThreadsBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<string> &names) {
	// Setup return schema - returns a single boolean indicating success/failure
	return_types.push_back(LogicalType::BOOLEAN);
	names.push_back("success");
	
	if (input.inputs.size() != 1) {
		throw BinderException("blobcache_numthreads: requires exactly one parameter (new_thread_count)");
	}
	
	if (input.inputs[0].IsNull()) {
		throw BinderException("blobcache_numthreads: new_thread_count cannot be NULL");
	}
	
	auto threads_val = input.inputs[0];
	if (threads_val.type().id() != LogicalTypeId::BIGINT && threads_val.type().id() != LogicalTypeId::INTEGER) {
		throw BinderException("blobcache_numthreads: new_thread_count must be an integer");
	}
	
	idx_t new_thread_count = threads_val.GetValue<idx_t>();
	if (new_thread_count > 256) {
		throw BinderException("blobcache_numthreads: new_thread_count cannot exceed 256");
	}
	
	return make_uniq<BlobCacheNumThreadsBindData>(new_thread_count);
}

// blobcache_numthreads(new_thread_count) - Change the number of writer threads at runtime
static void BlobCacheNumThreadsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &global_state = data_p.global_state->Cast<BlobCacheGlobalState>();
	
	// Return nothing if we've already processed our single tuple
	if (global_state.tuples_processed >= 1) {
		output.SetCardinality(0);
		return;
	}
	
	DUCKDB_LOG_DEBUG(*context.db, "[BlobCache] blobcache_numthreads called");
	
	// Process the single thread change tuple
	auto shared_cache = GetOrCreateBlobCache(*context.db);
	bool success = false;
	
	if (data_p.bind_data && shared_cache) {
		auto &bind_data = data_p.bind_data->Cast<BlobCacheNumThreadsBindData>();
		if (!shared_cache->IsCacheInitialized()) {
			DUCKDB_LOG_ERROR(*context.db, "[BlobCache] Cache not initialized - call blobcache_init first");
			success = false;
		} else {
			DUCKDB_LOG_DEBUG(*context.db, "[BlobCache] Changing writer threads to %zu", bind_data.new_thread_count);
			shared_cache->ChangeWriterThreadCount(bind_data.new_thread_count);
			success = true;
			DUCKDB_LOG_DEBUG(*context.db, "[BlobCache] Successfully changed writer thread count");
		}
	}
	
	// Return the result
	output.SetCardinality(1);
	output.data[0].SetValue(0, Value::BOOLEAN(success));
	global_state.tuples_processed = 1;
}

//===----------------------------------------------------------------------===//
// BlobCacheExtensionCallback - Automatic wrapping when target extensions load
//===----------------------------------------------------------------------===//
class BlobCacheExtensionCallback : public ExtensionCallback {
public:
	void OnExtensionLoaded(DatabaseInstance &db, const string &name) override {
		auto extension_name = StringUtil::Lower(name);
		if (extension_name == "httpfs" || extension_name == "azure") {
			DUCKDB_LOG_DEBUG(db, "[BlobCache] Target extension '%s' loaded, automatically wrapping filesystems", name.c_str());
			WrapExistingFilesystems(db);
		}
	}
};

void BlobcacheExtension::Load(DuckDB &db) {
	auto &instance = *db.instance;
	DUCKDB_LOG_DEBUG(instance, "[BlobCache] BlobCache extension loaded!");
	
	// Register table functions
	DUCKDB_LOG_DEBUG(instance, "[BlobCache] Registering table functions...");
	
	// Register blobcache_init table function (supports 0, 1, or 2 arguments)
	TableFunction blobcache_init_function("blobcache_init", {}, BlobCacheInitFunction);
	blobcache_init_function.bind = BlobCacheInitBind;
	blobcache_init_function.init_global = BlobCacheInitInitGlobal;
	blobcache_init_function.varargs = LogicalType::ANY;  // Allow variable arguments
	ExtensionUtil::RegisterFunction(instance, blobcache_init_function);
	DUCKDB_LOG_DEBUG(instance, "[BlobCache] Registered blobcache_init function");
	
	// Register blobcache_stats table function
	TableFunction blobcache_stats_function("blobcache_stats", {}, BlobCacheStatsFunction);
	blobcache_stats_function.bind = BlobCacheStatsBind;
	blobcache_stats_function.init_global = BlobCacheStatsInitGlobal;
	ExtensionUtil::RegisterFunction(instance, blobcache_stats_function);
	DUCKDB_LOG_DEBUG(instance, "[BlobCache] Registered blobcache_stats function");
	
	// Register blobcache_numthreads table function
	TableFunction blobcache_numthreads_function("blobcache_numthreads", {LogicalType::BIGINT}, BlobCacheNumThreadsFunction);
	blobcache_numthreads_function.bind = BlobCacheNumThreadsBind;
	blobcache_numthreads_function.init_global = BlobCacheNumThreadsInitGlobal;
	ExtensionUtil::RegisterFunction(instance, blobcache_numthreads_function);
	DUCKDB_LOG_DEBUG(instance, "[BlobCache] Registered blobcache_numthreads function");

	// Register extension callback for automatic wrapping
	auto &config = DBConfig::GetConfig(instance);
	config.extension_callbacks.push_back(make_uniq<BlobCacheExtensionCallback>());

	// Wrap any existing filesystems (in case some were already loaded)
	WrapExistingFilesystems(instance);
	DUCKDB_LOG_DEBUG(instance, "[BlobCache] Extension initialization complete!");
}


} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void blobcache_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::BlobcacheExtension>();
}

DUCKDB_EXTENSION_API const char *blobcache_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif