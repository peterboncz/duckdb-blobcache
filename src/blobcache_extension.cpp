#define DUCKDB_EXTENSION_MAIN

#include "blobcache_extension.hpp"
#include "blobfs_wrapper.hpp"
#include "duckdb/storage/object_cache.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// Table Functions
//===----------------------------------------------------------------------===//

// Bind data for blobcache_config function
struct BlobCacheConfigBindData : public FunctionData {
	string directory;
	idx_t max_size_mb;
	string regex_patterns; // New regex patterns parameter
	bool query_only; // True if no parameters provided - just query current values
	
	BlobCacheConfigBindData(string dir, idx_t size, string regexps = "", bool query = false)
		: directory(std::move(dir)), max_size_mb(size),
		  regex_patterns(std::move(regexps)), query_only(query) {}
	
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<BlobCacheConfigBindData>(directory, max_size_mb, regex_patterns, query_only);
	}
	
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<BlobCacheConfigBindData>();
		return directory == other.directory && max_size_mb == other.max_size_mb && 
		       regex_patterns == other.regex_patterns && query_only == other.query_only;
	}
};


// Global state for both table functions
struct BlobCacheGlobalState : public GlobalTableFunctionState {
	idx_t tuples_processed = 0;
	vector<BlobCache::RangeInfo> range_infos; // Cache statistics data
	
	idx_t MaxThreads() const override {
		return 1; // Single threaded for simplicity
	}
};


// Bind function for blobcache_config
static unique_ptr<FunctionData> BlobCacheConfigBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	// Setup return schema - returns useful cache statistics
	return_types.push_back(LogicalType::VARCHAR);   // cache_path
	return_types.push_back(LogicalType::BIGINT);    // max_size_bytes
	return_types.push_back(LogicalType::BIGINT);    // current_cache_size_bytes
	return_types.push_back(LogicalType::BOOLEAN);   // success

	names.push_back("cache_path");
	names.push_back("max_size_bytes");
	names.push_back("current_cache_size_bytes");
	names.push_back("success");
	
	string directory = ".blobcache"; // Default
	idx_t max_size_mb = 256; // Default 256MB
	idx_t writer_threads = 1; // Default 1 thread
	string regex_patterns = ""; // Default empty (conservative mode)
	bool query_only = false; // Default to configuration mode
	
	// Check if this is a query-only call (no parameters)
	if (input.inputs.size() == 0) {
		query_only = true;
	}
	
	// Parse arguments if provided
	if (input.inputs.size() >= 1) {
		if (input.inputs[0].IsNull()) {
			throw BinderException("blobcache_config: directory cannot be NULL");
		}
		directory = StringValue::Get(input.inputs[0]);
	}
	
	if (input.inputs.size() >= 2) {
		if (input.inputs[1].IsNull()) {
			throw BinderException("blobcache_config: max_size_mb cannot be NULL");
		}
		auto size_val = input.inputs[1];
		if (size_val.type().id() != LogicalTypeId::BIGINT && size_val.type().id() != LogicalTypeId::INTEGER) {
			throw BinderException("blobcache_config: max_size_mb must be an integer");
		}
		max_size_mb = size_val.GetValue<idx_t>();
		if (max_size_mb <= 0) {
			throw BinderException("blobcache_config: max_size_mb must be positive");
		}
	}
	
	if (input.inputs.size() >= 3) {
		if (input.inputs[2].IsNull()) {
			throw BinderException("blobcache_config: regex_patterns cannot be NULL");
		}
		auto patterns_val = input.inputs[2];
		if (patterns_val.type().id() != LogicalTypeId::VARCHAR) {
			throw BinderException("blobcache_config: regex_patterns must be a string");
		}
		regex_patterns = StringValue::Get(patterns_val);
	}
	
	return make_uniq<BlobCacheConfigBindData>(std::move(directory), max_size_mb, std::move(regex_patterns), query_only);
}

// Init function for blobcache_config global state
static unique_ptr<GlobalTableFunctionState> BlobCacheConfigInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<BlobCacheGlobalState>();
}

// Init function for blobcache_stats global state (same as config)
static unique_ptr<GlobalTableFunctionState> BlobCacheStatsInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<BlobCacheGlobalState>();
}

// Bind function for blobcache_stats
static unique_ptr<FunctionData> BlobCacheStatsBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	// Setup return schema - returns cache statistics with 7 columns
	return_types.push_back(LogicalType::VARCHAR);   // protocol
	return_types.push_back(LogicalType::VARCHAR);   // filename
	return_types.push_back(LogicalType::BIGINT);    // file_offset
	return_types.push_back(LogicalType::BIGINT);    // start
	return_types.push_back(LogicalType::BIGINT);    // end
	return_types.push_back(LogicalType::BIGINT);    // usage_count
	return_types.push_back(LogicalType::BIGINT);    // bytes_from_cache

	names.push_back("protocol");
	names.push_back("filename");
	names.push_back("file_offset");
	names.push_back("start");
	names.push_back("end");
	names.push_back("usage_count");
	names.push_back("bytes_from_cache");

	return nullptr; // No bind data needed for stats function
}

// blobcache_config(directory, max_size_mb, writer_threads) - Configure the blob cache
static void BlobCacheConfigFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &global_state = data_p.global_state->Cast<BlobCacheGlobalState>();
	
	// Return nothing if we've already processed our single tuple
	if (global_state.tuples_processed >= 1) {
		output.SetCardinality(0);
		return;
	}
	
	DUCKDB_LOG_DEBUG(*context.db, "[BlobCache] blobcache_config called");
	
	// Process the single configuration tuple
	auto shared_cache = GetOrCreateBlobCache(*context.db);
	bool success = false;
	string cache_path = "";
	idx_t max_size_bytes = 0;
	idx_t current_cache_size_bytes = 0;
	idx_t writer_threads = 0;
	
	if (data_p.bind_data && shared_cache) {
		auto &bind_data = data_p.bind_data->Cast<BlobCacheConfigBindData>();
		
		if (bind_data.query_only) {
			// Query-only mode - just return current values without changing anything
			DUCKDB_LOG_DEBUG(*context.db, "[BlobCache] Querying cache configuration");
			success = true; // Query always succeeds
		} else {
			// Configuration mode - actually configure the cache
			DUCKDB_LOG_DEBUG(*context.db, "[BlobCache] Configuring cache: directory='%s', max_size=%zu MB, writer_threads=%zu, regex_patterns='%s'",
							  bind_data.directory.c_str(), bind_data.max_size_mb, bind_data.regex_patterns.c_str());

			// Configure cache first
			shared_cache->ConfigureCache(bind_data.directory, bind_data.max_size_mb * 1024 * 1024);
			
			// Update regex patterns and purge non-qualifying cache entries
			shared_cache->UpdateRegexPatterns(bind_data.regex_patterns);
			shared_cache->PurgeCacheForPatternChange(nullptr);
			success = true;
			// Now that cache is configured, wrap any existing filesystems
			WrapExistingFilesystems(*context.db);
		}
	}
	
	// Get current cache statistics (works whether configuration succeeded or not)
	if (shared_cache && shared_cache->IsCacheInitialized()) {
		cache_path = shared_cache->GetCachePath();
		max_size_bytes = shared_cache->GetMaxSizeBytes();
		current_cache_size_bytes = shared_cache->GetCurrentCacheSize();
	}
	
	// Return the statistics tuple
	output.SetCardinality(1);
	output.data[0].SetValue(0, Value(cache_path));                       // cache_path
	output.data[1].SetValue(0, Value::BIGINT(max_size_bytes));           // max_size_bytes
	output.data[2].SetValue(0, Value::BIGINT(current_cache_size_bytes));   // current_cache_size_bytes
	output.data[3].SetValue(0, Value::BIGINT(writer_threads));           // writer_threads
	output.data[4].SetValue(0, Value::BOOLEAN(success));                 // success
	global_state.tuples_processed = 1;
}


// blobcache_stats() - Return cache statistics in LRU order with chunking
static void BlobCacheStatsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &global_state = data.global_state->Cast<BlobCacheGlobalState>();
	
	// Load data on first call (when tuples_processed == 0)
	if (global_state.tuples_processed == 0) {
		global_state.range_infos = GetOrCreateBlobCache(*context.db)->GetCacheStatistics();
	}
	
	// Calculate how many rows to output in this chunk
	idx_t chunk_size = static_cast<idx_t>(STANDARD_VECTOR_SIZE);
	if (global_state.tuples_processed + chunk_size > global_state.range_infos.size()) {
		chunk_size = global_state.range_infos.size() - global_state.tuples_processed;
	}
	output.SetCardinality(chunk_size);

	// Fill the output chunk directly from range_infos
	for (idx_t i = 0; i < chunk_size; i++) {
		idx_t row_idx = global_state.tuples_processed + i;
		const auto &info = global_state.range_infos[row_idx];
		output.data[0].SetValue(i, Value(info.protocol));
		output.data[1].SetValue(i, Value(info.filename));
		output.data[2].SetValue(i, Value::BIGINT(static_cast<int64_t>(info.file_offset)));
		output.data[3].SetValue(i, Value::BIGINT(static_cast<int64_t>(info.start)));
		output.data[4].SetValue(i, Value::BIGINT(static_cast<int64_t>(info.end)));
		output.data[5].SetValue(i, Value::BIGINT(static_cast<int64_t>(info.usage_count)));
		output.data[6].SetValue(i, Value::BIGINT(static_cast<int64_t>(info.bytes_from_cache)));
	}
	global_state.tuples_processed += chunk_size;
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
	
	// Get configuration for callbacks
	auto &config = DBConfig::GetConfig(instance);
	
	// Register table functions
	DUCKDB_LOG_DEBUG(instance, "[BlobCache] Registering table functions...");
	
	// Register blobcache_config table function (supports 0, 1, 2, 3, or 4 arguments)
	TableFunction blobcache_config_function("blobcache_config", {}, BlobCacheConfigFunction);
	blobcache_config_function.bind = BlobCacheConfigBind;
	blobcache_config_function.init_global = BlobCacheConfigInitGlobal;
	blobcache_config_function.varargs = LogicalType::ANY;  // Allow variable arguments
	ExtensionUtil::RegisterFunction(instance, blobcache_config_function);
	DUCKDB_LOG_DEBUG(instance, "[BlobCache] Registered blobcache_config function");
	
	// Register blobcache_stats table function
	TableFunction blobcache_stats_function("blobcache_stats", {}, BlobCacheStatsFunction);
	blobcache_stats_function.bind = BlobCacheStatsBind;
	blobcache_stats_function.init_global = BlobCacheStatsInitGlobal;
	ExtensionUtil::RegisterFunction(instance, blobcache_stats_function);
	DUCKDB_LOG_DEBUG(instance, "[BlobCache] Registered blobcache_stats function");

	// Register extension callback for automatic wrapping
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