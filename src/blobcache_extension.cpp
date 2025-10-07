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
	idx_t writer_threads;
	string regex_patterns;       // New regex patterns parameter
	idx_t small_range_threshold; // Small range threshold parameter
	bool query_only;             // True if no parameters provided - just query current values

	BlobCacheConfigBindData(string dir, idx_t size, idx_t threads, string regexps = "", idx_t threshold = 2047,
	                        bool query = false)
	    : directory(std::move(dir)), max_size_mb(size), writer_threads(threads), regex_patterns(std::move(regexps)),
	      small_range_threshold(threshold), query_only(query) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<BlobCacheConfigBindData>(directory, max_size_mb, writer_threads, regex_patterns,
		                                          small_range_threshold, query_only);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<BlobCacheConfigBindData>();
		return directory == other.directory && max_size_mb == other.max_size_mb &&
		       writer_threads == other.writer_threads && regex_patterns == other.regex_patterns &&
		       small_range_threshold == other.small_range_threshold && query_only == other.query_only;
	}
};

// Global state for both table functions
struct BlobCacheGlobalState : public GlobalTableFunctionState {
	idx_t tuples_processed = 0;
	vector<CacheRangeInfo> small_stats, large_stats;

	idx_t MaxThreads() const override {
		return 1; // Single threaded for simplicity
	}
};

// Bind function for blobcache_config
static unique_ptr<FunctionData> BlobCacheConfigBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	// Setup return schema - returns useful cache statistics
	return_types.push_back(LogicalType::VARCHAR); // cache_path
	return_types.push_back(LogicalType::BIGINT);  // max_size_bytes
	return_types.push_back(LogicalType::BIGINT);  // current_size_bytes
	return_types.push_back(LogicalType::BIGINT);  // writer_threads
	return_types.push_back(LogicalType::BOOLEAN); // success

	names.push_back("cache_path");
	names.push_back("max_size_bytes");
	names.push_back("current_size_bytes");
	names.push_back("writer_threads");
	names.push_back("success");

	string directory = ".blobcache";    // Default
	idx_t max_size_mb = 256;            // Default 256MB
	idx_t writer_threads = 1;           // Default 1 thread
	string regex_patterns = "";         // Default empty (conservative mode)
	idx_t small_range_threshold = 2047; // Default 2047 bytes
	bool query_only = false;            // Default to configuration mode

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
	}

	if (input.inputs.size() >= 3) {
		if (input.inputs[2].IsNull()) {
			throw BinderException("blobcache_config: writer_threads cannot be NULL");
		}
		auto threads_val = input.inputs[2];
		if (threads_val.type().id() != LogicalTypeId::BIGINT && threads_val.type().id() != LogicalTypeId::INTEGER) {
			throw BinderException("blobcache_config: writer_threads must be an integer");
		}
		writer_threads = threads_val.GetValue<idx_t>();
		if (writer_threads <= 0) {
			throw BinderException("blobcache_config: writer_threads must be positive");
		}
		if (writer_threads > 256) {
			throw BinderException("blobcache_config: writer_threads cannot exceed 256");
		}
	}

	if (input.inputs.size() >= 4) {
		if (input.inputs[3].IsNull()) {
			throw BinderException("blobcache_config: regex_patterns cannot be NULL");
		}
		auto patterns_val = input.inputs[3];
		if (patterns_val.type().id() != LogicalTypeId::VARCHAR) {
			throw BinderException("blobcache_config: regex_patterns must be a string");
		}
		regex_patterns = StringValue::Get(patterns_val);
	}

	if (input.inputs.size() >= 5) {
		if (input.inputs[4].IsNull()) {
			throw BinderException("blobcache_config: small_range_threshold cannot be NULL");
		}
		auto threshold_val = input.inputs[4];
		if (threshold_val.type().id() != LogicalTypeId::BIGINT && threshold_val.type().id() != LogicalTypeId::INTEGER) {
			throw BinderException("blobcache_config: small_range_threshold must be an integer");
		}
		small_range_threshold = threshold_val.GetValue<idx_t>();
		// Allow 0 to disable small-range cache (all ranges go to large cache)
	}

	return make_uniq<BlobCacheConfigBindData>(std::move(directory), max_size_mb, writer_threads,
	                                          std::move(regex_patterns), small_range_threshold, query_only);
}

// Init function for blobcache_config global state
static unique_ptr<GlobalTableFunctionState> BlobCacheConfigInitGlobal(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	return make_uniq<BlobCacheGlobalState>();
}

// Init function for blobcache_stats global state (same as config)
static unique_ptr<GlobalTableFunctionState> BlobCacheStatsInitGlobal(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
	return make_uniq<BlobCacheGlobalState>();
}

// Bind function for blobcache_stats
static unique_ptr<FunctionData> BlobCacheStatsBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	// Setup return schema - returns cache statistics with 8 columns (added cache_type)
	return_types.push_back(LogicalType::VARCHAR); // protocol
	return_types.push_back(LogicalType::VARCHAR); // filename
	return_types.push_back(LogicalType::VARCHAR); // cache_type
	return_types.push_back(LogicalType::BIGINT);  // file_offset
	return_types.push_back(LogicalType::BIGINT);  // start
	return_types.push_back(LogicalType::BIGINT);  // end
	return_types.push_back(LogicalType::BIGINT);  // usage_count
	return_types.push_back(LogicalType::BIGINT);  // bytes_from_cache

	names.push_back("protocol");
	names.push_back("filename");
	names.push_back("cache_type");
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
	idx_t current_size_bytes = 0;
	idx_t writer_threads = 0;

	if (data_p.bind_data && shared_cache) {
		auto &bind_data = data_p.bind_data->Cast<BlobCacheConfigBindData>();

		if (bind_data.query_only) {
			// Query-only mode - just return current values without changing anything
			DUCKDB_LOG_DEBUG(*context.db, "[BlobCache] Querying cache configuration");
			success = true; // Query always succeeds
		} else {
			// Configuration mode - actually configure the cache
			DUCKDB_LOG_DEBUG(*context.db,
			                 "[BlobCache] Configuring cache: directory='%s', max_size=%zu MB, writer_threads=%zu, "
			                 "regex_patterns='%s', small_range_threshold=%zu",
			                 bind_data.directory.c_str(), bind_data.max_size_mb, bind_data.writer_threads,
			                 bind_data.regex_patterns.c_str(), bind_data.small_range_threshold);

			// Configure cache first (including small_range_threshold)
			shared_cache->ConfigureCache(bind_data.directory, bind_data.max_size_mb * 1024 * 1024,
			                             bind_data.writer_threads, bind_data.small_range_threshold);

			// Update regex patterns and purge non-qualifying cache entries
			shared_cache->UpdateRegexPatterns(bind_data.regex_patterns);
			shared_cache->PurgeCacheForPatternChange(nullptr);
			success = true;
			// Now that cache is configured, wrap any existing filesystems
			WrapExistingFilesystems(*context.db);
		}
	}

	// Get current cache statistics (works whether configuration succeeded or not)
	if (shared_cache && shared_cache->config.cache_initialized) {
		cache_path = shared_cache->config.cache_dir;
		max_size_bytes = shared_cache->config.total_cache_capacity;
		current_size_bytes = shared_cache->small_cache->current_size + shared_cache->large_cache->current_size;
		writer_threads = shared_cache->num_writer_threads;
	}

	// Return the statistics tuple
	output.SetCardinality(1);
	output.data[0].SetValue(0, Value(cache_path));                 // cache_path
	output.data[1].SetValue(0, Value::BIGINT(max_size_bytes));     // max_size_bytes
	output.data[2].SetValue(0, Value::BIGINT(current_size_bytes)); // current_size_bytes
	output.data[3].SetValue(0, Value::BIGINT(writer_threads));     // writer_threads
	output.data[4].SetValue(0, Value::BOOLEAN(success));           // success
	global_state.tuples_processed = 1;
}

// blobcache_stats() - Return cache statistics in LRU order with chunking
static void BlobCacheStatsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &global_state = data.global_state->Cast<BlobCacheGlobalState>();

	// Load data on first call
	if (global_state.tuples_processed == 0) {
		auto cache = GetOrCreateBlobCache(*context.db);
		std::lock_guard<std::mutex> lock(cache->cache_mutex);
		global_state.small_stats = cache->small_cache->GetStatistics();
		global_state.large_stats = cache->large_cache->GetStatistics();
	}

	// Determine which cache to serve from
	auto &stats = (global_state.tuples_processed < global_state.small_stats.size()) ? global_state.small_stats
	                                                                                : global_state.large_stats;
	auto cache_type = (global_state.tuples_processed < global_state.small_stats.size()) ? "small" : "large";
	idx_t offset = (global_state.tuples_processed < global_state.small_stats.size())
	                   ? global_state.tuples_processed
	                   : global_state.tuples_processed - global_state.small_stats.size();

	idx_t chunk_size = MinValue<idx_t>(STANDARD_VECTOR_SIZE, stats.size() - offset);
	output.SetCardinality(chunk_size);

	for (idx_t i = 0; i < chunk_size; i++) {
		const auto &info = stats[offset + i];
		output.data[0].SetValue(i, Value(info.protocol));
		output.data[1].SetValue(i, Value(info.filename));
		output.data[2].SetValue(i, Value(cache_type));
		output.data[3].SetValue(i, Value::BIGINT(info.file_offset));
		output.data[4].SetValue(i, Value::BIGINT(info.start));
		output.data[5].SetValue(i, Value::BIGINT(info.end));
		output.data[6].SetValue(i, Value::BIGINT(info.usage_count));
		output.data[7].SetValue(i, Value::BIGINT(info.bytes_from_cache));
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
			DUCKDB_LOG_DEBUG(db, "[BlobCache] Target extension '%s' loaded, automatically wrapping filesystems",
			                 name.c_str());
			WrapExistingFilesystems(db);
		}
	}
};

void BlobcacheExtension::Load(ExtensionLoader &loader) {
	auto &instance = loader.GetDatabaseInstance();
	DUCKDB_LOG_DEBUG(instance, "[BlobCache] BlobCache extension loaded!");

	// Get configuration for callbacks
	auto &config = DBConfig::GetConfig(instance);

	// Register table functions
	DUCKDB_LOG_DEBUG(instance, "[BlobCache] Registering table functions...");

	// Register blobcache_config table function (supports 0, 1, 2, 3, or 4 arguments)
	TableFunction blobcache_config_function("blobcache_config", {}, BlobCacheConfigFunction);
	blobcache_config_function.bind = BlobCacheConfigBind;
	blobcache_config_function.init_global = BlobCacheConfigInitGlobal;
	blobcache_config_function.varargs = LogicalType::ANY; // Allow variable arguments
	loader.RegisterFunction(blobcache_config_function);
	DUCKDB_LOG_DEBUG(instance, "[BlobCache] Registered blobcache_config function");

	// Register blobcache_stats table function
	TableFunction blobcache_stats_function("blobcache_stats", {}, BlobCacheStatsFunction);
	blobcache_stats_function.bind = BlobCacheStatsBind;
	blobcache_stats_function.init_global = BlobCacheStatsInitGlobal;
	loader.RegisterFunction(blobcache_stats_function);
	DUCKDB_LOG_DEBUG(instance, "[BlobCache] Registered blobcache_stats function");

	/// create an initial cache
	auto shared_cache = GetOrCreateBlobCache(instance);
	shared_cache->config.path_sep = instance.GetFileSystem().PathSeparator("");
	shared_cache->ConfigureCache(".blobcache");

	// Register extension callback for automatic wrapping
	config.extension_callbacks.push_back(make_uniq<BlobCacheExtensionCallback>());

	// Wrap any existing filesystems (in case some were already loaded)
	WrapExistingFilesystems(instance);
	DUCKDB_LOG_DEBUG(instance, "[BlobCache] Extension initialization complete!");
}

} // namespace duckdb

#ifdef DUCKDB_BUILD_LOADABLE_EXTENSION
extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(blobcache, loader) {
	duckdb::BlobcacheExtension extension;
	extension.Load(loader);
}
}
#endif

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
