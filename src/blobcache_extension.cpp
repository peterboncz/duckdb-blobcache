#define DUCKDB_EXTENSION_MAIN

#include "blobcache_extension.hpp"
#include "blobfs_wrapper.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "duckdb/storage/external_file_cache.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// Table Functions
//===----------------------------------------------------------------------===//

// Bind data for blobcache_config function
struct BlobCacheConfigBindData : public FunctionData {
	string directory;
	idx_t max_size_mb;
	idx_t nr_io_threads;
	string regex_patterns; // New regex patterns parameter
	bool query_only;       // True if no parameters provided - just query current values

	BlobCacheConfigBindData(string dir, idx_t size, idx_t threads, string regexps = "", bool query = false)
	    : directory(std::move(dir)), max_size_mb(size), nr_io_threads(threads), regex_patterns(std::move(regexps)),
	      query_only(query) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<BlobCacheConfigBindData>(directory, max_size_mb, nr_io_threads, regex_patterns, query_only);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<BlobCacheConfigBindData>();
		return directory == other.directory && max_size_mb == other.max_size_mb &&
		       nr_io_threads == other.nr_io_threads && regex_patterns == other.regex_patterns &&
		       query_only == other.query_only;
	}
};

// Global state for both table functions
struct BlobCacheGlobalState : public GlobalTableFunctionState {
	idx_t tuples_processed = 0;
	vector<BlobCacheRangeInfo> smallrange_stats, largerange_stats;

	idx_t MaxThreads() const override {
		return 1; // Single threaded for simplicity
	}
};

void default_cache_sizes(DatabaseInstance &db, idx_t &max_size_mb, idx_t &nr_io_threads) {
	max_size_mb = db.NumberOfThreads() * 4096; // 4GB * threads
	nr_io_threads = std::min<idx_t>(256, db.NumberOfThreads());
}

// Bind function for blobcache_config
static unique_ptr<FunctionData> BlobCacheConfigBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	// Setup return schema - returns useful cache statistics
	return_types.push_back(LogicalType::VARCHAR); // cache_path
	return_types.push_back(LogicalType::BIGINT);  // max_size_bytes
	return_types.push_back(LogicalType::BIGINT);  // current_size_bytes
	return_types.push_back(LogicalType::BIGINT);  // io_threads
	return_types.push_back(LogicalType::BOOLEAN); // success

	names.push_back("cache_path");
	names.push_back("max_size_bytes");
	names.push_back("current_size_bytes");
	names.push_back("nr_io_threads");
	names.push_back("success");

	bool query_only = false;         // Default to configuration mode
	string directory = ".blobcache"; // Default
	string regex_patterns = "";      // Default empty (conservative mode)
	idx_t max_size_mb, nr_io_threads;
	default_cache_sizes(*context.db, max_size_mb, nr_io_threads);

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
			throw BinderException("blobcache_config: nr_io_threads cannot be NULL");
		}
		auto threads_val = input.inputs[2];
		if (threads_val.type().id() != LogicalTypeId::BIGINT && threads_val.type().id() != LogicalTypeId::INTEGER) {
			throw BinderException("blobcache_config: nr_io_threads must be an integer");
		}
		nr_io_threads = threads_val.GetValue<idx_t>();
		if (nr_io_threads <= 0) {
			throw BinderException("blobcache_config: nr_io_threads must be positive");
		}
		if (nr_io_threads > 256) {
			throw BinderException("blobcache_config: nr_io_threads cannot exceed 256");
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

	return make_uniq<BlobCacheConfigBindData>(std::move(directory), max_size_mb, nr_io_threads,
	                                          std::move(regex_patterns), query_only);
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
	// Setup return schema - returns cache statistics with 11 columns
	return_types.push_back(LogicalType::VARCHAR); // protocol
	return_types.push_back(LogicalType::VARCHAR); // uri
	return_types.push_back(LogicalType::VARCHAR); // cache_type
	return_types.push_back(LogicalType::VARCHAR); // file
	return_types.push_back(LogicalType::BIGINT);  // range_start_file
	return_types.push_back(LogicalType::BIGINT);  // range_start_uri
	return_types.push_back(LogicalType::BIGINT);  // range_size
	return_types.push_back(LogicalType::BIGINT);  // usage_count
	return_types.push_back(LogicalType::BIGINT);  // bytes_from_cache
	return_types.push_back(LogicalType::BIGINT);  // bytes_from_mem

	names.push_back("protocol");
	names.push_back("uri");
	names.push_back("cache_type");
	names.push_back("file");
	names.push_back("range_start_file");
	names.push_back("range_start_uri");
	names.push_back("range_size");
	names.push_back("usage_count");
	names.push_back("bytes_from_cache");
	names.push_back("bytes_from_mem");

	return nullptr; // No bind data needed for stats function
}

// blobcache_config(directory, max_size_mb, nr_io_threads) - Configure the blob cache
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
			                 "[BlobCache] Configuring cache: directory='%s', max_size=%zu MB, nr_io_threads=%zu, "
			                 "regex_patterns='%s'",
			                 bind_data.directory.c_str(), bind_data.max_size_mb, bind_data.nr_io_threads,
			                 bind_data.regex_patterns.c_str());

			// Configure cache first (including small_range_threshold)
			shared_cache->ConfigureCache(bind_data.directory, bind_data.max_size_mb * 1024 * 1024,
			                             bind_data.nr_io_threads);

			// Update regex patterns and purge non-qualifying cache entries
			shared_cache->UpdateRegexPatterns(bind_data.regex_patterns);
			success = true;
			// Now that cache is configured, wrap any existing filesystems
			WrapExistingFilesystems(*context.db);
		}
	}

	// Get current cache statistics (works whether configuration succeeded or not)
	if (shared_cache && shared_cache->state.blobcache_initialized) {
		cache_path = shared_cache->state.blobcache_dir;
		max_size_bytes = shared_cache->state.total_cache_capacity;
		current_size_bytes =
		    shared_cache->smallrange_map->current_cache_size + shared_cache->largerange_map->current_cache_size;
		writer_threads = shared_cache->nr_io_threads;
	}

	// Return the statistics tuple
	output.SetCardinality(1);
	output.data[0].SetValue(0, Value(cache_path));                 // cache_path
	output.data[1].SetValue(0, Value::BIGINT(max_size_bytes));     // max_size_bytes
	output.data[2].SetValue(0, Value::BIGINT(current_size_bytes)); // current_size_bytes
	output.data[3].SetValue(0, Value::BIGINT(writer_threads));     // nr_io_threads
	output.data[4].SetValue(0, Value::BOOLEAN(success));           // success
	global_state.tuples_processed = 1;
}

// blobcache_stats() - Return cache statistics in LRU order with chunking
static void BlobCacheStatsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &global_state = data.global_state->Cast<BlobCacheGlobalState>();

	// Load data on first call
	if (global_state.tuples_processed == 0) {
		auto cache = GetOrCreateBlobCache(*context.db);
		std::lock_guard<std::mutex> lock(cache->blobcache_mutex);
		global_state.smallrange_stats = cache->smallrange_map->GetStatistics();
		global_state.largerange_stats = cache->largerange_map->GetStatistics();
	}

	// Determine which cache to serve from
	auto &stats = (global_state.tuples_processed < global_state.smallrange_stats.size())
	                  ? global_state.smallrange_stats
	                  : global_state.largerange_stats;
	auto cache_type = (global_state.tuples_processed < global_state.smallrange_stats.size()) ? "small" : "large";
	idx_t offset = (global_state.tuples_processed < global_state.smallrange_stats.size())
	                   ? global_state.tuples_processed
	                   : global_state.tuples_processed - global_state.smallrange_stats.size();

	idx_t chunk_size = MinValue<idx_t>(STANDARD_VECTOR_SIZE, stats.size() - offset);
	output.SetCardinality(chunk_size);

	for (idx_t i = 0; i < chunk_size; i++) {
		const auto &info = stats[offset + i];
		output.data[0].SetValue(i, Value(info.protocol));
		output.data[1].SetValue(i, Value(info.uri));
		output.data[2].SetValue(i, Value(cache_type));
		output.data[3].SetValue(i, Value(info.file));
		output.data[4].SetValue(i, Value::BIGINT(info.range_start_file));
		output.data[5].SetValue(i, Value::BIGINT(info.range_start_uri));
		output.data[6].SetValue(i, Value::BIGINT(info.range_size));
		output.data[7].SetValue(i, Value::BIGINT(info.usage_count));
		output.data[8].SetValue(i, Value::BIGINT(info.bytes_from_cache));
		output.data[9].SetValue(i, Value::BIGINT(info.bytes_from_mem));
	}
	global_state.tuples_processed += chunk_size;
}

//===----------------------------------------------------------------------===//
// blobcache_prefetch - Scalar function to prefetch ranges into cache
//===----------------------------------------------------------------------===//

struct PrefetchRange {
	idx_t start;
	idx_t end;
	idx_t original_size; // Sum of original range sizes
};

static void BlobCachePrefetchFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto &db = *context.db;
	auto cache = GetOrCreateBlobCache(db);

	if (!cache->state.blobcache_initialized) {
		// Cache not initialized - mark all as failed
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
		ConstantVector::SetNull(result, true);
		return;
	}

	auto &uri_vec = args.data[0];
	auto &start_vec = args.data[1];
	auto &size_vec = args.data[2];
	auto count = args.size();

	// Flatten vectors to process them
	UnifiedVectorFormat uri_data, start_data, size_data;
	uri_vec.ToUnifiedFormat(count, uri_data);
	start_vec.ToUnifiedFormat(count, start_data);
	size_vec.ToUnifiedFormat(count, size_data);

	auto uri_ptr = UnifiedVectorFormat::GetData<string_t>(uri_data);
	auto start_ptr = UnifiedVectorFormat::GetData<int64_t>(start_data);
	auto size_ptr = UnifiedVectorFormat::GetData<int64_t>(size_data);

	// Group ranges by uri and concatenate nearby ranges
	unordered_map<string, vector<PrefetchRange>> ranges_by_file;

	for (idx_t i = 0; i < count; i++) {
		auto uri_idx = uri_data.sel->get_index(i);
		auto start_idx = start_data.sel->get_index(i);
		auto size_idx = size_data.sel->get_index(i);

		if (!uri_data.validity.RowIsValid(uri_idx) || !start_data.validity.RowIsValid(start_idx) ||
		    !size_data.validity.RowIsValid(size_idx)) {
			continue; // Skip null rows
		}

		string uri = uri_ptr[uri_idx].GetString();
		idx_t range_start = start_ptr[start_idx];
		idx_t range_size = size_ptr[size_idx];

		if (range_size == 0) {
			continue;
		}

		auto &ranges = ranges_by_file[uri];
		PrefetchRange new_range = {range_start, range_start + range_size, range_size};

		// Try to concatenate with the last range
		if (!ranges.empty()) {
			auto &last = ranges.back();
			idx_t concatenated_size = new_range.end - last.start;

			// Concatenate if concatenated_size seems cheaper to fetch than the two unconcatenated ranges
			if (EstimateS3(concatenated_size) < EstimateS3(last.original_size) + EstimateS3(new_range.original_size)) {
				last.end = new_range.end;
				last.original_size = last.original_size + new_range.original_size;;
				continue;
			}
		}
		ranges.push_back(new_range);
	}

	// Queue all concatenated ranges as async read jobs for parallel execution
	for (auto &entry : ranges_by_file) {
		const string &uri = entry.first;
		auto &ranges = entry.second;
		string key = cache->state.GenCacheKey(uri);

		for (auto &range : ranges) {
			BlobCacheReadJob job;
			job.key = key;
			job.uri = uri;
			job.range_start = range.start;
			job.range_size = range.end - range.start;
			cache->QueueIORead(job); // Queue read job to IO threads - will be executed in parallel
		}
	}

	// Return true for all rows
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::GetData<bool>(result)[0] = true;
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

	// Disable global external file cache to avoid double-caching with our disk cache
	// Our CachingFileSystem will still work (allocates BufferHandle but doesn't cache ranges)
	config.options.enable_external_file_cache = false;
	ExternalFileCache::Get(instance).SetEnabled(false);
	DUCKDB_LOG_DEBUG(instance, "[BlobCache] Disabled global external file cache");

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

	// Register blobcache_prefetch scalar function
	ScalarFunction blobcache_prefetch_function("blobcache_prefetch",
	                                           {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT},
	                                           LogicalType::BOOLEAN, BlobCachePrefetchFunction);
	loader.RegisterFunction(blobcache_prefetch_function);
	DUCKDB_LOG_DEBUG(instance, "[BlobCache] Registered blobcache_prefetch function");

	/// create an initial cache
	idx_t max_size_mb, nr_io_threads;
	default_cache_sizes(instance, max_size_mb, nr_io_threads);
	auto shared_cache = GetOrCreateBlobCache(instance);
	shared_cache->state.path_sep = instance.GetFileSystem().PathSeparator("");
	shared_cache->ConfigureCache(".blobcache", max_size_mb * 1024 * 1024, nr_io_threads);

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
