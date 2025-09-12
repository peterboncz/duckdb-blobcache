#include "blobfs_wrapper.hpp"
#include "blobcache.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/logging/logger.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// BlobFilesystemWrapper Implementation
//===----------------------------------------------------------------------===//

unique_ptr<FileHandle> BlobFilesystemWrapper::OpenFile(const string &path, FileOpenFlags flags,
                                                        optional_ptr<FileOpener> opener) {
	auto wrapped_handle = wrapped_fs->OpenFile(path, flags, opener);
	
	if (cache->IsCacheInitialized() && flags.OpenForReading() && !flags.OpenForWriting()) {
		string cache_key = path + ":" + wrapped_fs->GetName();
		DUCKDB_LOG_DEBUG(*db_instance, "[BlobCache] File opened with caching enabled: path=%s, cache_key=%s", 
		                  path.c_str(), cache_key.c_str());
		return make_uniq<BlobFileHandle>(*this, std::move(wrapped_handle), cache_key, cache, db_instance);
	}
	
	return wrapped_handle;
}

void BlobFilesystemWrapper::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &blob_handle = handle.Cast<BlobFileHandle>();
	
	// Try cache first if initialized
	if (blob_handle.cache && blob_handle.cache->IsCacheInitialized()) {
		idx_t cache_bytes = static_cast<idx_t>(nr_bytes);
		idx_t actual_cache_bytes = blob_handle.cache->ReadFromCache(blob_handle.cache_key, location, buffer, cache_bytes);
		if (actual_cache_bytes > 0) {
			DUCKDB_LOG_DEBUG(*blob_handle.db_instance, "[BlobCache] Cache hit: cache_key=%s, %zu bytes served from cache", 
			                  blob_handle.cache_key.c_str(), actual_cache_bytes);
			return; // Full cache hit, we're done
		}
		
		// Cache miss - read from filesystem and cache the data
		wrapped_fs->Read(*blob_handle.wrapped_handle, buffer, nr_bytes, location);
		blob_handle.cache->InsertCache(blob_handle.cache_key, location, buffer, nr_bytes);
		DUCKDB_LOG_DEBUG(*blob_handle.db_instance, "[BlobCache] Cache miss: cache_key=%s, %lld bytes read and cached", 
		                  blob_handle.cache_key.c_str(), static_cast<long long>(nr_bytes));
		return;
	}
	
	// No cache - single filesystem read call
	wrapped_fs->Read(*blob_handle.wrapped_handle, buffer, nr_bytes, location);
}

int64_t BlobFilesystemWrapper::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &blob_handle = handle.Cast<BlobFileHandle>();
	idx_t current_position = wrapped_fs->SeekPosition(*blob_handle.wrapped_handle);
	
	// Try cache first if initialized
	if (blob_handle.cache && blob_handle.cache->IsCacheInitialized()) {
		idx_t cache_bytes = static_cast<idx_t>(nr_bytes);
		idx_t actual_cache_bytes = blob_handle.cache->ReadFromCache(blob_handle.cache_key, current_position, buffer, cache_bytes);
		if (actual_cache_bytes > 0) {
			DUCKDB_LOG_DEBUG(*blob_handle.db_instance, "[BlobCache] Cache hit: cache_key=%s, %zu bytes served from cache", 
			                  blob_handle.cache_key.c_str(), actual_cache_bytes);
			return static_cast<int64_t>(actual_cache_bytes); // Cache hit
		}
		
		// Cache miss - restore position and read from filesystem
		wrapped_fs->Seek(*blob_handle.wrapped_handle, current_position);
		int64_t actual_read = wrapped_fs->Read(*blob_handle.wrapped_handle, buffer, nr_bytes);
		
		// Cache the data if read was successful
		if (actual_read > 0) {
			blob_handle.cache->InsertCache(blob_handle.cache_key, current_position, buffer, actual_read);
			DUCKDB_LOG_DEBUG(*blob_handle.db_instance, "[BlobCache] Cache miss: cache_key=%s, %lld bytes read and cached", 
			                  blob_handle.cache_key.c_str(), actual_read);
		}
		return actual_read;
	}
	
	// No cache - single filesystem read call
	return wrapped_fs->Read(*blob_handle.wrapped_handle, buffer, nr_bytes);
}

void BlobFilesystemWrapper::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &blob_handle = handle.Cast<BlobFileHandle>();
	
	// Invalidate cache entries for this file when writing
	if (blob_handle.cache) {
		blob_handle.cache->InvalidateCache(blob_handle.cache_key);
		if (blob_handle.cache->IsCacheInitialized()) {
			DUCKDB_LOG_DEBUG(*blob_handle.db_instance, "[BlobCache] Write called: cache_key=%s, nr_bytes=%lld, location=%zu - Cache invalidated", 
			                  blob_handle.cache_key.c_str(), static_cast<long long>(nr_bytes), location);
		}
	}
	wrapped_fs->Write(*blob_handle.wrapped_handle, buffer, nr_bytes, location);
}

int64_t BlobFilesystemWrapper::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &blob_handle = handle.Cast<BlobFileHandle>();
	
	// Invalidate cache entries for this file when writing
	if (blob_handle.cache) {
		blob_handle.cache->InvalidateCache(blob_handle.cache_key);
		if (blob_handle.cache->IsCacheInitialized()) {
			DUCKDB_LOG_DEBUG(*blob_handle.db_instance, "[BlobCache] Write (seekless) called: cache_key=%s, nr_bytes=%lld - Cache invalidated", 
			                  blob_handle.cache_key.c_str(), static_cast<long long>(nr_bytes));
		}
	}
	return wrapped_fs->Write(*blob_handle.wrapped_handle, buffer, nr_bytes);
}

void BlobFilesystemWrapper::Truncate(FileHandle &handle, int64_t new_size) {
	auto &blob_handle = handle.Cast<BlobFileHandle>();
	
	// Invalidate cache entries for this file when truncating
	if (blob_handle.cache) {
		blob_handle.cache->InvalidateCache(blob_handle.cache_key);
		if (blob_handle.cache->IsCacheInitialized()) {
			DUCKDB_LOG_DEBUG(*blob_handle.db_instance, "[BlobCache] Truncate called: cache_key=%s, new_size=%lld - Cache invalidated", 
			                  blob_handle.cache_key.c_str(), static_cast<long long>(new_size));
		}
	}
	wrapped_fs->Truncate(*blob_handle.wrapped_handle, new_size);
}

void BlobFilesystemWrapper::MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) {
	// Invalidate cache entries for both source and target
	if (cache) {
		string source_key = source + ":" + wrapped_fs->GetName();
		string target_key = target + ":" + wrapped_fs->GetName();
		cache->InvalidateCache(source_key);
		cache->InvalidateCache(target_key);
		if (cache->IsCacheInitialized()) {
			DUCKDB_LOG_DEBUG(*db_instance, "[BlobCache] MoveFile called: source=%s, target=%s - Cache invalidated for both files", 
			                  source.c_str(), target.c_str());
		}
	}
	wrapped_fs->MoveFile(source, target, opener);
}

void BlobFilesystemWrapper::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	// Invalidate cache entries for this file
	if (cache) {
		string cache_key = filename + ":" + wrapped_fs->GetName();
		cache->InvalidateCache(cache_key);
		if (cache->IsCacheInitialized()) {
			DUCKDB_LOG_DEBUG(*db_instance, "[BlobCache] RemoveFile called: filename=%s - Cache invalidated", filename.c_str());
		}
	}
	wrapped_fs->RemoveFile(filename, opener);
}

} // namespace duckdb