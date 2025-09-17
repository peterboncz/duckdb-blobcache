#include "blobfs_wrapper.hpp"
#include "blobcache.hpp"
#include "debug_filesystem.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/common/string_util.hpp"
#include <thread>
#include <chrono>

namespace duckdb {

//===----------------------------------------------------------------------===//
// BlobFilesystemWrapper Implementation
//===----------------------------------------------------------------------===//

unique_ptr<FileHandle> BlobFilesystemWrapper::OpenFile(const string &path, FileOpenFlags flags,
                                                        optional_ptr<FileOpener> opener) {
	auto wrapped_handle = wrapped_fs->OpenFile(path, flags, opener);

	if (cache->IsCacheInitialized() && flags.OpenForReading() && !flags.OpenForWriting()) {
		if (cache->ShouldCacheFile(path, opener)) {
			string cache_key = cache->GenerateCacheKey(path + ":" + wrapped_fs->GetName());
			return make_uniq<BlobFileHandle>(*this, std::move(wrapped_handle), cache_key, cache);
		}
	}

	return wrapped_handle;
}

#define HTTP_EFFICIENT_UNIT (1<<21)

static idx_t ReadChunk(duckdb::FileSystem &wrapped_fs, BlobFileHandle &blob_handle, char* buffer, idx_t location_orig, idx_t nr_bytes, bool add_debug_delay) {
	// NOTE: ReadFromCache() can return cached_bytes == 0 but adjust nr_bytes downwards to align with a cached range
	idx_t location = location_orig;
	idx_t nr_cached = blob_handle.cache->ReadFromCache(blob_handle.cache_key, location, buffer, nr_bytes);
	idx_t nr_read = nr_bytes - nr_cached;
	if (nr_read > 0) { // Read the non-cached range and cache it
		char* tmp_buffer = buffer + nr_cached;
		bool overfetch = false;
		idx_t actual_read_size = nr_read;

		// Debug logging
		blob_handle.cache->LogDebug(StringUtil::Format(
		    "[ReadChunk] location_orig=%llu, location=%llu, nr_bytes=%llu, nr_cached=%llu, nr_read=%llu",
		    location_orig, location, nr_bytes, nr_cached, nr_read));

		if (nr_read < HTTP_EFFICIENT_UNIT) { // overfetch to get bigger and faster requests
			tmp_buffer = new char[HTTP_EFFICIENT_UNIT];
			actual_read_size = HTTP_EFFICIENT_UNIT;
			overfetch = true;
			if (nr_cached == 0 && (location & ~(HTTP_EFFICIENT_UNIT-1)) + HTTP_EFFICIENT_UNIT > location+nr_bytes) {
				location &= ~(HTTP_EFFICIENT_UNIT-1);
			}
			blob_handle.cache->LogDebug(StringUtil::Format(
			    "[ReadChunk] Overfetching: new location=%llu, actual_read_size=%llu",
			    location, actual_read_size));
		}

		wrapped_fs.Seek(*blob_handle.wrapped_handle, location);
		// BUG FIX: Should read into tmp_buffer, not buffer!
		idx_t bytes_actually_read = wrapped_fs.Read(*blob_handle.wrapped_handle, tmp_buffer, actual_read_size);
		blob_handle.file_position = location + bytes_actually_read; // move file position

		blob_handle.cache->LogDebug(StringUtil::Format(
		    "[ReadChunk] Read %llu bytes from file at location %llu",
		    bytes_actually_read, location));

		// Insert into cache
		blob_handle.cache->InsertCache(blob_handle.cache_key, blob_handle.wrapped_handle->GetPath(), location, tmp_buffer, bytes_actually_read);

		if (overfetch) { // we overfetched to fill the cache
			// Copy only the requested portion to the output buffer
			idx_t offset = location_orig - location;
			idx_t copy_size = std::min(nr_read, bytes_actually_read - offset);

			blob_handle.cache->LogDebug(StringUtil::Format(
			    "[ReadChunk] Overfetch copy: offset=%llu, copy_size=%llu, buffer_offset=%llu",
			    offset, copy_size, nr_cached));

			memcpy(buffer + nr_cached, tmp_buffer + offset, copy_size);
			delete[] tmp_buffer;
			nr_read = copy_size;
		} else {
			// When not overfetching, tmp_buffer points to buffer+nr_cached, so data is already in place
			nr_read = bytes_actually_read;
		}

		if (add_debug_delay) { // Add delay for debug filesystem
			std::this_thread::sleep_for(std::chrono::seconds(1));
		}
	}
	return nr_cached + nr_read;
}

void BlobFilesystemWrapper::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &blob_handle = handle.Cast<BlobFileHandle>();
	if (!blob_handle.cache || !cache->IsCacheInitialized()) {
		wrapped_fs->Read(*blob_handle.wrapped_handle, buffer, nr_bytes, location);
		return; // a read that cannot cache
	}
	// the ReadFromCache() can break down one large range into multiple around caching boundaries
	char *buffer_ptr = static_cast<char*>(buffer);
	idx_t chunk_bytes;
	do { // keep iterating over ranges
		chunk_bytes = ReadChunk(*wrapped_fs, blob_handle, buffer_ptr, location, nr_bytes, ShouldAddDebugDelay());
		nr_bytes -= chunk_bytes;
		location += chunk_bytes;
		buffer_ptr += chunk_bytes;
	} while (nr_bytes > 0 && chunk_bytes > 0); //  not done reading and not EOF
}

int64_t BlobFilesystemWrapper::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &blob_handle = handle.Cast<BlobFileHandle>();
	if (!blob_handle.cache || !cache->IsCacheInitialized()) {
		return wrapped_fs->Read(*blob_handle.wrapped_handle, buffer, nr_bytes);
	}
	return ReadChunk(*wrapped_fs, blob_handle, static_cast<char*>(buffer), blob_handle.file_position, nr_bytes, ShouldAddDebugDelay());
}

void BlobFilesystemWrapper::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &blob_handle = handle.Cast<BlobFileHandle>();
	if (blob_handle.cache) {
		blob_handle.cache->InvalidateCache(blob_handle.cache_key);
	}
	wrapped_fs->Write(*blob_handle.wrapped_handle, buffer, nr_bytes, location);
	// Update position after write at explicit location
	blob_handle.file_position = location + nr_bytes;
}

int64_t BlobFilesystemWrapper::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &blob_handle = handle.Cast<BlobFileHandle>();
	if (blob_handle.cache) {
		blob_handle.cache->InvalidateCache(blob_handle.cache_key);
	}
	int64_t bytes_written = wrapped_fs->Write(*blob_handle.wrapped_handle, buffer, nr_bytes);
	if (bytes_written > 0) {
		blob_handle.file_position += bytes_written;
	}
	return bytes_written;
}

void BlobFilesystemWrapper::Truncate(FileHandle &handle, int64_t new_size) {
	auto &blob_handle = handle.Cast<BlobFileHandle>();
	if (blob_handle.cache) {
		blob_handle.cache->InvalidateCache(blob_handle.cache_key);
	}
	wrapped_fs->Truncate(*blob_handle.wrapped_handle, new_size);
}

void BlobFilesystemWrapper::MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) {
	if (cache) {
		cache->InvalidateCache(cache->GenerateCacheKey(source + ":" + wrapped_fs->GetName()));
		cache->InvalidateCache(cache->GenerateCacheKey(target + ":" + wrapped_fs->GetName()));
	}
	wrapped_fs->MoveFile(source, target, opener);
}

void BlobFilesystemWrapper::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	if (cache) {
		cache->InvalidateCache(cache->GenerateCacheKey(filename + ":" + wrapped_fs->GetName()));
	}
	wrapped_fs->RemoveFile(filename, opener);
}

bool BlobFilesystemWrapper::ShouldAddDebugDelay() {
	// Check if this is the debug filesystem and cache directory ends with '-filedebug'
	if (wrapped_fs->GetName() == "debug") {
		string cache_path = cache->GetCachePath();
		return StringUtil::EndsWith(cache_path, "-filedebug");
	}
	return false;
}

//===----------------------------------------------------------------------===//
// Cache Management Functions
//===----------------------------------------------------------------------===//
shared_ptr<BlobCache> GetOrCreateBlobCache(DatabaseInstance &instance) {
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

void WrapExistingFilesystems(DatabaseInstance &instance) {
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
			auto wrapped_fs = make_uniq<BlobFilesystemWrapper>(std::move(extracted_fs), shared_cache);
			DUCKDB_LOG_DEBUG(instance, "[BlobCache] Created wrapper with name: '%s'", wrapped_fs->GetName().c_str());
			vfs->RegisterSubSystem(std::move(wrapped_fs));
			DUCKDB_LOG_DEBUG(instance, "[BlobCache] Successfully registered wrapped subsystem for '%s'", name.c_str());
		} else {
			DUCKDB_LOG_ERROR(instance, "[BlobCache] Failed to extract '%s' - subsystem not wrapped", name.c_str());
		}
	}

	// Register debug:// filesystem for testing purposes - wrapped with caching
	DUCKDB_LOG_DEBUG(instance, "[BlobCache] Registering debug:// filesystem for testing");
	auto debug_fs = make_uniq<DebugFileSystem>();
	auto wrapped_debug_fs = make_uniq<BlobFilesystemWrapper>(std::move(debug_fs), shared_cache);
	vfs->RegisterSubSystem(std::move(wrapped_debug_fs));

	// Log final subsystem list
	auto final_subsystems = vfs->ListSubSystems();
	DUCKDB_LOG_DEBUG(instance, "[BlobCache] After wrapping, have %zu subsystems", final_subsystems.size());
	for (const auto &name : final_subsystems) {
		DUCKDB_LOG_DEBUG(instance, "[BlobCache] - %s", name.c_str());
	}
}

} // namespace duckdb