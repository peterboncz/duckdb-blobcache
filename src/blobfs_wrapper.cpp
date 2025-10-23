#include "blobfs_wrapper.hpp"
#include "blobcache.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// BlobFilesystemWrapper Implementation
//===----------------------------------------------------------------------===//

unique_ptr<FileHandle> BlobFilesystemWrapper::OpenFile(const string &path, FileOpenFlags flags,
                                                       optional_ptr<FileOpener> opener) {
	auto wrapped_handle = wrapped_fs->OpenFile(path, flags, opener);

	if (cache->state.blobcache_initialized && flags.OpenForReading() && !flags.OpenForWriting()) {
		if (cache->ShouldCacheFile(path, opener)) {
			string key = cache->state.GenCacheKey(path);
			return make_uniq<BlobFileHandle>(*this, path, std::move(wrapped_handle), key, cache);
		}
	}
	return wrapped_handle;
}

static idx_t ReadChunk(duckdb::FileSystem &wrapped_fs, BlobFileHandle &handle, char *buf, idx_t location, idx_t len) {
	// NOTE: ReadFromCache() can return cached_bytes == 0 but adjust max_nr_bytes downwards to align with a cached range
	handle.cache->state.LogDebug("ReadChunk(path=" + handle.uri + ", location=" + to_string(location) +
	                             ", max_nr_bytes=" + to_string(len) + ")");
	idx_t nr_cached = handle.cache->ReadFromCache(handle.key, handle.uri, location, len, buf);
#if 0
    if (nr_cached > 0) { // debug
		char *tmp_buf = new char[nr_cached];
		wrapped_fs.Seek(*blob_handle.wrapped_handle, location);
		idx_t tst_bytes = wrapped_fs.Read(*blob_handle.wrapped_handle, tmp_buf, nr_cached);
		if (tst_bytes != nr_cached) {
			throw "unable to read";
		} else if (memcmp(tmp_buf, buf, nr_cached)) {
			throw "unequal contents";
		}
	}
#endif
	if (len > nr_cached) { // Read the non-cached range and cache it
		idx_t nr_read = len - nr_cached;

		wrapped_fs.Seek(*handle.wrapped_handle, location + nr_cached);
		nr_read = wrapped_fs.Read(*handle.wrapped_handle, buf + nr_cached, nr_read);

		handle.cache->InsertCache(handle.key, handle.uri, location + nr_cached, nr_read, buf + nr_cached);

		if (nr_read && StringUtil::StartsWith(handle.uri, "fakes3://")) {
			// inspired on AnyBlob paper: lowest latency is 20ms, transfer 12MB/s for the first MB, 40MB/s beyond that
			uint64_t ms = (nr_read < (1 << 20)) ? (20 + ((80 * nr_read) >> 20)) : (75 + ((25 * nr_read) >> 20));
			std::this_thread::sleep_for(std::chrono::milliseconds(ms)); // simulate S3 latency
		}
	}
	handle.file_position = location + len; // move file position
	return len;
}

void BlobFilesystemWrapper::Read(FileHandle &handle, void *buf, int64_t nr_bytes, idx_t location) {
	auto &blob_handle = handle.Cast<BlobFileHandle>();
	if (!blob_handle.cache || !cache->state.blobcache_initialized) {
		wrapped_fs->Read(*blob_handle.wrapped_handle, buf, nr_bytes, location);
		return; // a read that cannot cache
	}
	// the ReadFromCache() can break down one large range into multiple around caching boundaries
	char *buf_ptr = static_cast<char *>(buf);
	idx_t chunk_bytes;
	do { // keep iterating over ranges
		chunk_bytes = ReadChunk(*wrapped_fs, blob_handle, buf_ptr, location, nr_bytes);
		nr_bytes -= chunk_bytes;
		location += chunk_bytes;
		buf_ptr += chunk_bytes;
	} while (nr_bytes > 0 && chunk_bytes > 0); //  not done reading and not EOF
}

int64_t BlobFilesystemWrapper::Read(FileHandle &handle, void *buf, int64_t nr_bytes) {
	auto &blob_handle = handle.Cast<BlobFileHandle>();
	if (!blob_handle.cache || !cache->state.blobcache_initialized) {
		return wrapped_fs->Read(*blob_handle.wrapped_handle, buf, nr_bytes);
	}
	return ReadChunk(*wrapped_fs, blob_handle, static_cast<char *>(buf), blob_handle.file_position, nr_bytes);
}

void BlobFilesystemWrapper::Write(FileHandle &handle, void *buf, int64_t nr_bytes, idx_t location) {
	auto &blob_handle = handle.Cast<BlobFileHandle>();
	wrapped_fs->Write(*blob_handle.wrapped_handle, buf, nr_bytes, location);
	if (blob_handle.cache && cache->state.blobcache_initialized) {
		blob_handle.cache->InsertCache(blob_handle.key, blob_handle.uri, location, nr_bytes, buf);
	}
	// Update position after write at explicit location
	blob_handle.file_position = location + nr_bytes;
}

int64_t BlobFilesystemWrapper::Write(FileHandle &handle, void *buf, int64_t nr_bytes) {
	auto &blob_handle = handle.Cast<BlobFileHandle>();
	nr_bytes = wrapped_fs->Write(*blob_handle.wrapped_handle, buf, nr_bytes);
	if (nr_bytes > 0) {
		if (blob_handle.cache && cache->state.blobcache_initialized) {
			blob_handle.cache->InsertCache(blob_handle.key, blob_handle.uri, blob_handle.file_position, nr_bytes, buf);
		}
		blob_handle.file_position += nr_bytes;
	}
	return nr_bytes;
}

void BlobFilesystemWrapper::Truncate(FileHandle &handle, int64_t new_size) {
	auto &blob_handle = handle.Cast<BlobFileHandle>();
	if (blob_handle.cache) {
		blob_handle.cache->EvictFile(blob_handle.key);
	}
	wrapped_fs->Truncate(*blob_handle.wrapped_handle, new_size);
}

void BlobFilesystemWrapper::MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener) {
	if (cache) {
		cache->EvictFile(source);
		cache->EvictFile(target);
	}
	wrapped_fs->MoveFile(source, target, opener);
}

void BlobFilesystemWrapper::RemoveFile(const string &uri, optional_ptr<FileOpener> opener) {
	if (cache) {
		cache->EvictFile(uri);
	}
	wrapped_fs->RemoveFile(uri, opener);
}

//===----------------------------------------------------------------------===//
// Cache Management Functions
//===----------------------------------------------------------------------===//
shared_ptr<BlobCache> GetOrCreateBlobCache(DatabaseInstance &instance) {
	auto &object_cache = instance.GetObjectCache();

	// Try to get existing cache
	auto cached_entry = object_cache.Get<BlobCacheObjectCacheEntry>("blobcache_instance");
	if (cached_entry) {
		DUCKDB_LOG_DEBUG(instance, "[BlobCache] Retrieved existing BlobCache from ObjectCache");
		return cached_entry->cache;
	}

	// Create new cache and store in ObjectCache
	auto new_cache = make_shared_ptr<BlobCache>(&instance);
	auto cache_entry = make_shared_ptr<BlobCacheObjectCacheEntry>(new_cache);
	object_cache.Put("blobcache_instance", cache_entry);
	DUCKDB_LOG_DEBUG(instance, "[BlobCache] Created and stored new BlobCache in ObjectCache");

	return new_cache;
}

void WrapExistingFilesystems(DatabaseInstance &instance) {
	auto &db_fs = FileSystem::GetFileSystem(instance);
	DUCKDB_LOG_DEBUG(instance, "[BlobCache] Filesystem type: %s", db_fs.GetName().c_str());

	// Get VirtualFileSystem
	auto vfs = dynamic_cast<VirtualFileSystem *>(&db_fs);
	if (!vfs) {
		auto *opener_fs = dynamic_cast<OpenerFileSystem *>(&db_fs);
		if (opener_fs) {
			DUCKDB_LOG_DEBUG(instance, "[BlobCache] Found OpenerFileSystem, getting wrapped VFS");
			vfs = dynamic_cast<VirtualFileSystem *>(&opener_fs->GetFileSystem());
		}
	}
	if (!vfs) {
		DUCKDB_LOG_DEBUG(instance, "[BlobCache] Cannot find VirtualFileSystem - skipping filesystem wrapping");
		return;
	}

	// Get the shared cache instance - only proceed if cache is initialized
	auto shared_cache = GetOrCreateBlobCache(instance);
	if (!shared_cache->state.blobcache_initialized) {
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

	// Register fakes3:// filesystem for testing purposes - wrapped with caching
	DUCKDB_LOG_DEBUG(instance, "[BlobCache] Registering fakes3:// filesystem for testing");
	auto fakes3_fs = make_uniq<FakeS3FileSystem>();
	auto wrapped_fakes3_fs = make_uniq<BlobFilesystemWrapper>(std::move(fakes3_fs), shared_cache);
	vfs->RegisterSubSystem(std::move(wrapped_fakes3_fs));

	// Log final subsystem list
	auto final_subsystems = vfs->ListSubSystems();
	DUCKDB_LOG_DEBUG(instance, "[BlobCache] After wrapping, have %zu subsystems", final_subsystems.size());
	for (const auto &name : final_subsystems) {
		DUCKDB_LOG_DEBUG(instance, "[BlobCache] - %s", name.c_str());
	}
}

} // namespace duckdb
