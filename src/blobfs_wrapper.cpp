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
		if (cache->ShouldCacheFile(path, opener)) {
			string cache_key = path + ":" + wrapped_fs->GetName();
			return make_uniq<BlobFileHandle>(*this, std::move(wrapped_handle), cache_key, cache);
		}
	}
	
	return wrapped_handle;
}

void BlobFilesystemWrapper::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &blob_handle = handle.Cast<BlobFileHandle>();
	
	if (blob_handle.cache && blob_handle.cache->IsCacheInitialized()) {
		idx_t cache_bytes = static_cast<idx_t>(nr_bytes);
		idx_t actual_cache_bytes = blob_handle.cache->ReadFromCache(blob_handle.cache_key, location, buffer, cache_bytes);
		if (actual_cache_bytes > 0) return;
		wrapped_fs->Read(*blob_handle.wrapped_handle, buffer, nr_bytes, location);
		blob_handle.cache->InsertCache(blob_handle.cache_key, location, buffer, nr_bytes);
		return;
	}
	
	// No cache - single filesystem read call
	wrapped_fs->Read(*blob_handle.wrapped_handle, buffer, nr_bytes, location);
}

int64_t BlobFilesystemWrapper::Read(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &blob_handle = handle.Cast<BlobFileHandle>();
	idx_t current_position = wrapped_fs->SeekPosition(*blob_handle.wrapped_handle);
	
	if (blob_handle.cache && blob_handle.cache->IsCacheInitialized()) {
		idx_t cache_bytes = static_cast<idx_t>(nr_bytes);
		idx_t actual_cache_bytes = blob_handle.cache->ReadFromCache(blob_handle.cache_key, current_position, buffer, cache_bytes);
		if (actual_cache_bytes > 0) return static_cast<int64_t>(actual_cache_bytes);
		wrapped_fs->Seek(*blob_handle.wrapped_handle, current_position);
		int64_t actual_read = wrapped_fs->Read(*blob_handle.wrapped_handle, buffer, nr_bytes);
		if (actual_read > 0) {
			blob_handle.cache->InsertCache(blob_handle.cache_key, current_position, buffer, actual_read);
		}
		return actual_read;
	}
	
	// No cache - single filesystem read call
	return wrapped_fs->Read(*blob_handle.wrapped_handle, buffer, nr_bytes);
}

void BlobFilesystemWrapper::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) {
	auto &blob_handle = handle.Cast<BlobFileHandle>();
	if (blob_handle.cache) {
		blob_handle.cache->InvalidateCache(blob_handle.cache_key);
	}
	wrapped_fs->Write(*blob_handle.wrapped_handle, buffer, nr_bytes, location);
}

int64_t BlobFilesystemWrapper::Write(FileHandle &handle, void *buffer, int64_t nr_bytes) {
	auto &blob_handle = handle.Cast<BlobFileHandle>();
	if (blob_handle.cache) {
		blob_handle.cache->InvalidateCache(blob_handle.cache_key);
	}
	return wrapped_fs->Write(*blob_handle.wrapped_handle, buffer, nr_bytes);
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
		cache->InvalidateCache(source + ":" + wrapped_fs->GetName());
		cache->InvalidateCache(target + ":" + wrapped_fs->GetName());
	}
	wrapped_fs->MoveFile(source, target, opener);
}

void BlobFilesystemWrapper::RemoveFile(const string &filename, optional_ptr<FileOpener> opener) {
	if (cache) {
		cache->InvalidateCache(filename + ":" + wrapped_fs->GetName());
	}
	wrapped_fs->RemoveFile(filename, opener);
}

} // namespace duckdb