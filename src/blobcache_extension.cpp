#define DUCKDB_EXTENSION_MAIN

#include "blobcache_extension.hpp"
#include "duckdb.hpp"

namespace duckdb {

// TODO: read consults the cache and serves from there if present. Else, it should add the data to the cache after read
void BlobFilesystemWrapper::Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location)  {
	auto *blob_handle = static_cast<BlobFileHandle*>(&handle);
	wrapped_fs->Read(*blob_handle->GetWrappedHandle(), buffer, nr_bytes, location);
	LogDebug(StringUtil::Format("Read: path=%s requested=%lld location=%lld (completed)", handle.GetPath().c_str(), nr_bytes, location));
}
int64_t BlobFilesystemWrapper::Read(FileHandle &handle, void *buffer, int64_t nr_bytes)  {
	auto *blob_handle = static_cast<BlobFileHandle*>(&handle);
	int64_t bytes_read = wrapped_fs->Read(*blob_handle->GetWrappedHandle(), buffer, nr_bytes);
	LogDebug(StringUtil::Format("Read: path=%s requested=%lld actual_read=%lld", handle.GetPath().c_str(), nr_bytes, bytes_read));
	return bytes_read;
}

// TODO: write, trim, move, truncate and remove must remove the file from the cache
void BlobFilesystemWrapper::Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location)  {
	LogDebug(StringUtil::Format("Write: path=%s nr_bytes=%lld location=%lld", handle.GetPath().c_str(), nr_bytes, location));
	auto *blob_handle = static_cast<BlobFileHandle*>(&handle);
	wrapped_fs->Write(*blob_handle->GetWrappedHandle(), buffer, nr_bytes, location);
}
int64_t BlobFilesystemWrapper::Write(FileHandle &handle, void *buffer, int64_t nr_bytes)  {
	auto *blob_handle = static_cast<BlobFileHandle*>(&handle);
	int64_t bytes_written = wrapped_fs->Write(*blob_handle->GetWrappedHandle(), buffer, nr_bytes);
	LogDebug(StringUtil::Format("Write: path=%s requested=%lld actual_written=%lld", handle.GetPath().c_str(), nr_bytes, bytes_written));
	return bytes_written;
}
bool BlobFilesystemWrapper::Trim(FileHandle &handle, idx_t offset_bytes, idx_t length_bytes)  {
	auto *blob_handle = static_cast<BlobFileHandle*>(&handle);
	bool result = wrapped_fs->Trim(*blob_handle->GetWrappedHandle(), offset_bytes, length_bytes);
	LogDebug(StringUtil::Format("Trim: path=%s offset=%lld length=%lld success=%d", handle.GetPath().c_str(), offset_bytes, length_bytes, result));
	return result;
}
void BlobFilesystemWrapper::Truncate(FileHandle &handle, int64_t new_size)  {
	LogDebug(StringUtil::Format("Truncate: path=%s new_size=%lld", handle.GetPath().c_str(), new_size));
	auto *blob_handle = static_cast<BlobFileHandle*>(&handle);
	wrapped_fs->Truncate(*blob_handle->GetWrappedHandle(), new_size);
}
void BlobFilesystemWrapper::MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener)  {
	LogDebug(StringUtil::Format("MoveFile: source=%s target=%s", source.c_str(), target.c_str()), opener);
	wrapped_fs->MoveFile(source, target, opener);
}
void BlobFilesystemWrapper::RemoveFile(const string &filename, optional_ptr<FileOpener> opener)  {
	LogDebug(StringUtil::Format("RemoveFile: filename=%s", filename.c_str()), opener);
	wrapped_fs->RemoveFile(filename, opener);
}
bool BlobFilesystemWrapper::TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener)  {
	bool removed = wrapped_fs->TryRemoveFile(filename, opener);
	LogDebug(StringUtil::Format("TryRemoveFile: filename=%s removed=%d", filename.c_str(), removed), opener);
	return removed;
}

static void WrapExistingFilesystems(DatabaseInstance &instance) {
	std::cerr << "[BlobCache] WrapExistingFilesystems called!" << std::endl;
	auto &db_fs = FileSystem::GetFileSystem(instance);
	std::cerr << "[BlobCache] filesystem type: " << db_fs.GetName() << std::endl;
	DUCKDB_LOG_DEBUG(instance, "[BlobCache] wrapping filesystem type: %s", db_fs.GetName().c_str());

	// Get VirtualFileSystem
	auto vfs = dynamic_cast<VirtualFileSystem*>(&db_fs);
	if (!vfs) {
		auto *opener_fs = dynamic_cast<OpenerFileSystem*>(&db_fs);
		if (opener_fs) {
			vfs = dynamic_cast<VirtualFileSystem*>(&opener_fs->GetFileSystem());
		}
	}
	if (!vfs) {
		DUCKDB_LOG_WARN(instance, "[BlobCache] Cannot find VirtualFileSystem");
		return;
	}

	// Try to wrap each blob storage filesystem
	auto subsystems = vfs->ListSubSystems();
	DUCKDB_LOG_DEBUG(instance, "[BlobCache] Found %lld registered subsystems", static_cast<long long>(subsystems.size()));
	for (const auto &name : subsystems) {
		auto extracted_fs = vfs->ExtractSubSystem(name);
		if (extracted_fs) {
			DUCKDB_LOG_DEBUG(instance, "[BlobCache] wrapping subsystem: '%s' (GetName returns: '%s')", name.c_str(), extracted_fs->GetName().c_str());
			auto wrapped_fs = make_uniq<BlobFilesystemWrapper>(std::move(extracted_fs));
			vfs->RegisterSubSystem(std::move(wrapped_fs));
		} else {
			DUCKDB_LOG_DEBUG(instance, "[BlobCache] Failed to extract '%s' - subsystem not wrapped", name.c_str());
		}
	}
}

//===----------------------------------------------------------------------===//
// BlobCacheExtensionCallback - Automatic wrapping when target extensions load
//===----------------------------------------------------------------------===//
class BlobCacheExtensionCallback : public ExtensionCallback {
public:
	void OnExtensionLoaded(DatabaseInstance &db, const string &name) override {
		auto extension_name = StringUtil::Lower(name);
		DUCKDB_LOG_DEBUG(db, "[BlobCache] Extension loaded: %s", name.c_str());

		// Check if this is an extension that provides blob storage filesystems
		if (extension_name == "httpfs" || extension_name == "azure") {
			DUCKDB_LOG_INFO(db, "[BlobCache] Target extension '%s' loaded, automatically wrapping filesystems", name.c_str());
			WrapExistingFilesystems(db);
		}
	}
};

void BlobcacheExtension::Load(DuckDB &db) {
	auto &instance = *db.instance;
	std::cerr << "[BlobCache] BlobCache extension loaded!" << std::endl;
	DUCKDB_LOG_INFO(instance, "[BlobCache] BlobCache extension loaded");
	
	// Register extension callback for automatic wrapping
	auto &config = DBConfig::GetConfig(instance);
	std::cerr << "[BlobCache] Registering extension callback..." << std::endl;
	config.extension_callbacks.push_back(make_uniq<BlobCacheExtensionCallback>());
	
	// Wrap any existing filesystems (in case some were already loaded)
	std::cerr << "[BlobCache] Wrapping existing filesystems..." << std::endl;
	WrapExistingFilesystems(instance);
	std::cerr << "[BlobCache] Extension initialization complete!" << std::endl;
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
