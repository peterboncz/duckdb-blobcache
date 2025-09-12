#pragma once

#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/config.hpp"
#include "blobcache.hpp"

namespace duckdb {

// Forward declarations
class BlobCache;
class BlobFileHandle;

//===----------------------------------------------------------------------===//
// BlobFileHandle - wraps original file handles to intercept reads
//===----------------------------------------------------------------------===//
class BlobFileHandle : public FileHandle {
public:
	BlobFileHandle(FileSystem &fs, unique_ptr<FileHandle> wrapped_handle, string cache_key,
	               shared_ptr<BlobCache> cache, DatabaseInstance *db)
	    : FileHandle(fs, wrapped_handle->GetPath(), wrapped_handle->GetFlags()),
	      wrapped_handle(std::move(wrapped_handle)), cache_key(std::move(cache_key)), 
	      cache(cache), db_instance(db) {
	}
	
	~BlobFileHandle() override = default;

	void Close() override {
		if (wrapped_handle) {
			wrapped_handle->Close();
		}
	}

public:
	unique_ptr<FileHandle> wrapped_handle;
	string cache_key;
	shared_ptr<BlobCache> cache;
	DatabaseInstance *db_instance;
};

//===----------------------------------------------------------------------===//
// BlobFilesystemWrapper - wraps the original blob filesystems with caching
//===----------------------------------------------------------------------===//
class BlobFilesystemWrapper : public FileSystem {
public:
	explicit BlobFilesystemWrapper(unique_ptr<FileSystem> wrapped_fs, DatabaseInstance *db_instance, shared_ptr<BlobCache> shared_cache)
	    : wrapped_fs(std::move(wrapped_fs)), cache(shared_cache), db_instance(db_instance) {
	}
	
	virtual ~BlobFilesystemWrapper() = default;

	// Initialize the cache
	void InitializeCache(const string &cache_dir, idx_t max_size) {
		cache->InitializeCache(cache_dir, max_size);
	}
	
	bool IsCacheInitialized() const {
		return cache->IsCacheInitialized();
	}
	
	void PopulateCacheStatistics(DataChunk &output, idx_t limit = 1000) {
		cache->PopulateCacheStatistics(output, limit);
	}

	// FileSystem interface implementation
	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override;
	
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	
	int64_t GetFileSize(FileHandle &handle) override {
		auto &blob_handle = handle.Cast<BlobFileHandle>();
		return wrapped_fs->GetFileSize(*blob_handle.wrapped_handle);
	}
	
	time_t GetLastModifiedTime(FileHandle &handle) override {
		auto &blob_handle = handle.Cast<BlobFileHandle>();
		return wrapped_fs->GetLastModifiedTime(*blob_handle.wrapped_handle);
	}
	
	FileType GetFileType(FileHandle &handle) override {
		auto &blob_handle = handle.Cast<BlobFileHandle>();
		return wrapped_fs->GetFileType(*blob_handle.wrapped_handle);
	}
	
	void Truncate(FileHandle &handle, int64_t new_size) override;
	
	void FileSync(FileHandle &handle) override {
		auto &blob_handle = handle.Cast<BlobFileHandle>();
		wrapped_fs->FileSync(*blob_handle.wrapped_handle);
	}
	
	void Seek(FileHandle &handle, idx_t location) override {
		auto &blob_handle = handle.Cast<BlobFileHandle>();
		if (blob_handle.wrapped_handle) {
			wrapped_fs->Seek(*blob_handle.wrapped_handle, location);
		}
	}
	
	void Reset(FileHandle &handle) override {
		auto &blob_handle = handle.Cast<BlobFileHandle>();
		if (blob_handle.wrapped_handle) {
			wrapped_fs->Reset(*blob_handle.wrapped_handle);
		}
	}
	
	idx_t SeekPosition(FileHandle &handle) override {
		auto &blob_handle = handle.Cast<BlobFileHandle>();
		if (blob_handle.wrapped_handle) {
			return wrapped_fs->SeekPosition(*blob_handle.wrapped_handle);
		}
		return 0;
	}
	
	bool CanSeek() override {
		return wrapped_fs->CanSeek();
	}
	
	bool OnDiskFile(FileHandle &handle) override {
		auto &blob_handle = handle.Cast<BlobFileHandle>();
		return wrapped_fs->OnDiskFile(*blob_handle.wrapped_handle);
	}
	
	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener = nullptr) override {
		return wrapped_fs->DirectoryExists(directory, opener);
	}
	
	void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override {
		wrapped_fs->CreateDirectory(directory, opener);
	}
	
	void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override {
		wrapped_fs->RemoveDirectory(directory, opener);
	}
	
	bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
	               FileOpener *opener = nullptr) override {
		return wrapped_fs->ListFiles(directory, callback, opener);
	}
	
	void MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener = nullptr) override;
	
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override {
		return wrapped_fs->FileExists(filename, opener);
	}
	
	bool IsPipe(const string &filename, optional_ptr<FileOpener> opener = nullptr) override {
		return wrapped_fs->IsPipe(filename, opener);
	}
	
	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	
	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override {
		return wrapped_fs->Glob(path, opener);
	}
	
	void RegisterSubSystem(unique_ptr<FileSystem> sub_fs) override {
		wrapped_fs->RegisterSubSystem(std::move(sub_fs));
	}
	
	void RegisterSubSystem(FileCompressionType compression_type, unique_ptr<FileSystem> fs) override {
		wrapped_fs->RegisterSubSystem(compression_type, std::move(fs));
	}
	
	void UnregisterSubSystem(const string &name) override {
		wrapped_fs->UnregisterSubSystem(name);
	}
	
	vector<string> ListSubSystems() override {
		return wrapped_fs->ListSubSystems();
	}
	
	bool CanHandleFile(const string &fpath) override {
		return wrapped_fs->CanHandleFile(fpath);
	}
	
	string GetName() const override {
		return "BlobCache:" + wrapped_fs->GetName();
	}
	
	string PathSeparator(const string &path) override {
		return wrapped_fs->PathSeparator(path);
	}
	
	unique_ptr<FileHandle> OpenCompressedFile(unique_ptr<FileHandle> handle, bool write) override {
		return wrapped_fs->OpenCompressedFile(std::move(handle), write);
	}

	// Statistics helper
	static string GetProtocolFromKey(const string &cache_key) {
		auto suffix_pos = cache_key.rfind(':');
		if (suffix_pos != string::npos) {
			return cache_key.substr(suffix_pos + 1);
		}
		return "";
	}

private:
	unique_ptr<FileSystem> wrapped_fs;
	shared_ptr<BlobCache> cache;
	DatabaseInstance *db_instance;
};

//===----------------------------------------------------------------------===//
// BlobCacheVirtualFileSystem - custom VFS for managing wrapper registration
//===----------------------------------------------------------------------===//
class BlobCacheVirtualFileSystem : public VirtualFileSystem {
public:
	explicit BlobCacheVirtualFileSystem(const string &path) : VirtualFileSystem() {
	}
	
	// Override ExtractSubSystem to prevent unwrapping our wrappers
	unique_ptr<FileSystem> ExtractSubSystem(const string &name) override {
		lock_guard<mutex> lock(wrapper_mutex);
		
		// Don't allow extraction of our wrappers
		if (IsWrapper(name)) {
			return nullptr;
		}
		
		return VirtualFileSystem::ExtractSubSystem(name);
	}
	
	// Track wrapper registration
	void RegisterWrapper(const string &name) {
		lock_guard<mutex> lock(wrapper_mutex);
		registered_wrappers.insert(name);
	}
	
	bool IsWrapper(const string &name) const {
		// No lock needed for const method with mutable mutex
		return registered_wrappers.find(name) != registered_wrappers.end();
	}

private:
	unordered_set<string> registered_wrappers;
	mutable mutex wrapper_mutex;
};

} // namespace duckdb