#pragma once

#include "duckdb.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/common/opener_file_system.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "blobcache.hpp"

namespace duckdb {

// Forward declarations
class BlobCache;
class BlobFileHandle;

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

	// ObjectCacheEntry is properly destructed automatically
	~BlobCacheEntry() override = default;
};

//===----------------------------------------------------------------------===//
// BlobFileHandle - wraps original file handles to intercept reads
//===----------------------------------------------------------------------===//
class BlobFileHandle : public FileHandle {
public:
	BlobFileHandle(FileSystem &fs, unique_ptr<FileHandle> wrapped_handle, string filename,
	               shared_ptr<BlobCache> cache)
	    : FileHandle(fs, wrapped_handle->GetPath(), wrapped_handle->GetFlags()),
	      wrapped_handle(std::move(wrapped_handle)), filename(std::move(filename)),
	      cache(cache), file_position(0) {
	}

	~BlobFileHandle() override = default;

	void Close() override {
		if (wrapped_handle) {
			wrapped_handle->Close();
		}
	}

public:
	unique_ptr<FileHandle> wrapped_handle;
	string filename;  // Full filename with protocol prefix
	shared_ptr<BlobCache> cache;
	idx_t file_position;  // Track our own file position
};

//===----------------------------------------------------------------------===//
// BlobFilesystemWrapper - wraps the original blob filesystems with caching
//===----------------------------------------------------------------------===//
class BlobFilesystemWrapper : public FileSystem {
public:
	explicit BlobFilesystemWrapper(unique_ptr<FileSystem> wrapped_fs, shared_ptr<BlobCache> shared_cache)
	    : wrapped_fs(std::move(wrapped_fs)), cache(shared_cache) {
	}
	virtual ~BlobFilesystemWrapper() = default;

	// FileSystem interface implementation
	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override;

	// read ops are our caching opportunity -- worked out in cpp file
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

	// write ops just wrap but also invalidate the file from the cache
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	void Truncate(FileHandle &handle, int64_t new_size) override;
	void MoveFile(const string &source, const string &target, optional_ptr<FileOpener> opener = nullptr) override;
	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;

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
	void FileSync(FileHandle &handle) override {
		auto &blob_handle = handle.Cast<BlobFileHandle>();
		wrapped_fs->FileSync(*blob_handle.wrapped_handle);
	}
	void Seek(FileHandle &handle, idx_t location) override {
		auto &blob_handle = handle.Cast<BlobFileHandle>();
		blob_handle.file_position = location;
		if (blob_handle.wrapped_handle) {
			wrapped_fs->Seek(*blob_handle.wrapped_handle, location);
		}
	}
	void Reset(FileHandle &handle) override {
		auto &blob_handle = handle.Cast<BlobFileHandle>();
		blob_handle.file_position = 0;
		if (blob_handle.wrapped_handle) {
			wrapped_fs->Reset(*blob_handle.wrapped_handle);
		}
	}
	idx_t SeekPosition(FileHandle &handle) override {
		auto &blob_handle = handle.Cast<BlobFileHandle>();
		return blob_handle.file_position;
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
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override {
		return wrapped_fs->FileExists(filename, opener);
	}
	bool IsPipe(const string &filename, optional_ptr<FileOpener> opener = nullptr) override {
		return wrapped_fs->IsPipe(filename, opener);
	}
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

private:
	unique_ptr<FileSystem> wrapped_fs;
	shared_ptr<BlobCache> cache;
};

class DebugFileSystem : public LocalFileSystem {
public:
	DebugFileSystem() : LocalFileSystem() {}
	~DebugFileSystem() override = default;

	// Override to claim we can handle debug:// URLs
	bool CanHandleFile(const string &fpath) override {
		return StringUtil::StartsWith(StringUtil::Lower(fpath), "debug://");
	}

	// Override GetName to identify as debug filesystem
	string GetName() const override {
		return "debug";
	}

	// Override FileExists to handle debug:// URLs
	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override {
		string actual_path = StripDebugPrefix(filename);
		return LocalFileSystem::FileExists(actual_path, opener);
	}

	// Override OpenFile to strip debug:// prefix
	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags,
	                                optional_ptr<FileOpener> opener = nullptr) override {
		// Strip debug:// prefix to get actual local path
		string actual_path = StripDebugPrefix(path);

		// Call parent implementation with actual path
		return LocalFileSystem::OpenFile(actual_path, flags, opener);
	}

private:
	// Helper method to strip debug:// prefix
	string StripDebugPrefix(const string &path) {
		if (StringUtil::StartsWith(StringUtil::Lower(path), "debug://")) {
			return path.substr(8);
		}
		return path;
	}

};

// Cache management functions
shared_ptr<BlobCache> GetOrCreateBlobCache(DatabaseInstance &instance);

// Filesystem wrapping utility function
void WrapExistingFilesystems(DatabaseInstance &instance);

} // namespace duckdb