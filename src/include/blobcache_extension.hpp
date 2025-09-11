#pragma once

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/virtual_file_system.hpp"
#include "duckdb/common/opener_file_system.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/planner/extension_callback.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/logging/logger.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <iostream>

namespace duckdb {

class BlobcacheExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override {
		return "blobcache";
	}
	std::string Version() const override {
		return "0.1";
	}
};

//===----------------------------------------------------------------------===//
// BlobFileHandle - Wrapper for FileHandle that ensures all operations go through our wrapper
//===----------------------------------------------------------------------===//
class BlobFileHandle : public FileHandle {
public:
	BlobFileHandle(FileSystem &wrapper_fs, unique_ptr<FileHandle> wrapped_handle)
	    : FileHandle(wrapper_fs, wrapped_handle->GetPath(), wrapped_handle->GetFlags()),
	      wrapped_handle(std::move(wrapped_handle)) { }

	void Close() override {
		wrapped_handle->Close();
	}

	FileHandle* GetWrappedHandle() {
		return wrapped_handle.get();
	}

private:
	unique_ptr<FileHandle> wrapped_handle;
};

class BlobFilesystemWrapper : public FileSystem {
private:
	void LogDebug(const string &message, optional_ptr<FileOpener> opener = nullptr) {
		// Temporarily force logging to stderr for debugging
		std::cerr << "[BlobCache DEBUG] " << message << std::endl;
#ifdef DEBUG
		if (opener) {
			auto db = FileOpener::TryGetDatabase(opener);
			if (db) {
				DUCKDB_LOG_DEBUG(*db, "[BlobCache] %s", message.c_str());
			}
		}
#endif
	}
public:
	explicit BlobFilesystemWrapper(unique_ptr<FileSystem> wrapped_fs)
	    : wrapped_fs(std::move(wrapped_fs)) {
		LogDebug("BlobFilesystemWrapper created");
	}

	unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags flags, optional_ptr<FileOpener> opener = nullptr) override {
		LogDebug(StringUtil::Format("OpenFile: path=%s flags=%d", path.c_str(), static_cast<int>(flags.OpenForReading())), opener);
		auto handle = wrapped_fs->OpenFile(path, flags, opener);
		if (handle) {
			// Wrap the returned handle to ensure operations go through our wrapper
			return make_uniq<BlobFileHandle>(*this, std::move(handle));
		}
		return nullptr;
	}

	// reads will check the cache. if not present, they will add the file to the cache
	void Read(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Read(FileHandle &handle, void *buffer, int64_t nr_bytes) override;

	// write operations must clear the file(s) from the cache
	void Write(FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override;
	int64_t Write(FileHandle &handle, void *buffer, int64_t nr_bytes) override;
	bool Trim(FileHandle &handle, idx_t offset_bytes, idx_t length_bytes) override;
	void Truncate(FileHandle &handle, int64_t new_size) override;
	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	bool TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	void MoveFile(const string &source, const string &target,
	              optional_ptr<FileOpener> opener = nullptr) override;

	// we do not mess with the remaining methods, just call the wrapped file(sub)system
	int64_t GetFileSize(FileHandle &handle)  {
		auto *blob_handle = static_cast<BlobFileHandle*>(&handle);
		int64_t size = wrapped_fs->GetFileSize(*blob_handle->GetWrappedHandle());
		LogDebug(StringUtil::Format("GetFileSize: path=%s size=%lld bytes", handle.GetPath().c_str(), size));
		return size;
	}
	time_t GetLastModifiedTime(FileHandle &handle)  {
		auto *blob_handle = static_cast<BlobFileHandle*>(&handle);
		time_t mtime = wrapped_fs->GetLastModifiedTime(*blob_handle->GetWrappedHandle());
		LogDebug(StringUtil::Format("GetLastModifiedTime: path=%s mtime=%lld", handle.GetPath().c_str(), static_cast<long long>(mtime)));
		return mtime;
	}
	string GetVersionTag(FileHandle &handle)  {
		auto *blob_handle = static_cast<BlobFileHandle*>(&handle);
		string version_tag = wrapped_fs->GetVersionTag(*blob_handle->GetWrappedHandle());
		LogDebug(StringUtil::Format("GetVersionTag: path=%s version_tag=%s", handle.GetPath().c_str(), version_tag.c_str()));
		return version_tag;
	}
	FileType GetFileType(FileHandle &handle)  {
		auto *blob_handle = static_cast<BlobFileHandle*>(&handle);
		FileType file_type = wrapped_fs->GetFileType(*blob_handle->GetWrappedHandle());
		LogDebug(StringUtil::Format("GetFileType: path=%s type=%d", handle.GetPath().c_str(), static_cast<int>(file_type)));
		return file_type;
	}

	// TODO: if the file is in the cache we can answer positively immediately
	bool BlobFilesystemWrapper::FileExists(const string &filename, optional_ptr<FileOpener> opener)  {
		bool exists = wrapped_fs->FileExists(filename, opener);
		LogDebug(StringUtil::Format("FileExists: filename=%s exists=%d", filename.c_str(), exists), opener);
		return exists;
	}

	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener = nullptr) override {
		bool exists = wrapped_fs->DirectoryExists(directory, opener);
		LogDebug(StringUtil::Format("DirectoryExists: directory=%s exists=%d", directory.c_str(), exists), opener);
		return exists;
	}

	void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override {
		LogDebug(StringUtil::Format("CreateDirectory: directory=%s", directory.c_str()), opener);
		wrapped_fs->CreateDirectory(directory, opener);
	}

	void CreateDirectoriesRecursive(const string &path, optional_ptr<FileOpener> opener = nullptr) override {
		LogDebug(StringUtil::Format("CreateDirectoriesRecursive: path=%s", path.c_str()), opener);
		wrapped_fs->CreateDirectoriesRecursive(path, opener);
	}

	void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener = nullptr) override {
		LogDebug(StringUtil::Format("RemoveDirectory: directory=%s", directory.c_str()), opener);
		wrapped_fs->RemoveDirectory(directory, opener);
	}

	bool ListFiles(const string &directory,
	               const std::function<void(const string &, bool)> &callback,
	               FileOpener *opener = nullptr) override {
		bool success = wrapped_fs->ListFiles(directory, callback, opener);
		LogDebug(StringUtil::Format("ListFiles: directory=%s success=%d", directory.c_str(), success), opener);
		return success;
	}

	bool IsPipe(const string &filename, optional_ptr<FileOpener> opener = nullptr) override {
		bool is_pipe = wrapped_fs->IsPipe(filename, opener);
		LogDebug(StringUtil::Format("IsPipe: filename=%s is_pipe=%d", filename.c_str(), is_pipe), opener);
		return is_pipe;
	}


	void FileSync(FileHandle &handle) override {
		LogDebug(StringUtil::Format("FileSync: path=%s", handle.GetPath().c_str()));
		auto *blob_handle = static_cast<BlobFileHandle*>(&handle);
		wrapped_fs->FileSync(*blob_handle->GetWrappedHandle());
	}

	string GetHomeDirectory() override {
		string home_dir = wrapped_fs->GetHomeDirectory();
		LogDebug(StringUtil::Format("GetHomeDirectory: %s", home_dir.c_str()));
		return home_dir;
	}

	// Required pure virtual methods
	std::string GetName() const override {
		return "BlobCache[" + wrapped_fs->GetName() + "]";
	}

	// Additional virtual methods that may need to be overridden
	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override {
		auto results = wrapped_fs->Glob(path, opener);
		LogDebug(StringUtil::Format("Glob: path=%s found=%lld files", path.c_str(), static_cast<long long>(results.size())), opener);
		return results;
	}

	string PathSeparator(const string &path) override {
		return wrapped_fs->PathSeparator(path);
	}

	void RegisterSubSystem(unique_ptr<FileSystem> sub_fs) override {
		LogDebug(StringUtil::Format("RegisterSubSystem: %s", sub_fs->GetName().c_str()));
		wrapped_fs->RegisterSubSystem(std::move(sub_fs));
	}

	void RegisterSubSystem(FileCompressionType compression_type, unique_ptr<FileSystem> fs) override {
		LogDebug(StringUtil::Format("RegisterSubSystem: compression_type=%d", static_cast<int>(compression_type)));
		wrapped_fs->RegisterSubSystem(compression_type, std::move(fs));
	}

	void UnregisterSubSystem(const string &name) override {
		LogDebug(StringUtil::Format("UnregisterSubSystem: name=%s", name.c_str()));
		wrapped_fs->UnregisterSubSystem(name);
	}

	unique_ptr<FileSystem> ExtractSubSystem(const string &name) override {
		LogDebug(StringUtil::Format("ExtractSubSystem: name=%s", name.c_str()));
		return wrapped_fs->ExtractSubSystem(name);
	}

	vector<string> ListSubSystems() override {
		auto subsystems = wrapped_fs->ListSubSystems();
		LogDebug(StringUtil::Format("ListSubSystems: found %lld subsystems", static_cast<long long>(subsystems.size())));
		return subsystems;
	}

	bool CanHandleFile(const string &fpath) override {
		bool can_handle = wrapped_fs->CanHandleFile(fpath);
		LogDebug(StringUtil::Format("CanHandleFile: fpath=%s can_handle=%d", fpath.c_str(), can_handle));
		return can_handle;
	}

	void Seek(FileHandle &handle, idx_t location) override {
		LogDebug(StringUtil::Format("Seek: path=%s location=%lld", handle.GetPath().c_str(), location));
		auto *blob_handle = static_cast<BlobFileHandle*>(&handle);
		wrapped_fs->Seek(*blob_handle->GetWrappedHandle(), location);
	}

	void Reset(FileHandle &handle) override {
		LogDebug(StringUtil::Format("Reset: path=%s", handle.GetPath().c_str()));
		auto *blob_handle = static_cast<BlobFileHandle*>(&handle);
		wrapped_fs->Reset(*blob_handle->GetWrappedHandle());
	}

	idx_t SeekPosition(FileHandle &handle) override {
		auto *blob_handle = static_cast<BlobFileHandle*>(&handle);
		idx_t position = wrapped_fs->SeekPosition(*blob_handle->GetWrappedHandle());
		LogDebug(StringUtil::Format("SeekPosition: path=%s position=%lld", handle.GetPath().c_str(), position));
		return position;
	}

	bool IsManuallySet() override {
		return wrapped_fs->IsManuallySet();
	}

	bool CanSeek() override {
		return wrapped_fs->CanSeek();
	}

	bool OnDiskFile(FileHandle &handle) override {
		auto *blob_handle = static_cast<BlobFileHandle*>(&handle);
		return wrapped_fs->OnDiskFile(*blob_handle->GetWrappedHandle());
	}

	void SetDisabledFileSystems(const vector<string> &names) override {
		LogDebug(StringUtil::Format("SetDisabledFileSystems: %lld filesystems", static_cast<long long>(names.size())));
		wrapped_fs->SetDisabledFileSystems(names);
	}

private:
	unique_ptr<FileSystem> wrapped_fs;
};

// Custom VirtualFileSystem that intercepts RegisterSubSystem calls
class BlobCacheVirtualFileSystem : public VirtualFileSystem {
public:
	explicit BlobCacheVirtualFileSystem(unique_ptr<FileSystem> &&inner)
	    : VirtualFileSystem(std::move(inner)) {
		// No logging here as we don't have access to FileOpener
	}

	void RegisterSubSystem(unique_ptr<FileSystem> sub_fs) override {
		// Note: No logging here as we don't have access to DatabaseInstance/FileOpener

		// Check if this is a filesystem we want to wrap
		string fs_name = sub_fs->GetName();
		if (IsBlobFilesystem(StringUtil::Lower(fs_name))) {
			auto wrapped_fs = make_uniq<BlobFilesystemWrapper>(std::move(sub_fs));
			VirtualFileSystem::RegisterSubSystem(std::move(wrapped_fs));
		} else {
			// Register normally
			VirtualFileSystem::RegisterSubSystem(std::move(sub_fs));
		}
	}

	std::string GetName() const override {
		return "BlobCache-VFS";
	}

private:
	bool IsBlobFilesystem(const string &fs) {
		return fs == "http" || fs == "https" || fs == "s3" || fs == "r2" || fs == "gcs" || fs == "azure";
	}
};

} // namespace duckdb
