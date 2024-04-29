#include <queue>
#include <mutex>
#include <condition_variable>
#include "./db.h"
#include "rocksdb/options.h"

using namespace rocksdb;


struct TransferSstFilesArgs {
	DB* db;
	Options options;
	std::vector<std::string> dbname;
	std::vector<SstFileData> file_datas;
	int number_of_files;
	int target_level;
	std::vector<std::vector<int>> fd;
	std::string id;

	TransferSstFilesArgs(DB* db_, 
					 const Options& options_, 
					 const std::vector<std::string>& dbname_,
					 const std::vector<SstFileData>& file_datas_, 
					 const int& number_of_files_, 
					 const int& target_level_, 
					 const std::vector<std::vector<int>>& fd_, 
					 const std::string& id_) 
	{
		db = db_;
		options = options_;
		dbname = dbname_;
		file_datas = file_datas_;
		number_of_files = number_of_files_;
		target_level = target_level_;
		fd = fd_;
		id = id_;
	}
};


class SstFile {
private:
	std::mutex kv_mtx;
	std::mutex file_mtx;
	std::condition_variable kv_cv;
	std::condition_variable file_cv;

public:
	void RecvKVPairs(const int& fd, 
					 std::queue<std::string>& que, 
					 const uint64_t& number_of_entries,
					 const ssize_t& buf_size, 
					 const int& thread_number /* for debug */, 
					 const int& func_id /* for debug */, 
					 const int& file_fd /* for debug */);
	void ImportKVPairs(const Options& options, 
					   const std::string& file_path, 
					   std::queue<std::string>& key_que, 
					   std::queue<std::string>& value_que,
					   std::queue<std::string>& file_que, 
					   const uint64_t& number_of_entries, 
					   const int& thread_number /* for debug */, 
					   const int& file_fd /* for debug */);
	void IngestSstFiles(DB* db, 
						std::queue<std::string>& file_que, 
						const int& file_level, 
						const int& thread_number /* for debug */);
};


void TransferSstFiles(TransferSstFilesArgs args, const int& number_of_threads, bool& share);
