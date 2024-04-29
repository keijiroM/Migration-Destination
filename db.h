#include <string>
#include <vector>
#include <cereal/archives/json.hpp>
#include <cereal/types/vector.hpp>
#include "rocksdb/db.h"

#define PMEMSIZE 32*1024*1024*1024ULL
#define DISKSIZE 128*1024*1024*1024ULL
#define KEYMAX 240*1024*1024
#define KEYLEN 9
#define VALUELEN 4*1024
#define BUFSIZE 4096

using namespace rocksdb;


#ifndef _MYDB__
#define _MYDB__
const std::string pmem_dir = "/mnt/pmem0";
const std::string disk_dir = "/mnt/sdc0";

#ifdef ONLY_PMEM
const std::string pmem_db_dir = pmem_dir + "/only_pmem";
#elif ONLY_DISK
const std::string disk_db_dir = disk_dir + "/only_disk";
#else
const std::string pmem_db_dir = pmem_dir + "/mixing";
const std::string disk_db_dir = disk_dir + "/mixing";
#endif


struct SrcOptions {
	size_t write_buffer_size;
	int number_of_files;
};


struct SstFileData {
	bool on_disk;
	int level;
	uint64_t number_of_entries;

	template<class Archive>
	void serialize(Archive &archive) {
		archive(on_disk, level, number_of_entries);
	}
};
#endif


DB* CreateDB(const Options& options, const std::string& dbname);
// void AssertStatus(Status status);
void AssertStatus(Status status, const std::string& flag_name);
int TargetLevel(size_t size, size_t lv1_total, size_t lv1_file, double lv_scale, double f_scale);
std::string ReturnFilePath(const std::vector<std::string>& dbname, 
						   const int& file_level, 
						   const int& target_level, 
						   const bool& on_disk, 
						   const int& file_number);
void RecvSstFileData(const int& fd, std::vector<SstFileData>& file_datas);
void RecvOptions(const int& fd, SrcOptions& src_options);
