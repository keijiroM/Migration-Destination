#include <iostream>
#include <sstream>
#include <sys/socket.h>
#include "./db.h"
#include "./socket.h"

using namespace rocksdb;


DB* CreateDB(const Options& options, const std::string& dbname) {
	Status status;
	// Status status = DestroyDB(dbname, options);
	// AssertStatus(status);

	DB* db;
	status = DB::Open(options, dbname, &db);

	if (!status.ok()) {
		std::cout << status.ToString() << std::endl;
		return nullptr;
	}

	return db;
}


void AssertStatus(Status status, const std::string& flag_name) {
	if (!status.ok()) {
		std::cout << status.ToString() << "("  + flag_name + ")" << std::endl;
		exit(EXIT_FAILURE);
	}
}


int TargetLevel(size_t size, size_t lv1_total, size_t lv1_file, double lv_scale, double f_scale) {
	size_t v = size, lv_size = lv1_total, fsize = lv1_file;
	int level;

	for (level = 0; lv_size <= v; ++level) {
		v -= lv_size;
		lv_size *= lv_scale;
		fsize *= f_scale;
	}

	return level;
}


std::string ReturnFilePath(const std::vector<std::string>& dbname,
						   const int& file_level,
						   const int& target_level, 
						   const bool& on_disk, 
						   const int& file_number) {
#ifdef RELOCATION
	if (file_level > target_level)
		return dbname[1]+"/import_file"+std::to_string(file_number)+".sst";
	else
		return dbname[0]+"/import_file"+std::to_string(file_number)+".sst";
#elif MIXING
	if (on_disk == true)
        return dbname[1]+"/import_file"+std::to_string(file_number)+".sst";
    else
        return dbname[0]+"/import_file"+std::to_string(file_number)+".sst";
#else
	return dbname[0]+"/import_file"+std::to_string(file_number)+".sst";
#endif
}


void RecvSstFileData(const int& fd, std::vector<SstFileData>& file_datas) {
	ssize_t size;

	if (recv(fd, &size, sizeof(size_t), 0) <= 0) {
		std::cerr << "recv()" << std::endl;
		exit(EXIT_FAILURE);
	}

	SendFlag(fd, 1);

	char buf[4096];
	std::string serialized_data;
	ssize_t total_size = 0;

	while (total_size < size) {
		ssize_t recv_size = recv(fd, buf, sizeof(buf), 0);

		if (recv_size <= 0) {
			std::cerr << "recv()" << std::endl;
			exit(EXIT_FAILURE);
		}

		serialized_data.append(buf, recv_size);
		total_size += recv_size;
	}

	// std::cout << serialized_data << std::endl;

	// JSON形式の文字列からベクターをデシリアライズ
	std::istringstream iss(serialized_data);
	{
		cereal::JSONInputArchive archive(iss);
		archive(file_datas);
	}

	SendFlag(fd, 1);
}


void RecvOptions(const int& fd, SrcOptions& src_options) {
	if (recv(fd, &src_options, sizeof(SrcOptions), 0) <= 0) {
		std::cerr << "recv(RecvOptions)" << std::endl;
		exit(EXIT_FAILURE);
	}
}
