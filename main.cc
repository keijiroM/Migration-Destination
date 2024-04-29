// Destination
#include <iostream>
#include <thread>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include "./sst.h"
#include "./workload.h"
#include "./socket.h"
#include "./debug.h"

using namespace rocksdb;


static void StartInstance(const std::string& id, const int& rate_of_workload, const int& number_of_threads) {
	std::cout << "Destination Instance is started." << std::endl;

/*-------------------------------------- Phase 1 --------------------------------------*/
	
	
	// ソケットをオープン
	int port_number = 20000 + (atoi(id.c_str()) * 100);
	const char* ip = "192.168.202.204";

	int main_fd = ConnectSocket(port_number++, ip);
#ifndef IDLE
	int kv_fd = ConnectSocket(port_number++, ip);
#endif
	std::vector<std::vector<int>> sst_fd;
	for (int i = 0; i < number_of_threads; ++i) {
		std::vector<int> fd_vec;
		for (int j = 0; j < 2; ++j) {
			int fd = ConnectSocket(port_number++, ip);
			fd_vec.push_back(fd);
		}
		sst_fd.push_back(fd_vec);
	}


	// DBのパスを指定
	const std::string db_path = "/mig_inst" + id;
#ifdef ONLY_PMEM
	const std::string pmem_db_path = pmem_db_dir + db_path;
	const std::vector<std::string> dbname = {pmem_db_path};
#elif ONLY_DISK
	const std::string disk_db_path = disk_db_dir + db_path;
	const std::vector<std::string> dbname = {disk_db_path};
#else
	const std::string pmem_db_path = pmem_db_dir + db_path;
	const std::string disk_db_path = disk_db_dir + db_path;
	const std::vector<std::string> dbname = {pmem_db_path, disk_db_path};
#endif


/*-------------------------------------- Phase 1 --------------------------------------*/

	
	// SSTableメタデータを受信
	std::vector<SstFileData> file_datas;
	RecvSstFileData(main_fd, file_datas);

	// Optionsを受信
	SrcOptions src_options;
	RecvOptions(main_fd, src_options);

	Options options;
	options.IncreaseParallelism();
	options.create_if_missing = true;
	options.write_buffer_size = src_options.write_buffer_size;
#ifdef MIXING
	options.db_paths = {{dbname[0], PMEMSIZE}, {dbname[1], DISKSIZE}};
#endif

	// DBを作成
	DB* db = CreateDB(options, dbname[0]);
	std::cout << "dbname: " << dbname[0] << std::endl;

	SendFlag(main_fd, 1);


/*-------------------------------------- Phase 2 --------------------------------------*/

	
	// 移送時間計測開始
	RunTime run_time;
	run_time.start = std::chrono::system_clock::now();

	// オートコンパクションをオフにする
	Status status;
	status = db->SetOptions({{"disable_auto_compactions", "true"},});
	AssertStatus(status, "SetOptions");

	// SSTables転送スレッドに渡す引数を初期化
#ifdef RELOCATION
	int target_level = TargetLevel(options.db_paths[0].target_size,
								   options.max_bytes_for_level_base,
								   options.target_file_size_base,
								   options.max_bytes_for_level_multiplier,
								   options.target_file_size_multiplier);
#else
	int target_level = -1;
#endif

	bool share = false;
#ifndef IDLE
	std::thread RunWorkloadThread([&]{ RunWorkload(db, kv_fd, rate_of_workload, id, std::ref(share)); });
#endif
	TransferSstFilesArgs args(db, options, dbname, file_datas, src_options.number_of_files, target_level, sst_fd, id);

	// SSTables転送スレッドを実行
	std::thread TransferSstFilesThread([&]{ TransferSstFiles(args, number_of_threads, std::ref(share)); });
	TransferSstFilesThread.join();
#ifndef IDLE
	RunWorkloadThread.join();
#endif


	// 移送時間計測終了
	run_time.end = std::chrono::system_clock::now();
	run_time.time = ReturnRunTime(run_time.start, run_time.end);
	std::cout << "Migration_Time: " << run_time.time / 1000000 << " seconds" << std::endl;

	
/*-------------------------------------- Phase 3 --------------------------------------*/


	// オートコンパクションをオンにする
    status = db->SetOptions({{"disable_auto_compactions", "false"},});
    AssertStatus(status, "SetOptions");


	// ソケットを閉じる
	close(main_fd);
#ifndef IDLE
	close(kv_fd);
#endif
	for (int i = 0; i < number_of_threads; ++i) {
		for (int j = 0; j < 2; ++j) {
			close(sst_fd[i][j]);
		}
	}
	
	delete db;
}


int main(int argc, char* argv[]) {
	if (argc != 4) {
		std::cerr << "argument error" << std::endl;
		return -1;
	}
	
	const std::string id = argv[1];
	int rate_of_workload = atoi(argv[2]);
	int number_of_threads = atoi(argv[3]);

	StartInstance(id, rate_of_workload, number_of_threads);

	return 0;
}
