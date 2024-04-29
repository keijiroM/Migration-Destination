#include <iostream>
#include <thread>
#include <sys/socket.h>
#include "./sst.h"
#include "./debug.h"
#include <unistd.h> // for debug

std::mutex transfer_file_mutex;
RunTime import_time, ingest_time;	// for debug


void SstFile::RecvKVPairs(const int& fd, 
						  std::queue<std::string>& que, 
						  const uint64_t& number_of_entries,
						  const ssize_t& buf_size, 
						  const int& thread_number /* for debug */, 
						  const int& func_id /* for debug */, 
						  const int& file_fd /* for debug */) {
	for (uint64_t i = 0; i < number_of_entries; ++i) {
		char buf[buf_size+1];
		ssize_t recv_size = recv(fd, buf, buf_size, 0);

		if (recv_size < 0) {
			std::cerr << "recv(RecvValues)" << std::endl;
			exit(EXIT_FAILURE);
		}
		
		buf[buf_size] = '\0';
        std::string kv = buf;
		ssize_t total_size = recv_size;

		while (total_size < buf_size) {
			ssize_t buf_size_ = buf_size - total_size;
			char* buf_ = new char[buf_size_+1];

			ssize_t recv_size_ = recv(fd, buf_, buf_size_, 0);
			buf_[buf_size_] = '\0';

			if (recv_size_ < 0) {
				std::cerr << "recv(RecvValues)" << std::endl;
				exit(EXIT_FAILURE);
			}

			if (kv.size() < (size_t)buf_size) {
				kv.append(buf_);
			}
			total_size += recv_size_;

			delete[] buf_;
		}

		if (func_id == 0) {
			std::string str = std::to_string(thread_number) + ": " + kv + "\n";
			write(file_fd, str.c_str(), str.size());
		}


		{
			std::lock_guard<std::mutex> lock(kv_mtx);
			que.push(kv);
		}
		kv_cv.notify_one();
	}
}


void SstFile::ImportKVPairs(const Options& options, 
							const std::string& file_path, 
							std::queue<std::string>& key_que, 
							std::queue<std::string>& value_que,
							std::queue<std::string>& file_que, 
							const uint64_t& number_of_entries, 
							const int& thread_number /* for debug */, 
							const int& file_fd /* for debug */) {
	SstFileWriter sst_file_writer(EnvOptions(), options);
	Status status = sst_file_writer.Open(file_path);
	AssertStatus(status, "ImportKVPairs(Open)");

	for (uint64_t i = 0; i < number_of_entries; ++i) {
		std::unique_lock<std::mutex> lock(kv_mtx);
		kv_cv.wait(lock, [&]{ return !key_que.empty() && !value_que.empty(); });
		
		{
			// for debug
			if (thread_number == 0)
				import_time.start = std::chrono::system_clock::now();
		}

		const std::string key   = key_que.front();
		key_que.pop();
		const std::string value = value_que.front();
		value_que.pop();

		const std::string str = std::to_string(thread_number) + ": " + key + "\n";
		write(file_fd, str.c_str(), str.size());

		status = sst_file_writer.Put(key, value);
        AssertStatus(status, "ImportKVPairs(Put)");
		
		{
			// for debug
			if (thread_number == 0) {
				import_time.end = std::chrono::system_clock::now();
				import_time.time += ReturnRunTime(import_time.start, import_time.end);
			}
		}
	}

	status = sst_file_writer.Finish();
	AssertStatus(status, "ImportKVPairs(Finish)");
	

	{
		std::lock_guard<std::mutex> lock(file_mtx);
		file_que.push(file_path);
	}
	file_cv.notify_one();
}


void SstFile::IngestSstFiles(DB* db, 
							 std::queue<std::string>& file_que, 
							 const int& file_level, 
							 const int& thread_number /* for debug */) {
	std::unique_lock<std::mutex> lock(file_mtx);
	file_cv.wait(lock, [&]{ return !file_que.empty(); });

	{
		// for debug
		if (thread_number == 0)
			ingest_time.start = std::chrono::system_clock::now();
	}

    std::vector<std::string> ingest_file_paths;
    ingest_file_paths.push_back(file_que.front());
	file_que.pop();

	IngestExternalFileOptions ifo;
	ifo.move_files = true;
	ifo.external_level = file_level;

	Status status = db->IngestExternalFile(ingest_file_paths, ifo);
	AssertStatus(status, "IngestSstFiles(IngestExternalFile)");

	{
		// for debug
		if (thread_number == 0) {
			ingest_time.end = std::chrono::system_clock::now();
			ingest_time.time += ReturnRunTime(ingest_time.start, ingest_time.end);
		}
	}
}


static int file_count = 0;
static void RecvSstFiles(TransferSstFilesArgs args, 
						 const int& thread_number, 
						 const int& recv_file_fd /* for debug */, 
						 const int& import_file_fd /* for debug */) {
	std::vector<int> fd = args.fd[thread_number];
	int file_number;

	while (file_count < args.number_of_files) {
		{
			std::lock_guard<std::mutex> lock(transfer_file_mutex);
			file_number = file_count++;
		}

		SstFile sst_file;
		SstFileData file_data = args.file_datas[file_number];
		const std::string file_path = ReturnFilePath(args.dbname, 
													 file_data.level, 
													 file_data.on_disk, 
													 args.target_level, 
													 file_number);
		std::queue<std::string> key_que;
        std::queue<std::string> value_que;
		std::queue<std::string> file_que;

		std::thread RecvKeysThread([&]{ sst_file.RecvKVPairs(fd[0], 
														  std::ref(key_que), 
														  file_data.number_of_entries,
														  KEYLEN, 
														  thread_number /* for debug */, 
														  0 /* for debug */, 
														  recv_file_fd /* for debug */); });
		std::thread RecvValuesThread([&]{ sst_file.RecvKVPairs(fd[1], 
															  std::ref(value_que), 
															  file_data.number_of_entries,
															  VALUELEN, 
															  thread_number /* for debug */, 
															  1 /* for debug */,
															  recv_file_fd /* for debug */); });
		std::thread ImportKVPairsThread([&]{ sst_file.ImportKVPairs(args.options, 
																	file_path, 
																	std::ref(key_que), 
																	std::ref(value_que),
																	std::ref(file_que),
																	file_data.number_of_entries, 
																	thread_number /* for debug */, 
																	import_file_fd /* for debug */); });
		std::thread IngestSstFilesThread([&]{ sst_file.IngestSstFiles(args.db, 
																	  std::ref(file_que), 
																	  file_data.level, 
																	  thread_number /* for debug */); });
		
		RecvKeysThread.join();
		RecvValuesThread.join();
		ImportKVPairsThread.join();
		IngestSstFilesThread.join();
	}
}


void TransferSstFiles(TransferSstFilesArgs args, const int& number_of_threads, bool& share) {
	std::cout << "TransferSstFilesThread is started." << std::endl;


	std::thread *threads[number_of_threads];
	int recv_file_fds[number_of_threads];
	int import_file_fds[number_of_threads];
	for (int i = 0; i < number_of_threads; ++i) {
		const std::string recv_file_path = "recv" + std::to_string(i) + ".dat";
		const std::string import_file_path = "import" + std::to_string(i) + ".dat";
		recv_file_fds[i] = FileOpen(recv_file_path);
		import_file_fds[i] = FileOpen(import_file_path);

		threads[i] = new std::thread(RecvSstFiles, 
									 args, 
									 i, 
									 recv_file_fds[i] /* for debug */, 
									 import_file_fds[i] /* for debug */);
	}
	
	for (int i = 0; i < number_of_threads; ++i) {
		threads[i]->join();
		close(recv_file_fds[i]); // for debug
		close(import_file_fds[i]); // for debug
	}

	share = true;


	std::cout << "ImportKVPairs : " << import_time.time / 1000000 << " sec" << std::endl;
	std::cout << "IngestSstFiles: " << ingest_time.time / 1000000 << " sec" << std::endl;


	std::cout << "TransferSstFilesThread is ended." << std::endl;
}
