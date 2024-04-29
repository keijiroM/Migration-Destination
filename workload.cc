#include <iostream>
#include <fcntl.h>
#include <unistd.h>
#include <sys/socket.h>
#include "./db.h"
#include "./workload.h"
#include "./debug.h"

using namespace rocksdb;


static void InitValue(std::string* value) {
	char buf[VALUELEN];
	for (int i = 0; i < VALUELEN; ++i)
		buf[i] = 'A' + (i % 26);

	buf[VALUELEN-1] = '\0';
	*value = buf;
}


static std::string ReturnRandomKey(std::mt19937 mt, const int& max_key_number) {
	std::uniform_int_distribution<> KeyDist(0, max_key_number-1);

	int n = KeyDist(mt);
	char buf[KEYLEN];

	sprintf(buf, "%09d", n);
	const std::string key = buf;

	return key;
}


static void OndemandPullValue(const int& fd, const std::string& key, std::string* value) {
	ssize_t send_size = send(fd, key.c_str(), KEYLEN, 0);
	if (send_size <= 0) {
		std::cerr << "recv(RecvValues)" << std::endl;
		exit(EXIT_FAILURE);
	}

	char buf[VALUELEN+1];
	ssize_t recv_size = recv(fd, buf, VALUELEN, 0);
	buf[VALUELEN] = '\0';

	if (recv_size < 0) {
		std::cerr << "recv(RecvValues)" << std::endl;
		exit(EXIT_FAILURE);
	}

	*value = buf;
	ssize_t total_size = recv_size;

	while (total_size < VALUELEN) {
		ssize_t buf_size_ = VALUELEN - total_size;
		char* buf_ = new char[buf_size_+1];

		ssize_t recv_size_ = recv(fd, buf_, buf_size_, 0);
		buf_[buf_size_] = '\0';

		if (recv_size_ < 0) {
			std::cerr << "recv(OndemandPullValue)" << std::endl;
			exit(EXIT_FAILURE);
		}

		if (value->size() < (size_t)buf_size_) {
			value->append(buf_);
		}

		total_size += recv_size_;

		delete[] buf_;
	}
}


void RunWorkload(DB* db, const int& fd, const int& rate_of_workload, const std::string& id, bool& share) {
	std::cout << "RunWorkloadThread is started." << std::endl;


	const std::string file_path = ReturnOutputFilePath(rate_of_workload, id);	
    int file_fd = open(file_path.c_str(), O_CREAT | O_WRONLY | O_TRUNC , 00644);
	if (file_fd < 0) {
		std::cerr << "open()" << std::endl;
		exit(EXIT_FAILURE);
	}


	std::string put_value;
	InitValue(&put_value);

	std::uniform_int_distribution<> PercentageDist(0, 99);
	std::mt19937 mt{ std::random_device{}() };
	
	OperationCount op_count;
    WLRunTime run_time;
	// std::chrono::system_clock::time_point start, end, tmp;
	run_time.start = std::chrono::system_clock::now();

	// while (share_value == 0) {
	while (!share) {
		int rate = PercentageDist(mt);
		const std::string key = ReturnRandomKey(mt, KEYMAX);

		if (rate < rate_of_workload) {
			// Put Operation
			Status status = db->Put(WriteOptions(), key, put_value);
			AssertStatus(status, "RunWorkload(Get)");

			++op_count.put_count;
		}
		else {
			// Get Operation
			std::string get_value;
			Status status = db->Get(ReadOptions(), db->DefaultColumnFamily(), key, &get_value);

			if (status.IsNotFound()) {
				OndemandPullValue(fd, key, &get_value);
				++op_count.get_count;
			}
			else
				AssertStatus(status, "RunWorkload(Get)");
        }


		run_time.mid = std::chrono::system_clock::now();
		run_time.mid_time = ReturnRunTime(run_time.start, run_time.mid);

		PrintThroughputToFile(file_fd, run_time.mid_time, run_time.prev_time, op_count.put_count, op_count.get_count);

		run_time.prev_time = run_time.mid_time;
	}

	run_time.end = std::chrono::system_clock::now();
	run_time.total_time = ReturnRunTime(run_time.start, run_time.end);

    close(file_fd);


	int total_req = op_count.put_count + op_count.get_count;
	std::cout << "Total Request: " << total_req << " (Put Reqest: " << op_count.put_count << ", Get Request: " << op_count.get_count << ")" << std::endl;
	std::cout << "Throughput   : " << ((double)total_req/1000)/(run_time.total_time/1000000) << " kreq/s" << std::endl;


	std::cout << "RunWorkloadThread is ended." << std::endl;
}
