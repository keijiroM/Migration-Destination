#include <iostream> // for debug
#include <vector>
#include <unistd.h>
#include "./debug.h"
#include <fcntl.h>  // for debug

template <typename ... Args>
static std::string format(const std::string& fmt, Args ... args ) {
	size_t len = std::snprintf( nullptr, 0, fmt.c_str(), args ... );
	std::vector<char> buf(len + 1);
	std::snprintf(&buf[0], len + 1, fmt.c_str(), args ... );

	return std::string(&buf[0], &buf[0] + len);
}


void PrintThroughputToFile(const int& fd, 
						   const double& mid_time, 
						   const double& prev_time, 
						   const int& put_count, 
						   const int& get_count) {
	if (mid_time > prev_time) {
		const std::string time_ = format("%4.0lf", mid_time) + ",";
		const std::string put_count_ = format("%9d", put_count) + ",";
		const std::string get_count_ = format("%9d", get_count);
		const std::string buf = time_ + put_count_ + get_count_ + "\n";

		write(fd, buf.c_str(), buf.length());
	}
}


std::string ReturnOutputFilePath(const int& rate_of_workload, const std::string& id) {
	std::string file_path = "./result";

#ifdef EMERGENCY_MIGRATION
	file_path += "/emergency_migration";
#elif LOAD_BALANCING
	file_path += "/load_balancing";
#elif CONSOLIDATION
	file_path += "/consolidation";
#else
	file_path += "/default";
#endif

#ifdef RELOCATION
	file_path += "/relocation";
#elif ONLY_PMEM
	file_path += "/only_pmem";
#elif ONLY_DISK
	file_path += "/only_disk";
#else
	file_path += "/mixing";
#endif
	
	switch (rate_of_workload) {
		case 0:
			file_path += "/r100/" + id + ".csv";
			break;
		case 5:
			file_path += "/r95-w5/" + id + ".csv";
			break;
		case 50:
			file_path += "/r50-w50/" + id + ".csv";
			break;
		case 95:
			file_path += "/r5-w95/" + id + ".csv";
			break;
		default:
			file_path += "/w100/" + id + ".csv";
			break;
	}


	return file_path;
}


double ReturnRunTime(const std::chrono::system_clock::time_point& start, 
					 const std::chrono::system_clock::time_point& end) {
	return static_cast<double>(std::chrono::duration_cast<std::chrono::seconds>(end-start).count());
}


int FileOpen(const std::string& file_path) {
	int file_fd = open(file_path.c_str(), O_CREAT | O_WRONLY | O_TRUNC , 00644);

	if (file_fd < 0) {
		std::cerr << "open()" << std::endl;
		exit(EXIT_FAILURE);
	}

	return file_fd;
}
