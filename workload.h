#include <string>
#include <random>
#include "rocksdb/db.h"

using namespace rocksdb;


void RunWorkload(DB* db, const int& fd, const int& rate_of_workload, const std::string& id, bool& share);
