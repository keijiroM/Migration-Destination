#include <string>


int ListenSocket(const int& port_number);
int AcceptSocket(const int& socket_fd);
int ConnectSocket(const int& port_number, const char* ip);
void SendFlag(const int& fd, const short& flag);
short RecvFlag(const int& fd, const std::string& flag_name);
