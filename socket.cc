#include <iostream>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>


int ListenSocket(const int& port_number) {
	sockaddr_in saddr;
	int         socket_fd;
	if ((socket_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
		std::cerr << "socket()" << std::endl;
		exit(EXIT_FAILURE);
	}

	memset(&saddr, 0, sizeof(saddr));
	saddr.sin_family      = AF_INET;
	saddr.sin_port        = htons(port_number);
	saddr.sin_addr.s_addr = htonl(INADDR_ANY);

	if (bind(socket_fd, (sockaddr*)&saddr, sizeof(saddr)) < 0) {
		std::cerr << "bind()" << std::endl;;
		close(socket_fd);
		exit(EXIT_FAILURE);
	}

	if (listen(socket_fd, SOMAXCONN) < 0) {
		std::cerr << "listen()" << std::endl;
		close(socket_fd);
		exit(EXIT_FAILURE);
	}

	return socket_fd;
}


int AcceptSocket(const int& socket_fd) {
	sockaddr_in caddr;
	int         fd;

	socklen_t len = sizeof(caddr);
	if ((fd = accept(socket_fd, (sockaddr*)&caddr, &len)) < 0) {
		std::cerr << "accept()" << std::endl;
		close(fd);
		close(socket_fd);
		exit(EXIT_FAILURE);
	}

	return fd;
}


int ConnectSocket(const int& port_number, const char* ip) {
	sockaddr_in saddr;
	int         fd;

	if ((fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
		std::cerr << "socket" << std::endl;
		exit(EXIT_FAILURE);
	}

	memset(&saddr, 0, sizeof(saddr));
	saddr.sin_family      = AF_INET;
	saddr.sin_port        = htons(port_number);
	saddr.sin_addr.s_addr = inet_addr(ip);

	if (connect(fd, (sockaddr*)&saddr, sizeof(saddr)) < 0) {
		std::cerr << "connect()" << std::endl;
		close(fd);
		exit(EXIT_FAILURE);
	}

	return fd;
}


void SendFlag(const int& fd, const short& flag) {
	send(fd, &flag, sizeof(short), 0);
}


short RecvFlag(const int& fd, const std::string& flag_name) {
	short flag = 0;

	if (recv(fd, &flag, sizeof(short), 0) < 0) {
		std::perror(("recv(" + flag_name + ")").c_str());
		exit(EXIT_FAILURE);
	}

	return flag;
}
