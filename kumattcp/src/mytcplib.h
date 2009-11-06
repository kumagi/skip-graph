#ifndef MYTCPLIB
#define MYTCPLIB
#include <stdio.h>
#include <stdlib.h>
#include <sys/un.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <assert.h>//assert


#define MAX_SENDBUF_SIZE (256 * 1024 * 1024)
#define MAX_RECVBUF_SIZE (256 * 1024 * 1024)
int create_tcpsocket(void);
void bind_inaddr_any(const int socket,const unsigned short port);
void connect_port_ip(const int socket,const int ip,const unsigned short port);
int connect_send_close(const int ip,const unsigned short port,const size_t length,const char* data);

void socket_maximize_sndbuf(const int socket);
void socket_maximize_rcvbuf(const int socket);
void set_reuse(const int socket);
void set_keepalive(const int socket);
void set_linger(const int socket);

#endif /* MYTCPLIB */
