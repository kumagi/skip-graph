#ifndef MYTCPLIB
#define MYTCPLIB
#include <stdio.h>
#include <stdlib.h>
#include <sys/un.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/ioctl.h>
#include <net/if.h>  

#include <assert.h>//assert

#define MAX_SENDBUF_SIZE (256 * 1024 * 1024)
#define MAX_RECVBUF_SIZE (256 * 1024 * 1024)
int create_tcpsocket(void);
char* my_ntoa(int ip);
void bind_inaddr_any(const int socket,const unsigned short port);
int connect_port_ip(const int socket,const int ip,const unsigned short port);

int connect_send_close(const int ip,const unsigned short port,const void* buff,const size_t bufflen);
int connect_send(int* socket,const int ip,const unsigned short port,const void* buff,const size_t bufflen);

void socket_maximize_sndbuf(const int socket);
void socket_maximize_rcvbuf(const int socket);
void set_reuse(const int socket);
void set_keepalive(const int socket);
void set_linger(const int socket);
int chk_myip(void);
int my_aton(char* ipaddress);

#endif /* MYTCPLIB */

