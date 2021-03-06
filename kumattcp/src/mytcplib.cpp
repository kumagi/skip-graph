#include "mytcplib.h"

static int OK=1;

int create_tcpsocket(void){
	return socket(AF_INET,SOCK_STREAM, 0);
}
void bind_inaddr_any(const int socket,const unsigned short port){
	struct sockaddr_in myaddr;
	bzero((char*)&myaddr,sizeof(myaddr));
	myaddr.sin_family = AF_INET;
	myaddr.sin_addr.s_addr = htonl(INADDR_ANY);
	myaddr.sin_port=htons(port);
	if(bind(socket,(struct sockaddr*)&myaddr, sizeof(myaddr)) < 0){
		perror("bind");
		close(socket);
		exit (0);
	}
}
void connect_port_ip(const int socket,const int ip,const unsigned short port){
	
	struct sockaddr_in addr;
	bzero((char*)&addr,sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = ip;
	addr.sin_port=htons(port);
	
	if(connect(socket,(struct sockaddr*)&addr,sizeof(addr)) != 0){
		perror("connect");
	}
}

int connect_send_close(const int ip,const unsigned short port,const size_t length,const char* data){
	int socket = create_tcpsocket();
	set_linger(socket);
	connect_port_ip(socket,ip,port);
	size_t postsend=0,tmpsend;
	while(postsend == length){
		tmpsend = write(socket,data,length);
		if(tmpsend>0){
			postsend+=tmpsend;
		}else{
			return 1;
		}
	}
	close(socket);
	return 0;
}

void socket_maximize_sndbuf(const int socket){
    socklen_t intsize = sizeof(int);
    int last_good = 0;
    int min, max, avg;
    int old_size;

    if (getsockopt(socket, SOL_SOCKET, SO_SNDBUF, &old_size, &intsize) != 0) {
        return;
    }

    min = old_size;
    max = MAX_SENDBUF_SIZE;

    while (min <= max) {
        avg = ((unsigned int)(min + max)) / 2;
        if (setsockopt(socket, SOL_SOCKET, SO_SNDBUF, (void *)&avg, intsize) == 0) {
            last_good = avg;
            min = avg + 1;
        } else {
            max = avg - 1;
        }
    }
}
void socket_maximize_rcvbuf(const int socket){
    socklen_t intsize = sizeof(int);
    int last_good = 0;
    int min, max, avg;
    int old_size;

    if (getsockopt(socket, SOL_SOCKET, SO_RCVBUF, &old_size, &intsize) != 0) {
        return;
    }

    min = old_size;
    max = MAX_RECVBUF_SIZE;

    while (min <= max) {
        avg = ((unsigned int)(min + max)) / 2;
        if (setsockopt(socket, SOL_SOCKET, SO_RCVBUF, (void *)&avg, intsize) == 0) {
            last_good = avg;
            min = avg + 1;
        } else {
            max = avg - 1;
        }
    }
}

void set_reuse(const int socket){
	setsockopt(socket, SOL_SOCKET, SO_REUSEADDR, (void *)&OK, sizeof(OK));
}
void set_keepalive(const int socket){
	setsockopt(socket, SOL_SOCKET, SO_KEEPALIVE, (void *)&OK, sizeof(OK));
}
void set_linger(const int socket){
	setsockopt(socket, SOL_SOCKET, SO_LINGER, (void *)&OK, sizeof(OK));
}
