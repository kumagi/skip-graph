#include "mytcplib.h"

static int OK=1;

int create_tcpsocket(void){
	int fd = socket(AF_INET,SOCK_STREAM, 0);
	if(fd < 0){
		perror("scocket");
	}
	return fd;
}
char* my_ntoa(int ip){
	struct sockaddr_in tmpAddr;
	tmpAddr.sin_addr.s_addr = ip;
	return inet_ntoa(tmpAddr.sin_addr);
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
int connect_port_ip(const int socket,const int ip,const unsigned short port){
	//return 0 if succeed
	
	struct sockaddr_in addr;
	bzero((char*)&addr,sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = ip;
	addr.sin_port=htons(port);
	
	
	if(connect(socket,(struct sockaddr*)&addr,sizeof(addr))){
		perror("connect");
		fprintf(stderr,"target:%s\n",my_ntoa(ip));
		exit(1);
		return 1;
	}
	return 0;
}

int connect_send_close(const int ip,const unsigned short port,const char* buff,const size_t bufflen){
	// return how many bytes sent
	int socket = create_tcpsocket();
	set_linger(socket);
	
	int success = connect_port_ip(socket,ip,port);
	if(success) {
		perror("connect");
		fprintf(stderr,"target:%s\n",my_ntoa(ip));
		exit(1);
		return 1;
	}
	size_t postsend=0,tmpsend;
	while(postsend != bufflen){
		tmpsend = write(socket,buff,bufflen);
		if(tmpsend>0){
			postsend+=tmpsend;
		}else{
			return postsend;
		}
	}
	close(socket);
	return postsend;
}
int connect_send(int* socket,const int ip,const unsigned short port,const char* buff,const size_t bufflen){
	*socket = create_tcpsocket();
	set_linger(*socket);
	connect_port_ip(*socket,ip,port);
	size_t postsend=0,tmpsend;
	while(postsend != bufflen){
		tmpsend = write(*socket,&buff[postsend],bufflen);
		if(tmpsend>0){
			postsend+=tmpsend;
		}else{
			return 0;
		}
	}
	return postsend;
}

int deep_write(const int socket,const void* buff,int length){
	const char* beginbuff = (char*)buff;
	int sendsize = 0,sentbuf = 0;
	
	int flag = 0;
	while(length > 0){
		sendsize = write(socket,&beginbuff[sentbuf],length);
		if(sendsize>0){
			sentbuf += sendsize;
			length -= sendsize;
		}else{
			if(flag == 0){
				fprintf(stderr,"\nsocket:%d ",socket);
				perror("deep_write");
				flag=1;
			}
		}
	}
	if(flag){
		fprintf(stderr,"but %d byte was sent corrctly\n",sentbuf);
	}
	return sentbuf;
}
int deep_read(const int socket,void* buff,int length){
	char* beginbuff = (char*)buff;
	int recvsize = 0,recvbuf = 0;
	
	int flag = 0;
	while(length > 0){
		recvsize = read(socket,&beginbuff[recvbuf],length);
		if(recvsize>0){
			recvbuf += recvsize;
			length -= recvsize;
		}else{
			if(flag==0){
				fprintf(stderr,"socket:%d ",socket);
				perror("deep read");
				flag = 1;
			}
		}
	}
	if(flag){
		fprintf(stderr," ...socket ok\n");
	}
	return recvbuf;
}

void socket_maximize_sndbuf(const int socket){
    socklen_t intsize = sizeof(int);
    int last_good = 0;
    int min, max, avg;
    int old_size;

	//*  minimize!
	avg = intsize;
	setsockopt(socket, SOL_SOCKET, SO_SNDBUF, (void *)&avg, intsize);
	return;
	//*
	
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
void set_nodelay(const int socket){

	int result = setsockopt(socket, IPPROTO_TCP, TCP_NODELAY, (void *)&OK, sizeof(OK));
	if(result < 0){
		perror("set nodelay ");
	}
}

int set_nonblock(const int socket) {
	return fcntl(socket, F_SETFL, O_NONBLOCK);
}

void set_reuse(const int socket){
	int result = setsockopt(socket, SOL_SOCKET, SO_REUSEADDR, (void *)&OK, sizeof(OK));
	if(result < 0){
		perror("set reuse ");
	}
}
void set_keepalive(const int socket){
	int result = setsockopt(socket, SOL_SOCKET, SO_KEEPALIVE, (void *)&OK, sizeof(OK));
	if(result < 0){
		perror("set keepalive ");
	}
}

void set_linger(const int socket){
	return;
	int result = setsockopt(socket, SOL_SOCKET, SO_LINGER, (void *)&OK, sizeof(OK));
	if(result < 0){
		perror("set linger ");
	}
}
int chk_myip(void){

	struct ifreq ifr;
	int fd=socket(AF_INET, SOCK_DGRAM,0);
	ifr.ifr_addr.sa_family = AF_INET;
	strncpy(ifr.ifr_name,"eth0",IFNAMSIZ-1);
	ioctl(fd,SIOCGIFADDR,&ifr);
	close(fd);
	return (int)((struct sockaddr_in*)&ifr.ifr_addr)->sin_addr.s_addr;
}

int my_aton(char* ipaddress){
	struct in_addr tmp_inaddr;
	int ip = 0;
	if(inet_aton(ipaddress,&tmp_inaddr)){
		ip = tmp_inaddr.s_addr;
	}else {
		printf("aton:address invalid\n");
	}
	return ip;
}
  
