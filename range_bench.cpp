#include "thread_pool.hpp"
#include <stdio.h> // fprintf
#include <stdlib.h> // malloc
#include <unistd.h> // sleep,usleep
#include <string.h>
#include <string>
#include <errno.h>
#include <arpa/inet.h> // inet_aton
#include <boost/shared_ptr.hpp>
#include <sys/time.h> // get time of day
#define MAX_RECVBUF_SIZE (256 * 1024 * 1024)

//#define DEBUG_MODE
#ifdef DEBUG_MODE
#define DEBUG(...) fprintf(stderr,__VA_ARGS__)
#else
#define DEBUG(...)
#endif

inline double get_time(void){
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return ((double)(tv.tv_sec) + (double)(tv.tv_usec) * 0.001 * 0.001);
}

int aton(const char* ipaddress){
	struct in_addr tmp_inaddr;
	int ip = 0;
	if(inet_aton(ipaddress,&tmp_inaddr)){
		ip = tmp_inaddr.s_addr;
	}else {
		printf("aton:address invalid\n");
	}
	return ip;
}
int create_tcpsocket(void){
	return socket(AF_INET,SOCK_STREAM, 0);
}
int connect_port_ip(const int socket,const int ip,const unsigned short port){
	//return 0 if succeed
	struct sockaddr_in addr;
	bzero((char*)&addr,sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_addr.s_addr = ip;
	addr.sin_port=port;
	
	if(connect(socket,(struct sockaddr*)&addr,sizeof(addr)) != 0){
		perror("connect");
		return 1;
	}
	return 0;
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

class address{
public:
	const int ip;
	const unsigned short port;
	
	address(void):ip(aton("127.0.0.1")),port(htons(11211)){ }
	address(const int i,const unsigned short p):ip(i),port(p){ }
};


class range_query{
private:
	std::string begin,end;
	int begin_closed,end_closed;
	const address target;
public:
	range_query(void){}
	range_query(const std::string& begin,const std::string& end,const int begin_closed,const int end_closed,const int ip,const unsigned short port):
		begin(begin),end(end),begin_closed(begin_closed),end_closed(end_closed),target(ip,port){}
	 
	bool write_query(const int socket) const {
		std::string query = "rget " + begin + " " + end + " " + (begin_closed?"1 ":"0 ") + (end_closed?"1\r\n":"0\r\n");
		int sentlen= 0, leftbuf=query.length();
		while(leftbuf > 0){
			int chklen = write(socket,&(query.c_str()[sentlen]),leftbuf);
			if(chklen>0){
				sentlen += chklen;
				leftbuf -= chklen;
			}
		}
		return true; 
	}
	range_query& operator=(const range_query& rhs){
		begin = rhs.begin;
		end = rhs.end;
		begin_closed = rhs.begin_closed;
		end_closed = rhs.end_closed;
		return *this;
	}
	const int& ip(void) const{
		return target.ip;
	}
	const unsigned short& port(void) const{
		return target.port;
	}
};
typedef boost::shared_ptr<range_query> node;
 
static int allkey  = 0;
static int cnt = 0;
void send_recv_query(node range){
	int newsocket = create_tcpsocket();
	socket_maximize_rcvbuf(newsocket);
	connect_port_ip(newsocket,range->ip(),range->port());
	
	// send query
	range->write_query(newsocket);
	
	// receive answer
	char* buff = (char*)malloc(512);
	int received=0,checked=0,restsize=512,size=512;
	int endflag = 0;
	while(1){
		int recvsize = read(newsocket,&buff[received],restsize);
		restsize -= recvsize;
		received += recvsize;
		if(restsize == 0){
			restsize = size;
			size *= 2;
			buff = (char*)realloc(buff,size);
		}
		//buff[received] = '\0';
		// wait for receive 'END'
		while(checked <= received - 5){
			if(strncmp(&buff[checked],"END\r\n",5) == 0){
				endflag = 1;
				break;
			}
			checked++;
		}
		if(endflag) break;
	}
	buff[checked+5] = '\0';
	DEBUG("%s\n",buff);
	DEBUG("No%d: %d keys hit\n",++cnt,(received-5)/20);
	allkey += (received-5)/20;
	free(buff);
	close(newsocket);
}

#define QUERY 10
int main(void){
	threadpool<node> threads(32);
	int queries = 0;
	double start = get_time(), end;
	for(int i=0;i<QUERY;i++){
		
		queries++;
		node range = node(new range_query("0","zzz",1,1,aton("133.68.129.212"),htons(11211)));
		threads.go(send_recv_query,range);
		
		queries++;
		range = node(new range_query("0","zzy",1,1,aton("133.68.129.213"),htons(11211)));
		threads.go(send_recv_query,range);
		
		queries++;
		range = node(new range_query("0","zzt",1,1,aton("133.68.129.214"),htons(11211)));
		threads.go(send_recv_query,range);
		queries++;
		range = node(new range_query("0","zzzz",1,1,aton("133.68.129.215"),htons(11211)));
		threads.go(send_recv_query,range);
		queries++;
		range = node(new range_query("0","zzzf",1,0,aton("133.68.129.216"),htons(11211)));
		threads.go(send_recv_query,range);
		queries++;
		range = node(new range_query("0","zzzzz",1,0,aton("133.68.129.217"),htons(11211)));
		threads.go(send_recv_query,range);
		queries++;
		range = node(new range_query("0","zz9",1,0,aton("133.68.129.218"),htons(11211)));
		threads.go(send_recv_query,range);
		queries++;
		range = node(new range_query("0","zzz0",1,1,aton("133.68.129.219"),htons(11211)));
		threads.go(send_recv_query,range);
		queries++;
		range = node(new range_query("0","zzzzl",0,1,aton("133.68.129.220"),htons(11211)));
		threads.go(send_recv_query,range);
		queries++;
		range = node(new range_query("0","zzzz",1,1,aton("133.68.129.221"),htons(11211)));
		threads.go(send_recv_query,range);
		/*
		*/
	}
	threads.wait();
	end = get_time();
	fprintf(stderr,"%3f sec %d key, %f kps  %f qps\n", end-start, allkey, (double)allkey/(end-start), queries/(end-start));
	
	return 0;
}
