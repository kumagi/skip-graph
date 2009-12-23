#ifndef LIBASO
#define LIBASO
#include "assert.h"
#include <sys/types.h>//send
#include <sys/socket.h>//send
#include <sys/epoll.h>//epoll
#include <unistd.h>//pipe
#include <fcntl.h>//O_NONBLOCK

#include <stdio.h>
#include <pthread.h>
#include <semaphore.h>
#include <queue>//std::queue
#include <map>//std::map
#define NDEBUG

#include <stdio.h>//fprintf

#define MAX_WRITE_EPOLL 10
namespace ASOWORKERS{
	void* worker(void* ptr);
	void* poller(void* ptr);
}
class aso_element{
protected:
	const int socket;
	const char* buff;
	char* dbuff;
	int length;
	int postsent;
	bool constFlag;
public:
	
public:
	aso_element(const int s,char* b,int l);
	aso_element(const int s,const char* b,int l);
	int send(void);
	~aso_element(void);
};
enum mode{
	EPOLL,
	WORK,
};
class aso{
protected:
	struct epoll_event ev,events[MAX_WRITE_EPOLL];
	int epollfd;
	std::map<int,std::queue<aso_element*>* > element_list;
	pthread_t* threads,epoller;
	sem_t works;
	std::queue<int> fifo;
public:
	int status;
	enum async_status{
		waiting,
		complete,
	};
public:
	aso(void);
	void send(int socket,char* data,int datalen);
	void send(int socket,const char* data,int datalen);
	void run(int threadnum);
	void working(void);
	void polling(void);
};

#endif// LIBASO
