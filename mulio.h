#ifndef MULIO
#define MULIO
#include <pthread.h>
#include <semaphore.h>

#include <list>
#include <set>
#include <vector>
#include <deque>

#include <arpa/inet.h>

#include <unistd.h>//pipe
#include <fcntl.h>//fcntl
#include <semaphore.h>//sem
#include <sys/epoll.h>//select,fd
#include <assert.h>//assert
#include "mytcplib.h"
#include "lockfree_stack.hpp"

#ifndef DEBUG_MACRO
#define DEBUG_MACRO


//#define DEBUG_MODE
#ifdef DEBUG_MODE
#define DEBUG_OUT(...) fprintf(stderr,__VA_ARGS__)
#define DEBUG(...) __VA_ARGS__
#else
#define NDEBUG
#define DEBUG_OUT(...)
#define DEBUG(...)
#endif

#endif //DEBUG_MACRO

#define MAXFD 16

class mulio{
private:
	// socket list
	std::list<int> mSocketList;
	
	lf_stack<int> mUpdatesocket;
	lf_stack<int> mNewsocket;
	lf_stack<int> mRemovesocket;
	
	lf_stack<int> mActivesocket;
	
	int mAcceptSocket;
	int mEpollfd;
	struct epoll_event ev,events[MAXFD];
	
	int mode_verbose;
	
	pthread_t mythread;
	sem_t sem_active;
	int updater[2];
	int adder[2];
	int remover[2];
	fd_set fds;
	int (*callback)(int);
	sem_t stopper;
	
	
	int mSocket; // passing
	
public:
	void printSocketList(void);
	void SetCallback(int (*cb)(int));
	void SetAcceptSocket(const int socket);
	void SetSocket(const int socket);
	void worker(void);
	void eventloop(void);
	void run(void);
	void setverbose(const int v);
	mulio(void);
	~mulio(void);
};
void* thread_pass(void* Imulio);

#endif /* MULIO */
