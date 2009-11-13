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
#include <sys/select.h>//select,fd
#include <assert.h>//assert

class mulio{
private:
	// socket list
	std::set<int> mSocketList;
	std::deque<int> mActiveSocket;
	std::vector<int> mDeleteSocket;
	std::vector<int> mAddSocket;
	
	int mAcceptSocket;
	int mMaxFd;
	
	pthread_t mythread;
	sem_t sem_active;
	int awaker[2];
	fd_set fds;
	int (*callback)(int);
	
	int calcMaxFd(void);
	int fds_set_all(fd_set* fds);
public:
	void printSocketList(void);
	void SetCallback(int (*cb)(int));
	void SetAcceptSocket(const int socket);
	void SetSocket(const int socket);
	void worker(void);
	void eventloop(void);
	void run(void);
	mulio(void);
	~mulio(void);
};
void* thread_pass(void* Imulio);

#endif /* MULIO */
