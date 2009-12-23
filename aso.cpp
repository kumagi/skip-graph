#include "aso.hpp"

void* ASOWORKERS::poller(void* ptr){
	aso* aso_p = (aso*)ptr;
	aso_p->polling();
	return NULL;
}
void* ASOWORKERS::worker(void* ptr){
	aso* aso_p = (aso*)ptr;
	aso_p->working();
	return NULL;
}

aso_element::aso_element(const int s,char* b,int l):socket(s),buff(b),length(l){
	dbuff = b;
	assert(s!=0);
	assert(b!=NULL);
	assert(length!=0);
	postsent = 0;
	constFlag = false;
}
aso_element::aso_element(const int s,const char* b,int l):socket(s),buff(b),length(l){
	assert(s!=0);
	assert(b!=NULL);
	assert(length!=0);
	postsent = 0;
	constFlag = true;
}
int aso_element::send(void){
	int newsent;
	while(this->length>0){
		newsent = ::send(this->socket,&this->buff[this->postsent],this->length,MSG_DONTWAIT);
		//fprintf(stderr,"buff:[%s]\n",&this->buff[this->postsent]);
		if(newsent > 0){
			this->postsent += newsent;
			this->length -= newsent;
		}else{
			break;
		}
	}
	if(this->length == 0){
		return 1;
	}else {
		return 0;
	}
}
aso_element::~aso_element(void){
	if(constFlag == false){
		fprintf(stderr,"deleting not const\n");
		if(this->buff != NULL){
			free(this->dbuff);
			this->dbuff = NULL;
		}
	}
}

aso::aso(void){
	epollfd = epoll_create(MAX_WRITE_EPOLL);
	if(epollfd == -1){
		perror("epoll_create");
	}
	sem_init(&works,0,0);
	status = complete;
}

void aso::send(int socket,char* data,int datalen){
	send(socket,static_cast<const char*>(data),datalen);
}
void aso::send(int socket,const char* data,int datalen){
	std::map<int,std::queue<aso_element*>* >::iterator elementIt;
	std::queue<aso_element*>* queue;
	status = waiting;
	elementIt = element_list.find(socket);
	if(elementIt == element_list.end()){
		//fprintf(stderr,"new queue created\n");
		queue = new std::queue<aso_element*>;
		queue->push(new aso_element(socket,data,datalen));
		element_list.insert(std::pair<int,std::queue<aso_element*>* >(socket, queue));
		ev.events = EPOLLOUT | EPOLLONESHOT | EPOLLET;
		ev.data.fd = socket;
		if (epoll_ctl(epollfd, EPOLL_CTL_ADD, socket, &ev) == -1) {
			perror("epoll_addsocket");
		}
		//fprintf(stderr,"append socket:%d\n",socket);
	}else{
		queue = elementIt->second;
		queue->push(new aso_element(socket,data,datalen));
	}
}
void aso::run(int threadnum){
	pthread_create(&this->epoller, NULL, ASOWORKERS::poller, this);
	this->threads = (pthread_t*)malloc(threadnum * sizeof(pthread_t));
	for(int i=0;i<threadnum;i++){
		pthread_create(&threads[i], NULL, ASOWORKERS::worker, this);
	}
}
void aso::working(void){
	int socket;
	std::map<int,std::queue<aso_element*>* >::iterator elementIt;
	std::queue<aso_element*>* queue;
	while(1){
		sem_wait(&works);
		socket = fifo.front();
		fifo.pop();
		elementIt = element_list.find(socket);
		assert(elementIt != element_list.end());
		queue = elementIt->second;
		do{
			if(queue->front()->send() == 1){
				delete &*queue->front();
				queue->pop();
				if(queue->empty()){
					delete queue;
					element_list.erase(elementIt);
					epoll_ctl(this->epollfd, EPOLL_CTL_DEL, socket, &ev);
					if (element_list.empty()){
						status = complete;
					}
					break;
				}
			}else {
				//more data rest but cannot send
				ev.events = EPOLLOUT | EPOLLONESHOT | EPOLLET;
				ev.data.fd = socket;
				epoll_ctl(this->epollfd, EPOLL_CTL_MOD, socket, &ev);
				break;
			}
		}while(!queue->empty());
	}
}
void aso::polling(void){
	int nfds;
	while(1){
		//fprintf(stderr,"poll!\n");
		nfds = epoll_wait(epollfd, this->events, MAX_WRITE_EPOLL, -1);
		if (nfds == -1) {
			perror("epoll_wait");
		}
		for(int i=0;i<nfds;i++){
			fifo.push(this->events[i].data.fd);
			//fprintf(stderr,"post! %d  queue_size:%d\n",this->events[i].data.fd,fifo.size());
			sem_post(&works);
		}
	}
}
