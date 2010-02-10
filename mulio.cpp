#include "mulio.h"

void mulio::printSocketList(void){
	std::list<int>::iterator it = mSocketList.begin();
	while( it != mSocketList.end() ){
		fprintf(stderr,"%d ",(*it));
		++it;
	}
}
void mulio::setverbose(int v){
	mode_verbose = v;
}
void mulio::SetCallback(int (*cb)(const int)){
	callback = cb;
}

void mulio::SetAcceptSocket(const int socket){
	mAcceptSocket = socket;
}

void mulio::SetSocket(const int socket){
	set_reuse(socket);
	set_nodelay(socket);
	socket_maximize_rcvbuf(socket);
	socket_maximize_sndbuf(socket);
	mNewsocket.push(socket);
	write(adder[1],"",1);
	DEBUG_OUT("mulio::SetSocket $%d$ ",socket);
}

void mulio::worker(void){
	fd_set fds_clone;
	char dummy;
	
	int flag=fcntl(mAcceptSocket, F_GETFL, 0);
	if(flag < 0){
		perror("fcntl(GET) error");
	}
	if(fcntl(mAcceptSocket, F_SETFL, flag|O_NONBLOCK)<0){
		perror("fcntl(NONBLOCK) error");
	}
	
	ev.events = EPOLLIN | EPOLLET;
	ev.data.fd = mAcceptSocket;
	epoll_ctl(mEpollfd, EPOLL_CTL_ADD, mAcceptSocket, &ev);
   
	
	while(1){
		memcpy(&fds_clone,&fds,sizeof(fds));
		
		// wait for changes
		if(mode_verbose){
			DEBUG_OUT("\n\nwaiting ... ");
		}
		int nfds = epoll_wait(mEpollfd, events, MAXFD,-1);
		
		if(mode_verbose){
			DEBUG_OUT("detect! %d sockets\n",nfds);
			if(nfds<0){
				perror("epoll");
			}
		}
		for(int i = 0; i < nfds; i++){
			if(events[i].data.fd == updater[0]){ // update fd
				while(1){
					int flag = read(updater[0],&dummy,1);
					if(flag <=0) break;
				}
				while(!mUpdatesocket.empty()){
				
					// while(mUpdatesocket == 0);
					int tmpsock;
					mUpdatesocket.atomic_pop(&tmpsock);
					
				
					ev.data.fd = tmpsock;
					ev.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
					epoll_ctl(mEpollfd, EPOLL_CTL_MOD, tmpsock, &ev);
				
					if(mode_verbose){
						DEBUG_OUT("reset (%d) done %d/%d\n",tmpsock,i+1,nfds);
					}
				}
			}else if(events[i].data.fd == adder[0]){ // add fd
				while(1){
					int flag = read(adder[0],&dummy,1);
					if(flag <=0) break;
				}
				while(!mNewsocket.empty()){
					// add
					int tmpsock;
					mNewsocket.atomic_pop(&tmpsock);
					
					if(mode_verbose){
						DEBUG_OUT("add socket %d to [",tmpsock);
						printSocketList();
						DEBUG_OUT("]...done %d/%d\n",i+1,nfds);
					}
					
					mSocketList.push_back(tmpsock);
					mSocketList.sort();
					mSocketList.unique();
					
					ev.data.fd = tmpsock;
					ev.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
					epoll_ctl(mEpollfd, EPOLL_CTL_ADD, tmpsock, &ev);
					
				}
			}else if(events[i].data.fd == remover[0]){ // delete fd
				while(1){
					int flag = read(remover[0],&dummy,1);
					if(flag <=0) break;
				}
				while(!mRemovesocket.empty()){
					// remove
					int tmpsock;
					mRemovesocket.atomic_pop(&tmpsock);
					
					epoll_ctl(mEpollfd, EPOLL_CTL_DEL, tmpsock, NULL);
					
					if(mode_verbose){
						DEBUG_OUT("remove socket %d from [",tmpsock);
						printSocketList();
						DEBUG_OUT("]...done %d/%d\n",i+1,nfds);
					}
					mSocketList.remove(tmpsock);
				}
			}else if(events[i].data.fd == mAcceptSocket){ // accept
				if(mode_verbose){
					DEBUG_OUT("muilo::accept{");
				}
				do{
					sockaddr_in clientaddr;
					socklen_t addrlen = sizeof(sockaddr_in);
					int newsocket = accept(mAcceptSocket, (struct sockaddr*)&clientaddr, &addrlen);
					
					if(newsocket == -1){
						if (errno == EAGAIN || errno == EWOULDBLOCK) {
							break;
						} else if (errno == EMFILE) {
							perror("accept() emfile");
						} else {
							perror("accept()");
						}
						break;
					}
					if(mode_verbose){
						DEBUG_OUT("socket:$%d$ from %s...",newsocket, my_ntoa((int)clientaddr.sin_addr.s_addr));
					}
					set_reuse(newsocket);
					set_nodelay(newsocket);
					socket_maximize_rcvbuf(newsocket);
					socket_maximize_sndbuf(newsocket);
					mSocketList.push_back(newsocket);
					mSocketList.sort();
				
					ev.data.fd = newsocket;
					ev.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
					epoll_ctl(mEpollfd, EPOLL_CTL_ADD, newsocket, &ev);
					if(mode_verbose){
						DEBUG_OUT("ok\n");
					}
				}while(1);
				if(mode_verbose){
					DEBUG_OUT("}accept %d/%d\n",i+1,nfds);
				}
			}else{// read
				if(mode_verbose){
					DEBUG_OUT("mulio::activate %d\n", events[i].data.fd);
				}
				
				mActivesocket.push(events[i].data.fd);
				
				int semvalue;
				
				if(mode_verbose){
					sem_getvalue(&stopper,&semvalue);
					DEBUG_OUT("semaphore!! pushing %d $$$[ %d ->",events[i].data.fd,semvalue);
				}
				
				sem_post(&stopper);
				
				if(mode_verbose){
					sem_getvalue(&stopper,&semvalue);
					DEBUG_OUT("%d ]$$$ UP\n",semvalue);
				}
			}
		}
	}
}
void mulio::eventloop(void){
	int socket,deleteflag;
	while(1){
		int semvalue;
		sem_getvalue(&stopper,&semvalue);
		if(mode_verbose){
			sem_getvalue(&stopper,&semvalue);
			fprintf(stderr,"semaphore!! ###[ %d ->",semvalue);
		}
		sem_wait(&stopper);
		
		mActivesocket.atomic_pop(&socket);
		if(mode_verbose){
			sem_getvalue(&stopper,&semvalue);
			fprintf(stderr,"%d ]### receive %d\n",semvalue,socket);
		}
		if(socket == 0){
			assert(!"arienai!!!");
		}
		
		// do work
		
		deleteflag = (*callback)(socket);
		
		if( deleteflag == 0 ){
			mUpdatesocket.push(socket);
			write(updater[1],"",1);
		}else {
			mRemovesocket.push(socket);
			write(remover[1],"",1);
		}
	}
}
	
void mulio::run(void){
	assert(mAcceptSocket && "set the Listening Socket");
	assert(callback != NULL && "set the callback function");
	pthread_create(&mythread,NULL,thread_pass,this);
}
mulio::mulio(void){
	pipe(adder);
	pipe(remover);
	pipe(updater);
	
	mEpollfd = epoll_create(255);
	if(mEpollfd < 0){
		perror("epoll create");
	}
	set_nonblock(updater[0]);
	ev.events = EPOLLIN | EPOLLET;
	ev.data.fd = updater[0];
	if(epoll_ctl(mEpollfd, EPOLL_CTL_ADD, updater[0], &ev) == -1){
		perror("epoll_ctl: updater");
	}
	set_nonblock(adder[0]);
	ev.events = EPOLLIN | EPOLLET;
	ev.data.fd = adder[0];
	if(epoll_ctl(mEpollfd, EPOLL_CTL_ADD, adder[0], &ev) == -1){
		perror("epoll_ctl: adder");
	}
	set_nonblock(remover[0]);
	ev.events = EPOLLIN | EPOLLET;
	ev.data.fd = remover[0];
	if(epoll_ctl(mEpollfd, EPOLL_CTL_ADD, remover[0], &ev) == -1){
		perror("epoll_ctl: remover");
	}
	
	mSocketList.clear();
	mode_verbose = 0;
	
	mUpdatesocket.clear();
	mNewsocket.clear();
	mRemovesocket.clear();
	
	
	sem_init(&stopper ,0 ,0);
	
}
mulio::~mulio(void){
	close(adder[0]);
	close(adder[1]);
	close(updater[0]);
	close(updater[1]);
	close(remover[0]);
	close(remover[1]);
	std::list<int>::iterator it = mSocketList.begin();
	while( it != mSocketList.end() ){
		close(*it);
		++it;
	}
	pthread_cancel(mythread);
}
void* thread_pass(void* Imulio){
	class mulio* Imuliop=(class mulio*)Imulio;
	Imuliop->worker();
	return NULL;
}
	/*
	  class CHoge {
	  private:
	  void ChildThread() { while( 1 ); }
	  friend int child_thread( void* pThis );
	  };

	  namespace {
	  int child_thread( void* pThis ) { ( (CHoge*) pThis )->ChildThread(); }
	  }//namespace

	  CHoge hoge;
	  beginthread( child_thread, &hoge );
	*/
