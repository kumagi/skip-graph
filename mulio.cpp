#include "mulio.h"

void mulio::printSocketList(void){
	std::set<int>::iterator it = mSocketList.begin();
	while( it != mSocketList.end() ){
		fprintf(stderr,"%d ",(*it));
		++it;
	}
}
int mulio::calcMaxFd(void){
	int max=0;
	if( *(mSocketList.end()) > max ){
		max =  *mSocketList.end();
	}
	if( mAcceptSocket > max ){
		max = mAcceptSocket;
	}
	if( awaker[0] > max ){
		max = awaker[0];
	}
	if( awaker[1] > max ){
		max = awaker[1];
	}
	return max;
}
int mulio::fds_set_all(fd_set* fds){
	FD_ZERO(fds);
	FD_SET(mAcceptSocket,fds);
	FD_SET(awaker[0],fds);
	
	int max;
	if(!mSocketList.empty()){
		std::set<int>::iterator it = mSocketList.begin();
		while( it != mSocketList.end() ){
			FD_SET( *it, fds );
			++it;
		}
		max = *it;
	}else {
		max = 0;
	}
	max = max > mAcceptSocket ? max : mAcceptSocket; 
	max = max > awaker[0] ? max : awaker[0];
	return max;
}
void mulio::SetCallback(int (*cb)(const int)){
	callback = cb;
}
void mulio::SetAcceptSocket(const int socket){
	mAcceptSocket = socket;
	if( socket > mMaxFd ){
		mMaxFd = socket;
	}
}
void mulio::SetSocket(const int socket){
	mAddSocket.push_back(socket);
	if( socket > mMaxFd ){
		mMaxFd = socket;
	}
	printf("setsocket:%d\n",socket);
	write(awaker[1],"",1);
}
void mulio::worker(void){
	fd_set fds_clone;
	mMaxFd = fds_set_all(&fds);
	
	while(1){
		memcpy(&fds_clone,&fds,sizeof(fds));
		select(mMaxFd+1, &fds_clone, NULL ,NULL ,NULL);
		if(FD_ISSET(awaker[0],&fds_clone)){ // rebuild fds and mSocketList
			char dummy;
			read(awaker[0],&dummy,1);
				
			// delete by DeleteList
			for(unsigned int i=0; i < mDeleteSocket.size(); i++){
				FD_CLR(mDeleteSocket[i],&fds);
				mSocketList.erase(mDeleteSocket[i]);
				if( mDeleteSocket[i] == mMaxFd ){
					mMaxFd = calcMaxFd();
				}
			}
			mDeleteSocket.clear();
			// add by AddList
			for(unsigned int i=0; i < mAddSocket.size(); i++){
				FD_SET(mAddSocket[i],&fds);
				mSocketList.erase(mAddSocket[i]);
				mSocketList.insert(mAddSocket[i]);
				if( mAddSocket[i] > mMaxFd ){
					mMaxFd = mAddSocket[i];
				}
			}
			mAddSocket.clear();
		}
		if(FD_ISSET(mAcceptSocket,&fds_clone)){ // accept
			
			sockaddr_in clientaddr;
			socklen_t addrlen = sizeof(sockaddr_in);
			int newsocket = accept(mAcceptSocket, (struct sockaddr*)&clientaddr, &addrlen);
			mSocketList.insert(newsocket);
			FD_SET( newsocket ,&fds );
			//printf("newsocket:%d\n",newsocket);
			if( mMaxFd < newsocket){
				mMaxFd = newsocket;
			}
		}else { // read
			std::set<int>::iterator it = mSocketList.begin();
			while( it != mSocketList.end() ){
				if( FD_ISSET(*it,&fds_clone )){
					FD_CLR(*it, &fds);
					mActiveSocket.push_back(*it);
					sem_post(&sem_active);
				}
				++it;
			}
		}
	}
}
void mulio::eventloop(void){
	int socket,deleteflag;
	while(1){
		sem_wait(&sem_active);
		/*
		fprintf(stderr,"All sockets ");
		printSocketList();
		fprintf(stderr,"\n");
		fprintf(stderr,"socket [ ");
		for(unsigned int i=0;i<mActiveSocket.size();i++){
			fprintf(stderr,"%d ",mActiveSocket[i]);
		}
		fprintf(stderr,"] in que\n");
		//*/
		socket = mActiveSocket[0];
		mActiveSocket.pop_front();
		
		deleteflag = (*callback)(socket);
		
		if( deleteflag == 0 ){
			mAddSocket.push_back( socket );
			write(awaker[1],"",1);
		}else {
			mDeleteSocket.push_back( socket );
			write(awaker[1],"",1);
		}
	}
}
	
void mulio::run(void){
	assert(mAcceptSocket && "set the Listening Socket");
	assert(callback != NULL && "set the callback function");
	pthread_create(&mythread,NULL,thread_pass,this);
}
mulio::mulio(void){
	pipe(awaker);
	mSocketList.clear();
	sem_init(&sem_active,0,0);
	mMaxFd=awaker[0];
}
mulio::~mulio(void){
	close(awaker[0]);
	close(awaker[1]);
	std::set<int>::iterator it = mSocketList.begin();
	while( it != mSocketList.end() ){
		close(*it);
		++it;
	}
	pthread_cancel(mythread);
	sem_destroy(&sem_active);
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
