#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

#include <iostream>

#include <list>
#include <set>
#include <vector>
#include <deque>

#include <sys/time.h>//gettimeofday
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <arpa/inet.h>

#include <assert.h>//assert
#include "mulio.h"
#include "mytcplib.h"
#include "skipgraph.h"

struct settings{
	unsigned short useport;
	int targetip;
	int verbose;
	int threads;
}settings;
const char keyop = 7;

void print_usage(void);
void settings_init(void);


enum Op{
	SearchOp,
	RangeOp,
	FoundOp,
	NotfoundOp
};

//Receive the key
int receive_key(int socket,size_t* keylength,char** key){
	size_t postread = 0,tmplen;
	read(socket,keylength,4);
	*key = (char*)malloc((unsigned int)*keylength+1);
	while(postread != *keylength){
		if((tmplen= read(socket,&(*key)[postread],*keylength))){
			postread+=tmplen;
		}else{
			return 1;
		}
	}
	*key[*keylength+1] = '\0';
	return 0;
}
int main_thread(int s){
	int socket = s;
	char op;
	int chklen;
	if(settings.verbose>1)
		printf("socket:%d\n",socket);
	
	int EndFlag = 0;
	while(EndFlag == 0){
		chklen = read(socket,&op,1);
		if(chklen <= 0){
			EndFlag = 1;
			break;
		}
		switch(op){
		case SearchOp:
			
			
			break;
		case FoundOp:
			break;
		case NotfoundOp:
			break;
		default:
			printf("error: undefined operation %d.\n",op);
		}
	}
	if(settings.verbose>1)
		printf("socket:%d end\n",socket);
	return 0;
}


int main(int argc,char** argv){
	srand(sysrand());
	char c;
	settings_init();
	while ((c = getopt(argc, argv, "a:t:vp:h")) != -1) {
		switch (c) {
		case 'a':// target addresss
			settings.targetip = my_aton(optarg);
			break;
		case 't'://number of threads
			settings.threads = atoi(optarg);
			break;
		case 'v':
			settings.verbose++;
			break;
		case 'p':
			settings.useport = atoi(optarg);
			break;
		case 'h':
			print_usage();
			exit(0);
			break;
		}
	}
	
	class membership_vector mv;
	
	if(settings.targetip != 0){ // join to the skip graph
		if(settings.verbose>1){
			struct in_addr tmp_inaddr;
			tmp_inaddr.s_addr=settings.targetip;
			printf("send to %s:%d\n\n",inet_ntoa(tmp_inaddr),settings.useport);
		}
		//mKeyList.insert(new node("dummy"));
		
	}else{
		
	}
	// set accepting thread
	class mulio mulio;
	int listening = create_tcpsocket();
	set_reuse(listening);
	bind_inaddr_any(listening,settings.useport);
	listen(listening,2048);
	mulio.SetAcceptSocket(listening);
	mulio.SetCallback(main_thread);
	mulio.run();// accept thread
	mulio.eventloop();
}

void print_usage(void){
	std::cout << "-a [xxx.xxx.xxx.xxx]:target IP" << std::endl;
	std::cout << "-t [x]              :number of threads" << std::endl;
	std::cout << "-v                  :verbose mode" << std::endl;
	std::cout << "-p [x]              :target port" << std::endl;
	std::cout << "-h                  :print this message" << std::endl;
}
void settings_init(void){
	settings.useport = 10005;
	settings.targetip = 0;
	settings.verbose = 3;
	settings.threads = 4;
}
