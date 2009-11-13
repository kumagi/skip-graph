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

typedef sg_node<intkey,intvalue> sg_Node;


class membership_vector MembershipVector;
std::list<sg_Node*> NodeList;

template<typename KeyType>
sg_Node* search_node_by_key(KeyType key){
	std::list<sg_Node*>::iterator it = NodeList.begin();
	while(it != NodeList.end() ){
		if((**it).mKey == key){
			return *it;
		}
		++it;
	}
	return NULL;
}

sg_Node* search_node_by_id(long long id){
	std::list<sg_Node*>::iterator it = NodeList.begin();
	while(it != NodeList.end() ){
		if((**it).mId == id){
			return *it;
		}
		++it;
	}
	return NULL;
}
	

void print_usage(void);
void settings_init(void);

enum Op{
	SearchOp,
	RangeOp,
	FoundOp,
	NotfoundOp,
	SetOp
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
	sg_Node* tmpnode;
	
	intkey tmpkey;
	intvalue tmpvalue;
	
	if(settings.verbose>1)
		fprintf(stderr,"socket:%d\n",socket);
	
	int EndFlag = 0;
	while(EndFlag == 0){
		chklen = read(socket,&op,1);
		if(chklen <= 0){
			EndFlag = 1;
			break;
		}
		switch(op){
		case FoundOp:
			fprintf(stderr,"FoundOp\n");
			tmpkey.Receive(socket);
			tmpvalue.Receive(socket);
			fprintf(stderr,"key:%d found!  value:%x\n",tmpkey.mKey,tmpvalue.mValue);
			exit(0);
			break;
		case NotfoundOp:
			fprintf(stderr,"NotfoundOp\n");
			tmpkey.Receive(socket);
			fprintf(stderr,"key:%d not found!",tmpkey.mKey);
			tmpkey.Receive(socket);
			fprintf(stderr,"nearest key:%d\n",tmpkey.mKey);
			exit(0);
			break;
		case SetOp:
			fprintf(stderr,"SetOp\n");
			tmpkey.Receive(socket);
			tmpvalue.Receive(socket);
			tmpnode = new sg_Node(tmpkey,tmpvalue);
			NodeList.push_back(tmpnode);
			
			// Build up list with memvership vector
			break;
		default:
			fprintf(stderr,"error: undefined operation %d.\n",op);
		}
	}
	if(settings.verbose>1)
		fprintf(stderr,"socket:%d end\n",socket);
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
	settings.myip = chk_myip();
	
	
	// set accepting thread
	class mulio mulio;
	int listening = create_tcpsocket();
	set_reuse(listening);
	bind_inaddr_any(listening,settings.useport);
	listen(listening,2048);
	mulio.SetAcceptSocket(listening);
	mulio.SetCallback(main_thread);
	mulio.run();// accept thread
	
	if(settings.verbose>1){
		printf("start warking as skipgraph server...\n");
	}
	int buffindex,bufflen,sendlen;
	char* buff;
	intkey tmpkey(-2147483648);
	intvalue tmpvalue;
	
	long long* plong;
	int* pint;
	unsigned short* pushort;
	int targetlevel = MAXLEVEL-1;
	
	
	
	buffindex = 0;
	bufflen = 1+8+tmpkey.size()+4+4+2;//[OP id key level ip port]
	buff = (char*)malloc(bufflen);
	buff[0] = SearchOp;
	buffindex+=1;
	plong = (long long*)&buff[buffindex];
	*plong = 0;
	buffindex+=sizeof(long long);
					
	buffindex+=tmpkey.Serialize(&buff[buffindex]);
					
	pint = (int*)&buff[buffindex];
	*pint = targetlevel;
	buffindex+=sizeof(int);
					
	pint = (int*)&buff[buffindex];
	*pint = settings.myip;
	buffindex+=sizeof(int);
					
	pushort = (unsigned short*)&buff[buffindex];
	*pushort = settings.useport;
	buffindex+=sizeof(unsigned short);
					
	assert(bufflen == buffindex && "buffsize ok");
	
	sendlen = connect_send_close(settings.targetip,settings.useport,buff,bufflen);
	
	fprintf(stderr,"send:%d bytes\n",sendlen);
	
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