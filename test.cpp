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


enum Op{
	SearchOp,
	RangeOp,
	FoundOp,
	NotfoundOp,
	SetOp
};

int main_thread(int s){
	int socket = s;
	char op;
	int chklen;
	unsigned long long targetid;
	sg_Node* tmpnode;
	int targetlevel;
	int targetip;
	unsigned short targetport;
	char* buff;
	int bufflen;
	int buffindex;
	
	intkey tmpkey;
	intvalue tmpvalue;
	
	//for serialize
	long long* plong;
	int* pint;
	unsigned short* pushort;
	
	
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
			read(socket,&targetid,8);
			tmpnode = search_node_by_id(targetid);
			tmpkey.Receive(socket);
			read(socket,&targetlevel,4);
			read(socket,&targetip,4);
			read(socket,&targetport,2); 
			
			if(tmpkey == tmpnode->mKey){
				//send FoundOP
				buffindex = 0;
				bufflen = 1 + tmpnode->mKey.size() + tmpnode->mValue.size();
				buff = (char*)malloc(bufflen);
				
				buff[buffindex] = NotfoundOp;
				buffindex+=1;
				
				buffindex+=tmpnode->mKey.Serialize(&buff[buffindex]);
				buffindex+=tmpnode->mValue.Serialize(&buff[buffindex]);
				
				connect_send_close(targetip,targetport,buff,bufflen);
			}else{
				if(tmpkey > tmpnode->mKey){
					//send SearchOP to Rightside
					for(;targetlevel>=0;targetlevel--){
						if(tmpnode->mRight[targetlevel].mKey < tmpkey){
							break;
						}
					}
				}else{
					//send SearchOP to Leftside
					for(;targetlevel>=0;targetlevel--){
						if(tmpnode->mLeft[targetlevel].mKey > tmpkey){
							break;
						}
					}
				}
				if(targetlevel > 0){
					//start creating message and serialize
					buffindex = 0;
					bufflen = 1+8+tmpkey.size()+4+4+2;//[OP id key level ip port]
					buff = (char*)malloc(bufflen);
					buff[0] = SearchOp;
					buffindex+=1;
					plong = (long long*)&buff[buffindex];
					*plong = tmpnode->mRight[targetlevel].mId;
					buffindex+=sizeof(long long);
					
					buffindex+=tmpkey.Serialize(&buff[buffindex]);
					
					pint = (int*)&buff[buffindex];
					*pint = targetlevel;
					buffindex+=sizeof(int);
					
					pint = (int*)&buff[buffindex];
					*pint = targetip;
					buffindex+=sizeof(int);
					
					pushort = (unsigned short*)&buff[buffindex];
					*pushort = targetport;
					buffindex+=sizeof(unsigned short);
					
					assert(bufflen == buffindex-1 && "buffsize ok");
					
					tmpnode->mRight[targetlevel].send(buff,bufflen);
					
				}else{
					//send NotfoundOP
					buffindex = 0;
					bufflen = 1 + tmpkey.size() + tmpkey.size();
					buff = (char*)malloc(bufflen);
					
					buff[buffindex] = NotfoundOp;
					buffindex+=1;
					
					buffindex+=tmpkey.Serialize(&buff[buffindex]);
					buffindex+=tmpnode->mKey.Serialize(&buff[buffindex]);
					
					connect_send_close(targetip,targetport,buff,bufflen);
				}
				
				
			}
			
			break;
		case FoundOp:
			tmpkey.Receive(socket);
			tmpvalue.Receive(socket);
			printf("key:%d found!  value:%d\n",tmpkey.mKey,tmpvalue.mValue);
			break;
		case NotfoundOp:
			tmpkey.Receive(socket);
			printf("key:%d not found!",tmpkey.mKey);
			tmpkey.Receive(socket);
			printf("nearest key:%d\n",tmpkey.mKey);
			
			break;
		case SetOp:
			tmpkey.Receive(socket);
			tmpvalue.Receive(socket);
			tmpnode = new sg_Node(tmpkey,tmpvalue);
			NodeList.push_back(tmpnode);
			
			// Build up list with memvership vector
			
			
			break;
			case 
		default:
			printf("error: undefined operation %d.\n",op);
		}
	}
	if(settings.verbose>1)
		printf("socket:%d end\n",socket);
	return 0;
}
int main(void){
	char* buff;
	int buffindex,bufflen;
	
	//for serialize
	long long* plong;
	int* pint;
	unsigned short* pushort;
	
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
	intkey tmpkey(5);

	settings.myip = chk_myip();
	
	
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
	
	buffindex = 0;
	bufflen = 1+8+tmpkey.size()+4+4+2;//[OP id key level ip port]
	buff = (char*)malloc(bufflen);
	buff[0] = SearchOp;
	buffindex+=1;
	plong = (long long*)&buff[buffindex];
	*plong = tmpnode->mRight[targetlevel].mId;
	buffindex+=sizeof(long long);
					
	buffindex+=tmpkey.Serialize(&buff[buffindex]);
					
	pint = (int*)&buff[buffindex];
	*pint = targetlevel;
	buffindex+=sizeof(int);
					
	pint = (int*)&buff[buffindex];
	*pint = targetip;
	buffindex+=sizeof(int);
					
	pushort = (unsigned short*)&buff[buffindex];
	*pushort = targetport;
	buffindex+=sizeof(unsigned short);
					
	assert(bufflen == buffindex-1 && "buffsize ok");
					
	tmpnode->mRight[targetlevel].send(buff,bufflen);
	
	
	mulio.eventloop();
	
	return 0;
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
