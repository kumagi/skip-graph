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

class mulio mulio;
class membership_vector myVector;
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
	return NodeList.front();
}

sg_Node* search_node_by_id(long long id){
	std::list<sg_Node*>::iterator it = NodeList.begin();
	while(it != NodeList.end() ){
		if((**it).mId == id){
			return *it;
		}
		++it;
	}
	return NodeList.front();
}
	

void print_usage(void);
void settings_init(void);

enum Op{
	SearchOp,
	RangeOp,
	FoundOp,
	NotfoundOp,
	SetOp,
	LinkOp,
	TreatOp,
	IntroduceOp,
};
enum Left_Right{
	Left,
	Right,
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
	int socket = s,tmpsocket;
	char op;
	int chklen;
	unsigned long long targetid,originid,originvector;
	sg_Node* tmpnode;
	int targetlevel,tmplevel,targetip,originip;
	unsigned short targetport,originport;
	char* buff;
	int bufflen,buffindex;
	char left_or_right;
	
	intkey tmpkey;
	intvalue tmpvalue;
	
	if(settings.verbose>1)
		fprintf(stderr,"socket:%d\n",socket);
	
	int EndFlag = 0,DeleteFlag = 0;
	while(EndFlag == 0){
		chklen = read(socket,&op,1);
		if(chklen <= 0){
			EndFlag = 1;
			break;
		}
		switch(op){
		case SearchOp:
			if(settings.verbose>1){
				fprintf(stderr,"Search\n");
			}
			read(socket,&targetid,8);
			tmpnode = search_node_by_id(targetid);
			tmpkey.Receive(socket);
			read(socket,&targetlevel,4);
			read(socket,&targetip,4);
			read(socket,&targetport,2); 
			printf("received searchkey:%d\n",tmpkey.mKey);
			
			if(tmpkey == tmpnode->mKey){
				//send FoundOP
				if(settings.verbose>1)
					fprintf(stderr,"found\n");
				//prepare buffer
				buffindex = 0;
				bufflen = 1 + tmpnode->mKey.size() + tmpnode->mValue.size();
 				buff = (char*)malloc(bufflen);
				//serialize
				buff[buffindex++] = FoundOp;
				buffindex+=tmpnode->mKey.Serialize(&buff[buffindex]);
				buffindex+=tmpnode->mValue.Serialize(&buff[buffindex]);
				
				connect_send_close(targetip,targetport,buff,bufflen);
				free(buff);
			}else{
				if(tmpkey > tmpnode->mKey){
					//send SearchOP to Rightside
					left_or_right = Right;
					for(;targetlevel>=0;targetlevel--){
						if(tmpnode->mRight[targetlevel].mKey.mValidFlag == 0){
							continue;
						}
						if(tmpnode->mRight[targetlevel].mKey < tmpkey){
							break;
						}
					}
				}else{
					//send SearchOcP to Leftside
					left_or_right = Left;
					for(;targetlevel>=0;targetlevel--){
						if(tmpnode->mLeft[targetlevel].mKey.mValidFlag == 0){
							continue;
						}
						if(tmpnode->mLeft[targetlevel].mKey > tmpkey){
							break;
						}
					}
				}
				if(targetlevel >= 0){
					//start creating message and serialize
					buffindex = 0;
					bufflen = 1+8+tmpkey.size()+4+4+2;//[OP id key level ip port]
					buff = (char*)malloc(bufflen);
					buff[0] = SearchOp;
					buffindex+=1;
					
					if(left_or_right == Right){
						serialize_longlong(buff,&buffindex,tmpnode->mRight[targetlevel].mId);
					}else{
						serialize_longlong(buff,&buffindex,tmpnode->mLeft[targetlevel].mId);
					}
					buffindex+=tmpkey.Serialize(&buff[buffindex]);
					serialize_int(buff,&buffindex,targetlevel);
					serialize_int(buff,&buffindex,targetip);
					serialize_short(buff,&buffindex,targetport);
					
					assert(bufflen == buffindex && "buffsize ok");
					
					if(left_or_right == Right){
						tmpnode->mRight[targetlevel].send(buff,bufflen);
					}else{
						tmpnode->mLeft[targetlevel].send(buff,bufflen);
					}
					free(buff);
				}else{
					//send NotfoundOP
					if(settings.verbose>1)
						fprintf(stderr,"Notfound\n");
					//prepare buffer
					buffindex = 0;
					bufflen = 1 + tmpkey.size() + tmpkey.size();
					buff = (char*)malloc(bufflen);
					//serialize
					buff[buffindex] = NotfoundOp;
					buffindex+=1;
					buffindex+=tmpkey.Serialize(&buff[buffindex]);
					buffindex+=tmpnode->mKey.Serialize(&buff[buffindex]);
					
					//send and cleanup
					connect_send_close(targetip,targetport,buff,bufflen);
					
					free(buff);
				}
			}
			EndFlag = 1;
			break;
		case LinkOp://id,key,originip,originid,originport,level,LorR
			if(settings.verbose>1)
				fprintf(stderr,"LinkOP\n");
			read(socket,&targetid,8);
			tmpnode = search_node_by_id(targetid);
			tmpkey.Receive(socket);
			read(socket,&originip,4);
			read(socket,&originid,8);
			read(socket,&originport,2); 
			read(socket,&targetlevel,4);
			read(socket,&left_or_right,1);
			
			if(left_or_right == Left){
				tmpnode->mLeft[targetlevel].set(socket,tmpkey,originip,originid,originport);
			}else{
				tmpnode->mRight[targetlevel].set(socket,tmpkey,originip,originid,originport);
			}
			mulio.SetSocket(socket);
			EndFlag = 1;
			break;
		case FoundOp:
			tmpkey.Receive(socket);
			tmpvalue.Receive(socket);
			fprintf(stderr,"key:%d found!  value:%d\n",tmpkey.mKey,tmpvalue.mValue);
			break;
		case NotfoundOp:
			tmpkey.Receive(socket);
			fprintf(stderr,"key:%d not found!",tmpkey.mKey);
			tmpkey.Receive(socket);
			fprintf(stderr,"nearest key:%d\n",tmpkey.mKey);
			break;
		case SetOp:
			if(settings.verbose>1)
				fprintf(stderr,"SetOP\n");
			tmpkey.Receive(socket);
			tmpvalue.Receive(socket);
			tmpnode = new sg_Node(tmpkey,tmpvalue);
			NodeList.push_back(tmpnode);
			
			// Build up list with memvership vector
			/*send TreatOp to the some node*/
			
		case TreatOp://targetid,key,originip,originid,originport
			// if you are nearest to origin, connect origin in level 0
			if(settings.verbose>1)
				fprintf(stderr,"TreatOP\n");
			
			read(socket,&targetid,8);
			tmpnode = search_node_by_id(targetid);
			tmpkey.Receive(socket);
			read(socket,&originip,4);
			read(socket,&originid,8);
			read(socket,&originport,2);
			read(socket,&originvector,8);
			
			if(tmpkey == tmpnode->mKey){
				//over write? <- TODO
				if(settings.verbose>1)
					fprintf(stderr,"found\n");
				//prepare buffer
				buffindex = 0;
				bufflen = 1 + tmpnode->mKey.size() + tmpnode->mValue.size();
				buff = (char*)malloc(bufflen);
				//serialize
				buff[buffindex++] = FoundOp;
				buffindex+=tmpnode->mKey.Serialize(&buff[buffindex]);
				buffindex+=tmpnode->mValue.Serialize(&buff[buffindex]);
				
				connect_send_close(targetip,targetport,buff,bufflen);
				free(buff);
			}else{
				tmplevel = MAXLEVEL-1;
				if(tmpkey > tmpnode->mKey){
					//send TreatOP to Rightside
					left_or_right = Right;
					for(;tmplevel>=0;tmplevel--){
						if(tmpnode->mRight[tmplevel].mKey.mValidFlag == 0){
							continue;
						}
						if(tmpnode->mRight[tmplevel].mKey < tmpkey){
							break;
						}
					}
				}else{
					//send TreatOP to Leftside
					left_or_right = Left;
					for(;tmplevel>=0;tmplevel--){
						if(tmpnode->mLeft[tmplevel].mKey.mValidFlag == 0){
							continue;
						}
						if(tmpnode->mLeft[tmplevel].mKey > tmpkey){
							break;
						}
					}
				}
				if(tmplevel >= 0){
					//start creating message and serialize
					buffindex = 0;
					bufflen = 1+8+tmpkey.size()+4+8+2;//[OP id key level ip id port]
					buff = (char*)malloc(bufflen);
					buff[buffindex++] = TreatOp;
					if(left_or_right == Right){
						serialize_longlong(buff,&buffindex,tmpnode->mRight[targetlevel].mId);
					}else{
						serialize_longlong(buff,&buffindex,tmpnode->mLeft[targetlevel].mId);
					}
					buffindex+=tmpkey.Serialize(&buff[buffindex]);
					serialize_int(buff,&buffindex,originip);
					serialize_longlong(buff,&buffindex,originid);
					serialize_short(buff,&buffindex,originport);
					
					assert(bufflen == buffindex && "buffsize ok");
					
					if(left_or_right == Right){
						tmpnode->mRight[tmplevel].send(buff,bufflen);
					}else{
						tmpnode->mLeft[tmplevel].send(buff,bufflen);
					}
					free(buff);
				}else{
					//send LinkOP
					if(settings.verbose>1)
						fprintf(stderr,"send LinkOP\n");
					//prepare
					targetlevel = 0;
					buffindex = 0;
					bufflen = 1+8+tmpkey.size()+4+8+2+4+1;
					buff = (char*)malloc(bufflen);
					//serialize
					buff[buffindex++] = LinkOp;
					serialize_longlong(buff,&buffindex,originid);
					buffindex += tmpnode->mKey.Serialize(&buff[buffindex]);
					serialize_int(buff,&buffindex,settings.myip);
					serialize_longlong(buff,&buffindex,tmpnode->mId);
					serialize_short(buff,&buffindex,settings.useport);
					serialize_longlong(buff,&buffindex,targetlevel);
					buff[buffindex++] = left_or_right;
					connect_send(&tmpsocket,targetip,targetport,buff,bufflen);
					mulio.SetSocket(tmpsocket);
					free(buff);
					
					//prepare
					targetlevel = 0;
					buffindex = 0;
					bufflen = 1+8+tmpkey.size()+4+8+2+4+8;
					buff = (char*)malloc(bufflen);
					//serialize
					buff[buffindex++] = IntroduceOp;
					serialize_longlong(buff,&buffindex,originid);
					buffindex += tmpkey.Serialize(&buff[buffindex]);
					serialize_int(buff,&buffindex,originip);
					serialize_longlong(buff,&buffindex,originid);
					serialize_short(buff,&buffindex,originport);
					serialize_int(buff,&buffindex,targetlevel);
					serialize_longlong(buff,&buffindex,originvector);
					if(left_or_right == Left){
						tmpnode->mRight[0].send(buff,bufflen);
					}else{
						tmpnode->mLeft[0].send(buff,bufflen);
					}
					free(buff);
					
					//search level
					targetlevel = myVector.compare(originvector);
					for(int i=0;i<=targetlevel;i++){
						if(left_or_right == Left){
							tmpnode->mRight[i].set(tmpsocket,tmpkey,targetip,targetid,targetport);
						}else{
							tmpnode->mLeft[i].set(tmpsocket,tmpkey,targetip,targetid,targetport);
						}
					}
					buffindex = 0;
					bufflen = 1+8+tmpkey.size()+4+8+2+4+8;
					buff = (char*)malloc(bufflen);
					//serialize
					buff[buffindex++] = IntroduceOp;
					serialize_longlong(buff,&buffindex,originid);
					buffindex += tmpkey.Serialize(&buff[buffindex]);
					serialize_int(buff,&buffindex,originip);
					serialize_longlong(buff,&buffindex,originid);
					serialize_short(buff,&buffindex,originport);
					serialize_int(buff,&buffindex,targetlevel);
					serialize_longlong(buff,&buffindex,originvector);
					if(left_or_right == Left){
						tmpnode->mRight[targetlevel].send(buff,bufflen);
					}else{
						tmpnode->mLeft[targetlevel].send(buff,bufflen);
					}
					free(buff);
				}
			}
			EndFlag = 1;
			break;
		case IntroduceOp:
			if(settings.verbose>1)
				fprintf(stderr,"TreatOP\n");
			
			read(socket,&targetid,8);
			tmpnode = search_node_by_id(targetid);
			tmpkey.Receive(socket);
			read(socket,&originip,4);
			read(socket,&originid,8);
			read(socket,&originport,2);
			read(socket,&targetlevel,4);
			read(socket,&originvector,8);
			
			if(tmpkey < tmpnode->mKey){
				left_or_right = Left;
			}else {
				left_or_right = Right;
			}
			
			tmplevel = myVector.compare(originvector);
			if(targetlevel < tmplevel){
				for(int i=targetlevel;i<=tmplevel;i++){
					//send LinkOP
					if(settings.verbose>1)
						fprintf(stderr,"send LinkOP\n");
					//prepare
					targetlevel = 0;
					buffindex = 0;
					bufflen = 1+8+tmpkey.size()+4+8+2+4+1;
					buff = (char*)malloc(bufflen);
					//serialize
					buff[buffindex++] = LinkOp;
					serialize_longlong(buff,&buffindex,originid);
					buffindex += tmpnode->mKey.Serialize(&buff[buffindex]);
					serialize_int(buff,&buffindex,settings.myip);
					serialize_longlong(buff,&buffindex,tmpnode->mId);
					serialize_short(buff,&buffindex,settings.useport);
					serialize_longlong(buff,&buffindex,targetlevel);
					buff[buffindex++] = left_or_right;
					connect_send(&tmpsocket,targetip,targetport,buff,bufflen);
					mulio.SetSocket(tmpsocket);
					free(buff);
					if(left_or_right == Left){
						tmpnode->mRight[i].set(tmpsocket,tmpkey,targetip,targetid,targetport);
					}else{
						tmpnode->mLeft[i].set(tmpsocket,tmpkey,targetip,targetid,targetport);
					}
				}
			}
			
			break;
		default:
			printf("error: undefined operation %d.\n",op);
		}
	}
	if(settings.verbose>1)
		printf("socket:%d end\n",socket);
	return DeleteFlag;
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
	int listening = create_tcpsocket();
	set_reuse(listening);
	bind_inaddr_any(listening,settings.useport);
	listen(listening,2048);
	mulio.SetAcceptSocket(listening);
	mulio.SetCallback(main_thread);
	mulio.run();// accept thread
	
	if(settings.verbose>1){
		std::cerr << "start warking as skipgraph server..." << std::endl;
	}
	int buffindex,bufflen;
	char* buff;
	intkey tmpkey(1234);
	intvalue tmpvalue(512);
	
	
	buffindex = 0;
	bufflen = 1+tmpkey.size()+tmpvalue.size();
	buff = (char*)malloc(bufflen);
	
	buff[buffindex++] = SetOp;
	buffindex+=tmpkey.Serialize(&buff[buffindex]);
	buffindex+=tmpvalue.Serialize(&buff[buffindex]);
	
	connect_send_close(settings.targetip,settings.useport,buff,bufflen);
	
	
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
