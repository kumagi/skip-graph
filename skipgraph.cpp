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

void print_nodelist(void){
	std::list<sg_Node*>::iterator it = NodeList.begin();
	while(it != NodeList.end() ){
		//if((*it)->mId != (*it)->pPointer.mId){printf("not sanity\n");}
		printf("%lld: key=%d  value=%d  left=%d right=%d\n",(*it)->mId,(*it)->mKey.mKey,(*it)->mValue.mValue,(*it)->mLeft[0].mKey.mKey,(*it)->mRight[0].mKey.mKey);
		++it;
	}
}


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

int send_with_connect(sg_neighbor<intkey>& ptr,char* buff,int bufflen){
	int sendsize;
	if(ptr.mSocket != 0){
		sendsize = write(ptr.mSocket,buff,bufflen);
		if(sendsize<=0){
			fprintf(stderr,"send_with_connect:error\n");
		}
	}else{
		if(ptr.mFlagValid){
			sendsize = connect_send(&ptr.mSocket,ptr.mAddress.ip,ptr.mAddress.port,buff,bufflen);
			ptr.mFlagValid = 1;
			mulio.SetSocket(ptr.mSocket);
			return sendsize;
		}else{
			return 0;
		}
	}
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
		fprintf(stderr,"socket:%d ",socket);
	
	int EndFlag = 0,DeleteFlag = 0;
	while(EndFlag == 0){
		chklen = read(socket,&op,1);
		if(chklen <= 0){
			fprintf(stderr," close\n");
			close(socket);
			EndFlag = 1;
			DeleteFlag = 1;
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
			fprintf(stderr,"received searchkey:%d mykey:%d\n",tmpkey.mKey,tmpnode->mKey.mKey);
			
			if(tmpkey == tmpnode->mKey){
				//send FoundOP
				if(settings.verbose>1)
					fprintf(stderr,"found in ID:%lld\n",tmpnode->mId);
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
				fprintf(stderr,"%d : %d ?\n",tmpkey.mKey, tmpnode->mKey.mKey);
				if(tmpkey > tmpnode->mKey){
					//send SearchOP to Rightside
					left_or_right = Right;
					for(;targetlevel>=0;targetlevel--){
						fprintf(stderr,"%d > %d ?\n",tmpnode->mRight[targetlevel].mKey.mKey , tmpkey.mKey);
						if(tmpnode->mRight[targetlevel].mKey.mValidFlag == 0){
							continue;
						}
						fprintf(stderr,"%d > %d ?\n",tmpnode->mRight[targetlevel].mKey.mKey , tmpkey.mKey);
						if(tmpnode->mRight[targetlevel].mKey <= tmpkey){
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
						if(tmpnode->mLeft[targetlevel].mKey >= tmpkey){
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
						send_with_connect(tmpnode->mRight[targetlevel],buff,bufflen);
					}else{
						send_with_connect(tmpnode->mLeft[targetlevel],buff,bufflen);
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
				fprintf(stderr,"LinkOP ");
			
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
			fprintf(stderr,"target:%d from:%d   targetlevel:%d",tmpnode->mKey.mKey,tmpkey.mKey,targetlevel);
			if(left_or_right == Left){
				fprintf(stderr,"  in left side\n");
			}else{
				fprintf(stderr,"  in right side\n");
			}
			mulio.SetSocket(socket);
			EndFlag = 1;
			break;
		case FoundOp:
			tmpkey.Receive(socket);
			tmpvalue.Receive(socket);
			fprintf(stderr,"key:%d found!  value:%d\n",tmpkey.mKey,tmpvalue.mValue);
			EndFlag = 1;
			break;
		case NotfoundOp:
			tmpkey.Receive(socket);
			fprintf(stderr,"key:%d not found ! ",tmpkey.mKey);
			tmpkey.Receive(socket);
			fprintf(stderr,"nearest key:%d\n",tmpkey.mKey);
			EndFlag = 1;
			break;
		case SetOp:
			if(settings.verbose>1)
				fprintf(stderr,"SetOP  ");
			tmpkey.Receive(socket);
			tmpvalue.Receive(socket);
			tmpnode = new sg_Node(tmpkey,tmpvalue);
			NodeList.push_back(tmpnode);
			
			// Build up list with memvership vector
			//prepare buffer
			buffindex = 0;
			bufflen = 1+8+tmpnode->mKey.size()+4+8+2+8;
			buff = (char*)malloc(bufflen);
			//serialize
			buff[buffindex++] = TreatOp;
			if(NodeList.size()>0){
				serialize_longlong(buff,&buffindex,NodeList.front()->mId);
			}else{
				targetid=0;
				serialize_longlong(buff,&buffindex,targetid);
			}
			buffindex += tmpnode->mKey.Serialize(&buff[buffindex]);
			serialize_int(buff,&buffindex,settings.myip);
			serialize_longlong(buff,&buffindex,tmpnode->mId);
			serialize_short(buff,&buffindex,settings.useport);
			serialize_longlong(buff,&buffindex,myVector.mVector);
			
			assert(bufflen == buffindex && "buffsize ok");
			connect_send_close(settings.targetip,settings.useport,buff,buffindex);//TreatOp
			free(buff);
			fprintf(stderr,"key:%d ,value:%d set in ID:%lld\n",tmpkey.mKey,tmpvalue.mValue,tmpnode->mId);
			print_nodelist();
			fprintf(stderr,"end of SetOP\n");
			/*
			EndFlag = 1;
			DeleteFlag = 1;
			*/
			break;
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
			
			//fprintf(stderr,"node:%lld key:%d\n",targetid,tmpnode->mKey.mKey);
			if(tmpkey == tmpnode->mKey){
				if(settings.verbose>1)
					fprintf(stderr,"tmpkey:%d\n",tmpkey.mKey);
				//over write? <- TODO
				//prepare buffer
				buffindex = 0;
				bufflen = 1 + tmpnode->mKey.size() + tmpnode->mValue.size();
				buff = (char*)malloc(bufflen);
				//serialize
				buff[buffindex++] = FoundOp;
				buffindex+=tmpnode->mKey.Serialize(&buff[buffindex]);
				buffindex+=tmpnode->mValue.Serialize(&buff[buffindex]);
				
				connect_send_close(originip,originport,buff,bufflen);
				free(buff);
			}else{
				tmplevel = MAXLEVEL-1;
				if(tmpkey > tmpnode->mKey){
					//send TreatOP to Rightside
					left_or_right = Right;
					for(;tmplevel>=0;tmplevel--){
						if(tmpnode->mRight[tmplevel].mFlagValid == 0){
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
						if(tmpnode->mLeft[tmplevel].mFlagValid == 0){
							continue;
						}
						if(tmpnode->mLeft[tmplevel].mKey > tmpkey){
							break;
						}
					}
				}
				if(tmplevel >= 0){
					//start creating message and serialize
					//send LinkOP
					if(settings.verbose>1)
						fprintf(stderr,"send TreatOP level:%d\n",tmplevel);
					
					
					buffindex = 0;
					bufflen = 1+8+tmpkey.size()+4+8+2+8;//[OP id key level ip id port]
					buff = (char*)malloc(bufflen);
					buff[buffindex++] = TreatOp;
					if(left_or_right == Right){
						serialize_longlong(buff,&buffindex,tmpnode->mRight[tmplevel].mId);
					}else{
						serialize_longlong(buff,&buffindex,tmpnode->mLeft[tmplevel].mId);
					}
					buffindex+=tmpkey.Serialize(&buff[buffindex]);
					serialize_int(buff,&buffindex,originip);
					serialize_longlong(buff,&buffindex,originid);
					serialize_short(buff,&buffindex,originport);
					serialize_longlong(buff,&buffindex,originvector);
					assert(bufflen == buffindex && "buffsize ok");
					
					if(left_or_right == Right){
						send_with_connect(tmpnode->mRight[tmplevel],buff,bufflen);
					}else{
						send_with_connect(tmpnode->mLeft[tmplevel],buff,bufflen);
					}
					free(buff);
					EndFlag = 1;
				}else{
					//send LinkOP
					if(settings.verbose>1)
						fprintf(stderr,"treated by ID:%lld key:%d\n",tmpnode->mId,tmpnode->mKey.mKey);
					
					//opposite
					if(settings.verbose>1){
						if(left_or_right == Left){
							fprintf(stderr,"send introduceOp from %d to %d\n",tmpnode->mKey.mKey,tmpnode->mLeft[0].mKey.mKey);
						}else{
							fprintf(stderr,"send introduceOp from %d to %d\n",tmpnode->mKey.mKey,tmpnode->mRight[0].mKey.mKey);
						}
					}
					targetlevel = 0;
					buffindex = 0;
					bufflen = 1+8+tmpkey.size()+4+8+2+4+8;
					buff = (char*)malloc(bufflen);
					buff[buffindex++] = IntroduceOp;
					if(left_or_right == Left){
						serialize_longlong(buff,&buffindex,tmpnode->mLeft[0].mId);
					}else{
						serialize_longlong(buff,&buffindex,tmpnode->mRight[0].mId);
					}
					buffindex += tmpkey.Serialize(&buff[buffindex]);
					serialize_int(buff,&buffindex,originip);
					serialize_longlong(buff,&buffindex,originid);
					serialize_short(buff,&buffindex,originport);
					serialize_int(buff,&buffindex,targetlevel);
					serialize_longlong(buff,&buffindex,originvector);
					assert(bufflen == buffindex);
					if(left_or_right == Left){
						connect_send_close(tmpnode->mLeft[0].mAddress.ip,tmpnode->mLeft[0].mAddress.port,buff,bufflen);
					}else{
						connect_send_close(tmpnode->mRight[0].mAddress.ip,tmpnode->mRight[0].mAddress.port,buff,bufflen);
					}
					free(buff);
					
					//LinkOp to new node
					targetlevel = 0;
					buffindex = 0; 
					bufflen = 1+8+tmpkey.size()+4+8+2+4+1;
					buff = (char*)malloc(bufflen);
					buff[buffindex++] = LinkOp;
					serialize_longlong(buff,&buffindex,originid);
					buffindex += tmpnode->mKey.Serialize(&buff[buffindex]);
					serialize_int(buff,&buffindex,settings.myip);
					serialize_longlong(buff,&buffindex,tmpnode->mId);
					serialize_short(buff,&buffindex,settings.useport);
					serialize_int(buff,&buffindex,targetlevel);
					buff[buffindex++] = left_or_right == Left ? Right : Left ;
					connect_send(&tmpsocket,originip,originport,buff,bufflen);
					mulio.SetSocket(tmpsocket);
					free(buff);
					if(left_or_right == Left){
						tmpnode->mLeft[0].set(tmpsocket,tmpkey,originip,originid,originport);
					}else{
						tmpnode->mRight[0].set(tmpsocket,tmpkey,originip,originid,originport);
					}
					
					//search level
					targetlevel = myVector.compare(originvector);
					
					bufflen = 1+8+tmpkey.size()+4+8+2+4+1;
					buff = (char*)malloc(bufflen);
					//prepare
					buffindex = 0;
					//serialize
					buff[buffindex++] = LinkOp;
					serialize_longlong(buff,&buffindex,originid);
					buffindex += tmpnode->mKey.Serialize(&buff[buffindex]);//0
					serialize_int(buff,&buffindex,settings.myip);
					serialize_longlong(buff,&buffindex,tmpnode->mId);
					serialize_short(buff,&buffindex,settings.useport);
					for(int i=1;i<=targetlevel;i++){
						serialize_int(buff,&buffindex,i);
						buff[buffindex++] = left_or_right == Left ? Right : Left;
						connect_send(&tmpsocket,originip,originport,buff,buffindex);
						buffindex -= sizeof(int)+1;
						
						mulio.SetSocket(tmpsocket);
						if(left_or_right == Left){
							tmpnode->mLeft[i].set(tmpsocket,tmpkey,originip,originid,originport);
						}else{
							tmpnode->mRight[i].set(tmpsocket,tmpkey,originip,originid,originport);
						}
						printf("Link from %d to %d at level %d\n",tmpnode->mKey.mKey,tmpkey.mKey,i);
						
					}
					free(buff);
					
					targetlevel++;
					if(targetlevel < MAXLEVEL){
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
							send_with_connect(tmpnode->mRight[targetlevel],buff,bufflen);
						}else{
							send_with_connect(tmpnode->mLeft[targetlevel],buff,bufflen);
						}
						free(buff);
					}
				}
			}
			if(settings.verbose>1)
				fprintf(stderr,"end of TreatOP\n");
			EndFlag = 1;
			break;
		case IntroduceOp:
			if(settings.verbose>1)
				fprintf(stderr,"IntroduceOP\n");
			
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
			
			tmplevel = myVector.compare(originvector);
			if(targetlevel < tmplevel){
				for(int i=targetlevel;i<=tmplevel;i++){
					serialize_int(buff,&buffindex,i);
					buff[buffindex++] = left_or_right==Left ? Right : Left;
					assert(buffindex == bufflen);
					connect_send(&tmpsocket,originip,originport,buff,buffindex);
					buffindex -= sizeof(int)+1;
					
					mulio.SetSocket(tmpsocket);
					if(left_or_right == Left){
						tmpnode->mLeft[i].set(tmpsocket,tmpkey,originip,originid,originport);
					}else{
						tmpnode->mRight[i].set(tmpsocket,tmpkey,originip,originid,originport);
					}
				}
			}
			free(buff);
			if(tmplevel < MAXLEVEL-1){
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
				serialize_int(buff,&buffindex,tmplevel);
				serialize_longlong(buff,&buffindex,originvector);
				if(left_or_right == Left){
					send_with_connect(tmpnode->mRight[targetlevel],buff,bufflen);
				}else{
					send_with_connect(tmpnode->mLeft[targetlevel],buff,bufflen);
				}
				free(buff);
			}
			fprintf(stderr,"end of Introduce Op\n");
			EndFlag = 1;
			break;
		default:
			fprintf(stderr,"error: undefined operation %d.\n",op);
		}
	}
	/*
	  if(settings.verbose>1)
	  fprintf(stderr,"socket:%d end\n",socket);
	//*/
	return DeleteFlag;
}
void* worker(void* arg){
	mulio.run();// accept thread
	return NULL;
}

int main(int argc,char** argv){
	srand(sysrand());
	pthread_t* threads;
	char c;
	intkey min,max;
	min.Minimize();
	max.Maximize();
	intvalue dummy(0xdeadbeef);
	sg_Node minnode(min,dummy),maxnode(max,dummy);
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
	
	if(settings.targetip != 0){ // join to the skip graph
		if(settings.verbose>1){
			struct in_addr tmp_inaddr;
			tmp_inaddr.s_addr=settings.targetip;
			fprintf(stderr,"send to %s:%d\n\n",inet_ntoa(tmp_inaddr),settings.useport);
		}
	}else{ // I am master
		
		// create left&right end
		minnode.setMaxLevel(MAXLEVEL);
		maxnode.setMaxLevel(MAXLEVEL);
		
		minnode.mPointer.mAddress.ip = maxnode.mPointer.mAddress.ip = settings.myip;
		minnode.mPointer.mAddress.port = maxnode.mPointer.mAddress.port = settings.useport;
		minnode.mPointer.mAddress.id = maxnode.mPointer.mAddress.id = minnode.mId;
		minnode.mPointer.mSocket = maxnode.mPointer.mSocket = 0;
		
		for(int i=0;i<MAXLEVEL;i++){
			minnode.mRight[i] = maxnode.mPointer;
			minnode.mLeft[i].mFlagValid = 0;
			
			maxnode.mRight[i].mFlagValid = 0;
			maxnode.mLeft[i] = minnode.mPointer;
		}
		/*
		for(int i=0;i<MAXLEVEL;i++){
			fprintf(stderr,"%d:mLeft = %d  valid:%d\n",i,minnode.mLeft[i].mKey.mKey,minnode.mLeft[i].mFlagValid);
		}
		*/
		
		NodeList.push_back(&minnode);
		NodeList.push_back(&maxnode);
		
		fprintf(stderr,"min:ID%lld  max:ID%lld\n",minnode.mId,maxnode.mId);
	}
	// set accepting thread
	int listening = create_tcpsocket();
	set_reuse(listening);
	bind_inaddr_any(listening,settings.useport);
	listen(listening,2048);
	mulio.SetAcceptSocket(listening);
	mulio.SetCallback(main_thread);
	mulio.run();// accept thread
	
	threads = (pthread_t*)malloc((settings.threads-1)*sizeof(pthread_t));
	for(int i=0;i<settings.threads-1;i++){
		//pthread_create(&threads[i],NULL,worker,NULL);
	}
	if(settings.verbose>1){
		std::cerr << "start warking as skipgraph server..." << std::endl;
	}
	
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
