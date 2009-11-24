
#include "skipgraph.h"
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


typedef sg_node<intkey,intvalue> sg_Node;
typedef sg_neighbor<intkey> sg_Neighbor;
typedef neighbor_list<intkey> neighbor_List;
mulio mulio;
membership_vector myVector;
neighbor_list<intkey> gNeighborList;
std::list<sg_Node*> NodeList;
std::list<address> gAddressList;

void print_nodelist(void){
	std::list<sg_Node*>::iterator it = NodeList.begin();
	while(it != NodeList.end() ){
		//if((*it)->mId != (*it)->pPointer.mId){printf("not sanity\n");}
		printf("%lld: key=%d  value=%x",(*it)->mId,(*it)->mKey.mKey,(*it)->mValue.mValue);
		if((*it)->mLeft[0]){
			printf("  left=%d ",(*it)->mLeft[0]->mKey.mKey);
		}else{
			printf("  left=no connect");
		}
		if((*it)->mRight[0]){
			printf("  right=%d\n",(*it)->mRight[0]->mKey.mKey);
		}else{
			printf("  right=no connect\n");
		}
		++it;
		/*
		  fprintf(stderr,"%lld: key:%d,value:%x :minnode->pointers[0]  left:%x  right:%x\n",it->mId,it->mKey.mKey,it->mValue.mValue,(unsigned int)(*it).mLeft[0],(unsigned int)it->mRight[0]);
		  fprintf(stderr,"address mLeft diff:%d\n",(unsigned int)&it->mLeft[0]-(unsigned int)&*it);
		  fprintf(stderr,"address mRight diff:%d\n",(unsigned int)&it->mRight[0]-(unsigned int)&*it);
		*/
	}
}
class address* get_some_address(void){
  std::list<address>::iterator it = gAddressList.begin();
  for(;it!=gAddressList.end();it++){
    if(it->mIP != settings.myip || it->mPort != settings.myport){
      return &*it;
    }
  }
  return NULL;
}

address* search_from_addresslist(int ip,unsigned short port){
  std::list<address>::iterator it = gAddressList.begin();
  for(;it!=gAddressList.end();it++){
    if(it->mIP==ip && it->mPort==port){
      return &*it;
    }
  }
  return NULL;
}
address* add_new_address(const int socket,const int ip,const unsigned short port){
	address* ad = new address(socket,port,ip);
	gAddressList.push_back(*ad);
	return ad;
}
	


template<typename KeyType>
sg_Node* search_node_by_key(KeyType key){
	std::list<sg_Node*>::iterator it = NodeList.begin();
	while(it != NodeList.end() ){
		if((*it)->mKey == key){
			return *it;
		}
		++it;
	}
	return NodeList.front();
}


sg_Node* search_node_by_id(long long id){
	//printf("search_node_by_id began, id:%lld\n",id);
	if(NodeList.empty()){
		return NULL;
	}
	std::list<sg_Node*>::iterator it = NodeList.begin();
	while(it != NodeList.end() ){
		if((*it)->mId == id){
			return *it;
		}
		++it;
	}
	return NULL;
}

sg_Node* search_node_by_key(long long id){
	//it may return nearest key I have
	// and now, it returns 
	if(NodeList.empty()){
		return NULL;
	}
	std::list<sg_Node*>::iterator it = NodeList.begin();
	
	while(it != NodeList.end() ){
		if((*it)->mId == id){
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
	SetOp,
	LinkOp,
	TreatOp,
	IntroduceOp,
};
enum Left_Right{
	Left,
	Right,
};

int send_to_address(const address* ad,const char* buff,const int bufflen){
	int sendsize;
	assert(ad->mSocket != 0);
	
	sendsize = write(ad->mSocket,buff,bufflen);
	if(sendsize<=0){
		fprintf(stderr,"send_to_address:failed to write %d\n",ad->mSocket);
		exit(0);
	}
	return 0;
}

int main_thread(int s){
	// communication management
	int socket = s,chklen;
	char op;
	
	// buffer management
	char* buff;
	int bufflen,buffindex;
	int targetip,originip;
	unsigned long long targetid,originid;
	unsigned short targetport,originport;
	int targetlevel,originlevel;
	long long originvector; //myVector is global
	sg_Node *targetnode,*newnode;
	char left_or_right;
	std::list<class address>::iterator AddressIt;
	address *targetaddress,*originaddress;
	
	intkey rKey;//received Key
	intvalue rValue;//received Value
	
	if(settings.verbose>1)
		fprintf(stderr,"socket:%d ",socket);
	
	int EndFlag = 0,DeleteFlag = 0;
	while(EndFlag == 0){
		chklen = read(socket,&op,1);
		if(chklen <= 0){
			fprintf(stderr," closed %d\n",socket);
			close(socket);
			for(AddressIt = gAddressList.begin(); AddressIt != gAddressList.end(); ++AddressIt){
				if(AddressIt->mSocket == socket){
					gAddressList.erase(AddressIt);
					break;
				}
			}
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
			targetnode = search_node_by_id(targetid);
			assert(targetnode!=NULL);
			
			rKey.Receive(socket);
			read(socket,&targetlevel,4);
			read(socket,&targetip,4);
			read(socket,&targetport,2); 
			fprintf(stderr,"received searchkey:%d mykey:%d\n",rKey.mKey,targetnode->mKey.mKey);
			
			if(rKey == targetnode->mKey){
				//send FoundOP
				if(settings.verbose>1)
					fprintf(stderr,"found in ID:%lld\n",targetnode->mId);
				//prepare buffer
				buffindex = 0;
				bufflen = 1 + targetnode->mKey.size() + targetnode->mValue.size();
				buff = (char*)malloc(bufflen);
				//serialize
				buff[buffindex++] = FoundOp;
				buffindex+=targetnode->mKey.Serialize(&buff[buffindex]);
				buffindex+=targetnode->mValue.Serialize(&buff[buffindex]);
				
				connect_send_close(targetip,targetport,buff,bufflen);
				free(buff);
			}else{
				fprintf(stderr,"%d : %d ?\n",rKey.mKey, targetnode->mKey.mKey);
				if(rKey > targetnode->mKey){
					//send SearchOP to Rightside
					left_or_right = Right;
					for(;targetlevel>=0;targetlevel--){
						if(!(targetnode->mRight[targetlevel])) continue;
						if(!(targetnode->mRight[targetlevel]->mKey > rKey)) break;
					}
				}else{
					//send SearchOcP to Leftside
					left_or_right = Left;
					for(;targetlevel>=0;targetlevel--){
						if(targetnode->mLeft[targetlevel])continue;
						if(!(targetnode->mLeft[targetlevel]->mKey < rKey)) break;
					}
				}
				if(targetlevel >= 0){
					// pass the message to next node
					//start creating message and serialize
					buffindex = 0;
					bufflen = 1+8+rKey.size()+4+4+2;//[OP id key level ip port]
					buff = (char*)malloc(bufflen);
					buff[0] = SearchOp;
					buffindex+=1;
					
					if(left_or_right == Right){
						serialize_longlong(buff,&buffindex,targetnode->mRight[targetlevel]->mId);
					}else{
						serialize_longlong(buff,&buffindex,targetnode->mLeft[targetlevel]->mId);
					}
					buffindex+=rKey.Serialize(&buff[buffindex]);
					serialize_int(buff,&buffindex,targetlevel);
					serialize_int(buff,&buffindex,targetip);
					serialize_short(buff,&buffindex,targetport);
					
					assert(bufflen == buffindex && "buffsize ok");
	  
					if(left_or_right == Right){
						send_to_address(targetnode->mRight[targetlevel]->mAddress,buff,bufflen);
					}else{
						send_to_address(targetnode->mLeft[targetlevel]->mAddress,buff,bufflen);
					}
					free(buff);
				}else{
					//send NotfoundOP
					if(settings.verbose>1)
						fprintf(stderr,"Notfound\n");
					//prepare buffer
					buffindex = 0;
					bufflen = 1 + rKey.size() + targetnode->mKey.size();
					buff = (char*)malloc(bufflen);
					//serialize
					buff[buffindex] = NotfoundOp;
					buffindex+=1;
					buffindex+=rKey.Serialize(&buff[buffindex]);
					buffindex+=targetnode->mKey.Serialize(&buff[buffindex]);
	  
					//send and cleanup
					originaddress = search_from_addresslist(targetip,targetport);
					if(originaddress != NULL){
						send_to_address(originaddress,buff,bufflen);
					}else{
						connect_send_close(targetip,targetport,buff,bufflen);
					}
					free(buff);
				}
			}
			EndFlag = 1;
			break;
		case LinkOp://id,key,originip,originid,originport,level,LorR
			if(settings.verbose>1)
				fprintf(stderr,"LinkOP ");
			
			read(socket,&targetid,8);
			targetnode = search_node_by_id(targetid);
			rKey.Receive(socket);
			read(socket,&originip,4);
			read(socket,&originid,8);
			read(socket,&originport,2); 
			read(socket,&targetlevel,4);
			read(socket,&left_or_right,1);
      
			originaddress = search_from_addresslist(originip,originport);
			if(originaddress == NULL){
				originaddress = add_new_address(socket,originip,originport);
				mulio.SetSocket(socket);
			}
			
			if(left_or_right == Left){
				targetnode->mLeft[targetlevel] = gNeighborList.retrieve(rKey,originid,originaddress);
			}else{
				targetnode->mRight[targetlevel] = gNeighborList.retrieve(rKey,originid,originaddress);
			}
						
			if(settings.verbose>1)
				fprintf(stderr,"target:%d from:%d   targetlevel:%d",targetnode->mKey.mKey,rKey.mKey,targetlevel);
			
			if(left_or_right == Left){
				fprintf(stderr,"  in left side\n");
			}else{
				fprintf(stderr,"  in right side\n");
			}
			EndFlag = 1;
			break;
		case FoundOp:
			rKey.Receive(socket);
			rValue.Receive(socket);
			fprintf(stderr,"key:%d found!  value:%d\n",rKey.mKey,rValue.mValue);
			EndFlag = 1;
			break;
		case NotfoundOp:
			rKey.Receive(socket);
			fprintf(stderr,"key:%d not found ! ",rKey.mKey);
			rKey.Receive(socket);
			fprintf(stderr,"nearest key:%d\n",rKey.mKey);
			EndFlag = 1;
			break;
		case SetOp:
			if(settings.verbose>1)
				fprintf(stderr,"SetOP ");
			
			rKey.Receive(socket);
			rValue.Receive(socket);
			newnode = new sg_Node(rKey,rValue);
			
			//search the nearest neighbor or node from my list
			if(NodeList.size()>0){
				targetnode=NodeList.front();
			}else{
				targetnode=NULL;
			}
			/*  TODO  */
			
			// Build up list with memvership vector
			//prepare buffer
			buffindex = 0;
			bufflen = 1+8+newnode->mKey.size()+4+8+2+8;
			buff = (char*)malloc(bufflen);
			buff[buffindex++] = TreatOp;
			
			targetnode=NULL;//TODO
			if(targetnode){
				serialize_longlong(buff,&buffindex,targetnode->mId);
			}else{
				serialize_longlong(buff,&buffindex,0);
			}
			buffindex += newnode->mKey.Serialize(&buff[buffindex]);
			serialize_int(buff,&buffindex,settings.myip);
			serialize_longlong(buff,&buffindex,newnode->mId);
			serialize_short(buff,&buffindex,settings.myport);
			serialize_longlong(buff,&buffindex,myVector.mVector);
			
			NodeList.push_back(newnode);
			
			assert(bufflen == buffindex && "buffsize ok");
			
			chklen = 0;
			
			printf("addresses:%d\n",gAddressList.size());
			for(AddressIt = gAddressList.begin();AddressIt != gAddressList.end();++AddressIt){
				if(AddressIt->mIP == settings.myip && NodeList.empty()){++AddressIt; continue;}
				fprintf(stderr,"trying:%s .. ",my_ntoa(AddressIt->mIP));
				chklen = connect_send_close(AddressIt->mIP,AddressIt->mPort,buff,buffindex);//TreatOp
				if(chklen > 0){
					fprintf(stderr,"ok\n");
					break;
				}else{
					fprintf(stderr,"NG,try next address..\n");
				}
			}
			if(chklen <= 0){
				fprintf(stderr,"\n All Address tried but failed.\n");
			}
			
			free(buff);
			fprintf(stderr,"key:%d ,value:%d set in ID:%lld\n",rKey.mKey,rValue.mValue,newnode->mId);
			print_nodelist();
			fprintf(stderr,"end of SetOP\n");
			
			EndFlag = 1;
			break;
		case TreatOp://targetid,key,originip,originid,originport
			// if you are nearest to origin, connect origin in level 0
			if(settings.verbose>1)
				fprintf(stderr,"TreatOP\n");
			
			read(socket,&targetid,8);
			targetnode = search_node_by_id(targetid);
			
			rKey.Receive(socket);
			read(socket,&originip,4);
			read(socket,&originid,8);
			read(socket,&originport,2);
			read(socket,&originvector,8);
			
			if(rKey == targetnode->mKey && originip == settings.myip && originport == settings.myport && originvector == myVector.mVector){
				assert("boomerang of TreatOP");
				//TODO
			}
			if(rKey == targetnode->mKey){
				if(settings.verbose>1)
					fprintf(stderr,"received key:%d but I already have it\n",rKey.mKey);
				//over write? <- TODO
				
			}else{
				targetlevel = MAXLEVEL-1;
				if(rKey > targetnode->mKey){
					//send TreatOP to Rightside
					left_or_right = Right;
					for(;targetlevel>=0;targetlevel--){
						if(!targetnode->mRight[targetlevel]) continue;
						if(!(targetnode->mRight[targetlevel]->mKey > rKey)) break;
					}
				}else{
					//send TreatOP to Leftside
					left_or_right = Left;
					for(;targetlevel>=0;targetlevel--){
						if(!targetnode->mLeft[targetlevel]) continue;
						if(!(targetnode->mLeft[targetlevel]->mKey < rKey)) break;
					}
				}
				if(targetlevel >= 0){
					//start creating message and serialize
					//pass LinkOP
					if(settings.verbose>1)
						fprintf(stderr,"passing TreatOP at level:%d\n",targetlevel);
	  
					buffindex = 0;
					bufflen = 1+8+rKey.size()+4+8+2+8;//[OP id key level ip id port]
					buff = (char*)malloc(bufflen);
					buff[buffindex++] = TreatOp;
					if(left_or_right == Right){
						serialize_longlong(buff,&buffindex,targetnode->mRight[targetlevel]->mId);
					}else{
						serialize_longlong(buff,&buffindex,targetnode->mLeft[targetlevel]->mId);
					}
					buffindex+=rKey.Serialize(&buff[buffindex]);
					serialize_int(buff,&buffindex,originip);
					serialize_longlong(buff,&buffindex,originid);
					serialize_short(buff,&buffindex,originport);
					serialize_longlong(buff,&buffindex,originvector);
					assert(bufflen == buffindex && "buffsize ok");
					
					if(left_or_right == Right){
						send_to_address(targetnode->mRight[targetlevel]->mAddress,buff,bufflen);
					}else{
						send_to_address(targetnode->mLeft[targetlevel]->mAddress,buff,bufflen);
					}
					free(buff);
					EndFlag = 1;
				}else{
					if(settings.verbose>1)
						fprintf(stderr,"finally treated by ID:%lld key:%d\n",targetnode->mId,targetnode->mKey.mKey);
					
					// begin treating new node
					
					//send IntroduceOP to opposite site
					targetlevel = 0;
					buffindex = 0;
					bufflen = 1+8+rKey.size()+4+8+2+4+8;// [OP ID key originIP originID originPort level vector]
					buff = (char*)malloc(bufflen);
					buff[buffindex++] = IntroduceOp;
					if(left_or_right == Left && targetnode->mLeft[0]){
						fprintf(stderr,"target ID:%lld\n",targetnode->mLeft[0]->mId);
						serialize_longlong(buff,&buffindex,targetnode->mLeft[0]->mId);
					}else if(left_or_right == Right && targetnode->mRight[0]) {
						fprintf(stderr,"target ID:%lld\n",targetnode->mRight[0]->mId);
						serialize_longlong(buff,&buffindex,targetnode->mRight[0]->mId);
					}else{
						fprintf(stderr,"no node to introduce from %d\n",targetnode->mKey.mKey);
					}
					
					buffindex += rKey.Serialize(&buff[buffindex]);
					serialize_int(buff,&buffindex,originip);
					serialize_longlong(buff,&buffindex,originid);
					serialize_short(buff,&buffindex,originport);
					serialize_int(buff,&buffindex,0);
					serialize_longlong(buff,&buffindex,originvector);
					
					assert(bufflen == buffindex);
					if(left_or_right == Left){
						send_to_address(targetnode->mLeft[0]->mAddress,buff,buffindex);
					}else{
						send_to_address(targetnode->mRight[0]->mAddress,buff,buffindex);
					}
					if(settings.verbose>1){
						if(left_or_right == Left){
							fprintf(stderr,"sending introduceOp from %d to %d by socket:%d\n",targetnode->mKey.mKey,targetnode->mLeft[0]->mKey.mKey,targetnode->mLeft[0]->mAddress->mSocket);
						}else if(left_or_right == Right){
							fprintf(stderr,"sending introduceOp from %d to %d by socket:%d\n",targetnode->mKey.mKey,targetnode->mRight[0]->mKey.mKey,targetnode->mRight[0]->mAddress->mSocket);
						}
					}
					free(buff);
	  
					//LinkOp to new node
					targetlevel = myVector.compare(originvector);
					printf("vector1:%lld\nvector2:%lld\n%d bit equal\n",myVector.mVector,originvector,targetlevel);
					bufflen = 1+8+rKey.size()+4+8+2+4+1;
					buff = (char*)malloc(bufflen);
					buffindex = 0;
					//serialize
					buff[buffindex++] = LinkOp;
					serialize_longlong(buff,&buffindex,originid);
					buffindex += targetnode->mKey.Serialize(&buff[buffindex]);//0
					serialize_int(buff,&buffindex,settings.myip);
					serialize_longlong(buff,&buffindex,targetnode->mId);
					serialize_short(buff,&buffindex,settings.myport);
					
					targetaddress = search_from_addresslist(originip,originport);
					if(targetaddress == NULL){
						targetaddress = add_new_address(socket,originip,originport);
						mulio.SetSocket(socket);
					}
					
					for(int i=0;i<=targetlevel;i++){
						serialize_int(buff,&buffindex,i);
						buff[buffindex++] = left_or_right == Left ? Right : Left;
						send_to_address(targetaddress,buff,bufflen);
						buffindex -= sizeof(int)+1;
						
						if(left_or_right == Left){
							targetnode->mLeft[i] = gNeighborList.retrieve(rKey,originid,targetaddress);
						}else{
							targetnode->mRight[i] = gNeighborList.retrieve(rKey,originid,targetaddress);
						}
						if(settings.verbose>1){
							fprintf(stderr,"Link from %d to %d at level %d\n",targetnode->mKey.mKey,rKey.mKey,i);
						}
					}
					free(buff);
					
					targetlevel++;
					if(targetlevel < MAXLEVEL){
						if((left_or_right == Left && targetnode->mRight[targetlevel] != NULL) ||
						   (left_or_right == Right && targetnode->mLeft[targetlevel] != NULL)){
							buffindex = 0;
							bufflen = 1+8+rKey.size()+4+8+2+4+8;
							buff = (char*)malloc(bufflen);
							//serialize
							buff[buffindex++] = IntroduceOp;
							serialize_longlong(buff,&buffindex,originid);
							buffindex += rKey.Serialize(&buff[buffindex]);
							serialize_int(buff,&buffindex,originip);
							serialize_longlong(buff,&buffindex,originid);
							serialize_short(buff,&buffindex,originport);
							serialize_int(buff,&buffindex,targetlevel);
							serialize_longlong(buff,&buffindex,originvector);
	    
							if(left_or_right == Left && targetnode->mRight[targetlevel]){
								send_to_address(targetnode->mRight[targetlevel]->mAddress,buff,bufflen);
							}else if(left_or_right == Right && targetnode->mLeft[targetlevel]){
								send_to_address(targetnode->mLeft[targetlevel]->mAddress,buff,bufflen);
							}
							free(buff);
						}
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
			targetnode = search_node_by_id(targetid);
			fprintf(stderr,"found %lld key:%d\n",targetnode->mId,targetnode->mKey.mKey);
			
			rKey.Receive(socket);
			read(socket,&originip,4);
			read(socket,&originid,8);
			read(socket,&originport,2);
			read(socket,&originlevel,4);
			read(socket,&originvector,8);
			
			if(rKey < targetnode->mKey){
				left_or_right = Left;
			}else {
				left_or_right = Right;
			}
			//LinkOp to new node
			targetlevel = myVector.compare(originvector);
			
			bufflen = 1+8+targetnode->mKey.size()+4+8+2+4+1;
			buff = (char*)malloc(bufflen);
			//prepare
			buffindex = 0;
			//serialize
			buff[buffindex++] = LinkOp;
			serialize_longlong(buff,&buffindex,originid);
			buffindex += targetnode->mKey.Serialize(&buff[buffindex]);
			serialize_int(buff,&buffindex,settings.myip);
			serialize_longlong(buff,&buffindex,targetnode->mId);
			serialize_short(buff,&buffindex,settings.myport);
			
			targetaddress = search_from_addresslist(originip,originport);
			if(targetaddress == NULL){
				targetaddress = add_new_address(socket,originip,originport);
				mulio.SetSocket(socket);
			}
			for(int i=originlevel;i<=targetlevel;i++){
				serialize_int(buff,&buffindex,i);
				buff[buffindex++] = left_or_right == Left ? Right : Left;
				send_to_address(targetaddress,buff,bufflen);
				buffindex -= sizeof(int)+1;
				
				if(left_or_right == Left){
					targetnode->mLeft[i] = gNeighborList.retrieve(rKey,originid,targetaddress);
				}else{
					targetnode->mRight[i] = gNeighborList.retrieve(rKey,originid,targetaddress);
				}
				printf("Link from %d to %d at level %d in socket:%d\n",targetnode->mKey.mKey,rKey.mKey,i,socket);
			}
			free(buff);
					
			targetlevel++;
			if(targetlevel < MAXLEVEL){
				if((left_or_right == Left && targetnode->mRight[targetlevel] != NULL) ||
				   (left_or_right == Right && targetnode->mLeft[targetlevel] != NULL)){
					buffindex = 0;
					bufflen = 1+8+rKey.size()+4+8+2+4+8;
					buff = (char*)malloc(bufflen);
					//serialize
					buff[buffindex++] = IntroduceOp;
					serialize_longlong(buff,&buffindex,originid);
					buffindex += rKey.Serialize(&buff[buffindex]);
					serialize_int(buff,&buffindex,originip);
					serialize_longlong(buff,&buffindex,originid);
					serialize_short(buff,&buffindex,originport);
					serialize_int(buff,&buffindex,targetlevel);
					serialize_longlong(buff,&buffindex,originvector);
				
					if(left_or_right == Left && targetnode->mRight[targetlevel]){
						send_to_address(targetnode->mRight[targetlevel]->mAddress,buff,bufflen);
					}else if(left_or_right == Right && targetnode->mLeft[targetlevel]){
						send_to_address(targetnode->mLeft[targetlevel]->mAddress,buff,bufflen);
					}
					free(buff);
				}
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
	
	//initialize
	min.Minimize();
	max.Maximize();
	gAddressList.clear();
	intvalue dummy(0xdeadbeef);
	
	settings_init();
	NodeList.clear();
	myVector.init();
	
	// parse options
	while ((c = getopt(argc, argv, "a:t:vPp:h")) != -1) {
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
			settings.targetport = atoi(optarg);
			break;
		case 'P':
			settings.myport = atoi(optarg);
			break;
		case 'h':
			print_usage();
			exit(0);
			break;
		}
	}
	settings.myip = chk_myip();
	
	
	// set myself
	gAddressList.push_back( address(0,settings.myip, settings.myport) );
	if(settings.targetip != 0){ // join to the skip graph
		if(settings.verbose>1){
			struct in_addr tmp_inaddr;
			tmp_inaddr.s_addr=settings.targetip;
			fprintf(stderr,"send to %s:%d\n\n",inet_ntoa(tmp_inaddr),settings.targetport);
			gAddressList.push_back(address(0,settings.targetip,settings.targetport));
		}
	}else{ // I am master
		sg_Node* minnode;
		sg_Node* maxnode;
		minnode = new sg_Node(min,dummy);
		maxnode = new sg_Node(max,dummy);
		// create left&right end
		
		sg_neighbor<intkey> *minpointer,*maxpointer;
		class address myAddress(0,settings.myip,settings.myport);
		minpointer = new sg_Neighbor(min,&myAddress,minnode->mId);
		maxpointer = new sg_Neighbor(max,&myAddress,maxnode->mId);
		
		for(int i=0;i<MAXLEVEL;i++){
			minnode->mLeft[i] = NULL;
			minnode->mRight[i] = maxpointer;
			maxnode->mLeft[i] = minpointer;
			maxnode->mRight[i] = NULL;
		}
		/*
		  for(int i=0;i<MAXLEVEL;i++){
		  fprintf(stderr,"%d:mLeft = %d  valid:%d\n",i,minnode.mLeft[i]->mKey.mKey,minnode.mLeft[i]->mValidFlag);
		  }
		*/
		NodeList.push_back(minnode);
		NodeList.push_back(maxnode);
		
		fprintf(stderr,"min:ID%lld  max:ID%lld level:%d\n",minnode->mId,maxnode->mId,MAXLEVEL);
	}
	print_nodelist();
	// set accepting thread
	int listening = create_tcpsocket();
	set_reuse(listening);
	bind_inaddr_any(listening,settings.myport);
	listen(listening,2048);
	mulio.SetAcceptSocket(listening);
	mulio.SetCallback(main_thread);
	mulio.run();// accept thread
	
	threads = (pthread_t*)malloc((settings.threads-1)*sizeof(pthread_t));
	for(int i=0;i<settings.threads-1;i++){
		pthread_create(&threads[i],NULL,worker,NULL);
	}
	if(settings.verbose>1){
		printf("start warking as skipgraph server...\n");
		printf("myIP:%s myport:%d\ntargetIP:%s targetport:%d\nverbose:%d\nthreads:%d\nVector:%lld\n\n",
			   my_ntoa(settings.myip),settings.myport,my_ntoa(settings.targetip),settings.targetport,
			   settings.verbose,settings.threads,myVector.mVector);
	}
	
	mulio.eventloop();
}

void print_usage(void){
	std::cout << "-a [xxx.xxx.xxx.xxx]:target IP" << std::endl;
	std::cout << "-p [x]              :target port" << std::endl;
	std::cout << "-t [x]              :number of threads" << std::endl;
	std::cout << "-v                  :verbose mode" << std::endl;
	std::cout << "-P [x]              :use port" << std::endl;
	std::cout << "-h                  :print this message" << std::endl;
}
void settings_init(void){
	settings.myport = 10005;
	settings.targetip = 0;
	settings.targetport = 10005;
	settings.verbose = 3;
	settings.threads = 4;
}
