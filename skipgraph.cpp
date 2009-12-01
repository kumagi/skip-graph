
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
#include <unistd.h>
#include <errno.h>

#include <assert.h>//assert
#include "mulio.h"
#include "mytcplib.h"


typedef sg_neighbor<defkey> sg_Neighbor;
typedef neighbor_list<defkey> neighbor_List;
mulio mulio,memcached;
membership_vector myVector;
neighbor_list<defkey> gNeighborList;
std::list<sg_Node*> NodeList;
std::list<address> gAddressList;

void print_nodelist(void){
	std::list<sg_Node*>::iterator it = NodeList.begin();
	while(it != NodeList.end() ){
		//if((*it)->mId != (*it)->pPointer.mId){printf("not sanity\n");}
		printf("%lld: key=%s  value=%s",(*it)->mId,(*it)->mKey.toString(),(*it)->mValue.toString());
		if((*it)->mLeft[0]){
			printf("  left=%s ",(*it)->mLeft[0]->mKey.toString());
		}else{
			printf("  left=no connect");
		}
		if((*it)->mRight[0]){
			printf("  right=%s\n",(*it)->mRight[0]->mKey.toString());
		}else{
			printf("  right=no connect\n");
		}
		++it;
	}
}
address* get_some_address(void){
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
	address* ad = new address(socket,ip,port);
	if(socket == 0){
		printf("dont register the 0 socket\n");
	}
	gAddressList.push_back(*ad);
	
	fprintf(stderr,"new address %s:%d socket:%d\n",my_ntoa(ad->mIP),ad->mPort,ad->mSocket);
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
	ViewOp,
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
	return sendsize;
}

int main_thread(const int s){
	// communication management
	int socket = s,chklen,newsocket;
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
	std::list<address>::iterator AddressIt;
	address *targetaddress,*originaddress;
	
	defkey rKey;//received Key
	defvalue rValue;//received Value
	
	if(settings.verbose>1)
		fprintf(stderr,"socket:%d ",socket);
	
	int EndFlag = 0,DeleteFlag = 0;
	while(EndFlag == 0){
		chklen = read(socket,&op,1);
		if(chklen <= 0){
			fprintf(stderr," closed %d",socket);
			perror("  ");
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
			fprintf(stderr,"received searchkey:%s mykey:%s\n",rKey.toString(),targetnode->mKey.toString());
			
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
				fprintf(stderr,"%s : %s ?\n",rKey.toString(), targetnode->mKey.toString());
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
					// passthe message to next node
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
				fprintf(stderr,"target:%s from:%s   targetlevel:%d\n",targetnode->mKey.toString(),rKey.toString(),targetlevel);
			
			EndFlag = 1;
			break;
		case FoundOp:
			rKey.Receive(socket);
			rValue.Receive(socket);
			fprintf(stderr,"key:%s found!  value:%s\n",rKey.toString(),rValue.toString());
			EndFlag = 1;
			break;
		case NotfoundOp:
			rKey.Receive(socket);
			fprintf(stderr,"key:%s not found ! ",rKey.toString());
			rKey.Receive(socket);
			fprintf(stderr,"nearest key:%s\n",rKey.toString());
			EndFlag = 1;
			break;
		case SetOp:
			if(settings.verbose>1)
				fprintf(stderr,"SetOP ");
			
			rKey.Receive(socket);
			rValue.Receive(socket);
			newnode = new sg_Node(rKey,rValue);
			
			//search the nearest neighbor or node from my list
			/*  TODO  */
			if(NodeList.size()>0){
				targetnode=NodeList.front();
			}else{
				targetnode=NULL;
			}
			
			// Build up list with memvership vector
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
				if(AddressIt->mIP == settings.myip && NodeList.empty()){
					continue;
				}
				fprintf(stderr,"trying:%s .. ",my_ntoa(AddressIt->mIP));
				chklen = send_to_address(&*AddressIt,buff,bufflen);
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
			fprintf(stderr,"key:%s ,value:%s set in ID:%lld\n",rKey.toString(),rValue.toString(),newnode->mId);
			print_nodelist();
			fprintf(stderr,"end of SetOP\n");
			
			EndFlag = 1;
			//DeleteFlag = 1;
			break;
		case TreatOp://targetid,key,originip,originid,originport
			// if you are nearest to origin, connect origin in level 0
			if(settings.verbose>1){
				fprintf(stderr,"TreatOP\n");
			}
			
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
						fprintf(stderr,"TreatOP to address %s:%d socket:%d\n",my_ntoa(targetnode->mRight[targetlevel]->mAddress->mIP),targetnode->mRight[targetlevel]->mAddress->mPort,targetnode->mRight[targetlevel]->mAddress->mSocket);
					}else{
						send_to_address(targetnode->mLeft[targetlevel]->mAddress,buff,bufflen);
						fprintf(stderr,"TreatOP to address %s:%d socket:%d\n",my_ntoa(targetnode->mLeft[targetlevel]->mAddress->mIP),targetnode->mLeft[targetlevel]->mAddress->mPort,targetnode->mLeft[targetlevel]->mAddress->mSocket);
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
						fprintf(stderr,"no node to introduce from %s\n",targetnode->mKey.toString());
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
							fprintf(stderr,"sending introduceOp from %s to %s by socket:%d\n",targetnode->mKey.toString(),targetnode->mLeft[0]->mKey.toString(),targetnode->mLeft[0]->mAddress->mSocket);
						}else if(left_or_right == Right){
							fprintf(stderr,"sending introduceOp from %s to %s by socket:%d\n",targetnode->mKey.toString(),targetnode->mRight[0]->mKey.toString(),targetnode->mRight[0]->mAddress->mSocket);
						}
					}
					free(buff);
	  
					//LinkOp to new node
					
					//decide how much level to link
					targetlevel = myVector.compare(originvector);
					fprintf(stderr,"vector1:%llx\nvector2:%llx\n%d bit equal\n",myVector.mVector,originvector,targetlevel);
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
						newsocket = create_tcpsocket();
						connect_port_ip(newsocket,originip,originport);
						targetaddress = add_new_address(newsocket,originip,originport);
						mulio.SetSocket(newsocket);
					}
					fprintf(stderr,"LinkOP to address %s:%d socket:%d\n",my_ntoa(targetaddress->mIP),targetaddress->mPort,targetaddress->mSocket);
					
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
							fprintf(stderr,"Link from %s to %s at level %d\n",targetnode->mKey.toString(),rKey.toString(),i);
						}
					}
					free(buff);
					
					targetlevel++;
					if(targetlevel < MAXLEVEL){
						if((left_or_right == Left && targetnode->mRight[targetlevel] != NULL) ||
						   (left_or_right == Right && targetnode->mLeft[targetlevel] != NULL)){
							buffindex = 0;
							bufflen = 1+8+rKey.size()+4+8+2+4+8; // OP ID Key fromIP fromID fromPort level fromVector
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
			if(targetnode == NULL){
				fprintf(stderr,"there is no node such ID:%lld\n",targetid);
				break;
			}
			fprintf(stderr,"found %lld key:%s\n",targetnode->mId,targetnode->mKey.toString());
			
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
				newsocket = create_tcpsocket();
				connect_port_ip(newsocket,originip,originport);
				targetaddress = add_new_address(newsocket,originip,originport);
				mulio.SetSocket(newsocket);
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
				printf("Link from %s to %s at level %d in socket:%d\n",targetnode->mKey.toString(),rKey.toString(),i,targetaddress->mSocket);
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
					if(left_or_right == Left && targetnode->mRight[targetlevel]){
						serialize_longlong(buff,&buffindex,targetnode->mRight[targetlevel]->mId);
					}else if(left_or_right == Right && targetnode->mLeft[targetlevel]){
						serialize_longlong(buff,&buffindex,targetnode->mLeft[targetlevel]->mId);
					}
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
					if(left_or_right == Left && targetnode->mRight[targetlevel]){
						fprintf(stderr,"relay IntroduceOP from %s to %s at level %d in socket:%d\n",targetnode->mKey.toString(),targetnode->mRight[targetlevel]->mKey.toString(),targetlevel,targetnode->mRight[targetlevel]->mAddress->mSocket);
					}else if(left_or_right == Right && targetnode->mLeft[targetlevel]){
						fprintf(stderr,"relay IntroduceOP from %s to %s at level %d in socket:%d\n",targetnode->mKey.toString(),targetnode->mLeft[targetlevel]->mKey.toString(),targetlevel,targetnode->mLeft[targetlevel]->mAddress->mSocket);
					}else{
						fprintf(stderr,"end of relay\n");
					}
				}
			}
			fprintf(stderr,"end of Introduce Op\n");
			EndFlag = 1;
			break;
		case ViewOp:
			fprintf(stderr,"view\n");
			print_nodelist();
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


int natoi(char* str,int length){
	int ans = 0;
	while(length > 0){
		assert('0' <= *str && *str <= '9' );
		ans = ans * 10 + *str - '0';
		str++;
	}
	return ans;
}

enum memcached_buffer_constants{
	TOKENMAX = 8,
	SET_KEY = 0,
	SET_FLAGS = 1,
	SET_EXPTIME = 2,
	SET_LENGTH = 3,
	SET_VALUE = 4,
	GET_KEY = 0,
	DELETE_KEY = 0,
};
class memcached_buffer{
private:
	const int mSocket;
	int mState;
	char* mBuff;
	int mSize;
	int mStart;
	int mChecked;
	int mRead;
	int mReft;
	
	int moreread;
public:
	enum state {
		state_free,
		state_set,
		state_get,
		state_delete,
		state_value, // wait until n byte receive
		state_continue, // not all data received
		state_close,
		state_error,
	};
	struct token{
		char* str;
		int length;
	} tokens[TOKENMAX];
	int tokenmax;
	
	void nextParse(void){
		mState = state_free;
		if(mChecked == mRead){
			if(mSize > 128){
				mBuff = (char*)realloc((void*)mBuff,128);
			}
			mChecked = mRead = mStart = 0;
			mSize = mReft = 128;
			mState = state_free;
		}
	}
	
	memcached_buffer(const int socket):mSocket(socket){
		mSize = 128; // buffer size
		mStart = 0; // head of parse
		mRead = 0; // received data
		mChecked = 0; // checked data
		mReft = mSize; // reft buffer
		mBuff = (char*)malloc(mSize);
	}
	const int& getState(void) const{
		return mState;
	}
	const int& getSocket(void) const{
		return mSocket;
	}
	void receive(void){
		int newread;
		switch(mState){
		case state_free:
			mStart = mChecked;
		case state_continue:
			do{
				if(mReft==0){
					mBuff=(char*)realloc(mBuff,mSize*2);
					mReft = mSize;
					mSize *= 2;
				}
				newread = recv(mSocket,&mBuff[mRead],mReft,MSG_DONTWAIT);
				if(newread == 0){
					mState = state_close;
				}
				mRead += newread;
				mReft -= newread;
			}while(errno!=EAGAIN && errno!=EWOULDBLOCK);
			while(mBuff[mChecked] != '\n' && mChecked < mRead){
				mChecked++;
			}
			if(mChecked == mRead){
				mState = state_continue;
				return;
			}
			mBuff[mChecked] = '\0';
			//mBuff[mStart ~ mChecked] <- command is in this range.
			parse(&mBuff[mStart],&mBuff[mChecked-1]);
			mStart = mChecked + 1;
			break;
		case state_value:
			do{
				if(mReft < moreread){
					mBuff=(char*)realloc(mBuff,mSize*2);
					mReft = mSize;
					mSize *= 2;
				}
				newread = recv(mSocket,&mBuff[mRead],mReft,MSG_DONTWAIT);
				mRead += newread;
				mReft -= newread;
			}while(errno!=EAGAIN && errno!=EWOULDBLOCK);
			if (mRead - mStart < moreread){
				return;
			}
			mChecked += moreread;
			tokens[SET_VALUE].str = &mBuff[mStart];
			tokens[SET_VALUE].length = moreread;
			mStart += moreread;
			mState = state_set;
			break;
		}
	}
	bool operator<(const memcached_buffer& rightside) const{
		return mSocket < rightside.mSocket;
	}
private:
	inline void parse(char* start,char* end){
		int cnt;
		assert(start < end);
		if(strncmp(start,"set ",4)){ // set [key] <flags> <exptime> <length>
			mState = state_value;
			start += 3;
			for(int i=0; i<4; i++){
				while(*start == ' ' && *start != '\0') {
					start++;
				}
				tokens[i].str = start;
				cnt = 0;
				while(start < end && *start != ' ' && *start != '\0'){
					start++;
					cnt++;
				}
				tokens[i].length = cnt;
			}
			moreread = natoi(tokens[SET_LENGTH].str,tokens[SET_LENGTH].length);
		}else if(strncmp(start,"get ",4)){ // get [key] ([key] ([key] ([key].......)))
			mState = state_get;
			start += 3;
			for(int i=0; i<8; i++){
				while(*start == ' ' && *start != '\0') {
					start++;
				}
				tokens[i].str = start;
				cnt = 0;
				while(start < end && *start != ' ' && *start != '\0'){
					start++;
					cnt++;
				}
				tokens[i].length = cnt;
			}
		}else if(strncmp(start,"delete ",7)){ // delete [key] ([key] ([key] ([key].......)))
			mState = state_delete;
			start += 6;
			for(int i=0; i<8; i++){
				while(*start == ' ' && *start != '\0') {
					start++;
				}
				tokens[i].str = start;
				cnt = 0;
				while(start < end && *start != ' ' && *start != '\0'){
					start++;
					cnt++;
				}
				tokens[i].length = cnt;
			}
		}else{
			assert(!"invalid operation\n");
		}
	}
};

int memcached_thread(int socket){
	char op[7];
	int opoffset;
	char buff[32];
	
	fprintf(stderr,"hello\n");
	write(socket,"hello",5);
	opoffset = 0;
	do{
		opoffset += read(socket,op,3);
		op[3] = '\0';
		fprintf(stderr,"%s\n",op);
	}while(opoffset == 3);
	if(strncmp(op,"set",3) == 0){
		read(socket,buff,1);
		fprintf(stderr,"set\n");
	}else if(strncmp(op,"get",3) == 0){
		fprintf(stderr,"get\n");
	}
	
	
	return 0;
}

void* worker(void* arg){
	mulio.eventloop();// accept thread
	return NULL;
}
void* memcached_work(void* arg){
	fprintf(stderr,"memcached thread start\n");
	memcached.eventloop();
	return NULL;
}

int main(int argc,char** argv){
	srand(sysrand());
	pthread_t* threads;
	char c;
	defkey min,max;
	int myself;//loopback socket
	int targetsocket;
	address* myAddress;
	address* newAddress;
	
	//initialize
	min.Minimize();
	max.Maximize();
	gAddressList.clear();
	defvalue dummy(43);
	
	settings_init();
	NodeList.clear();
	myVector.init();
	
	// parse options
	while ((c = getopt(argc, argv, "a:t:m:vP:p:h")) != -1) {
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
		case 'm':
			settings.memcacheport = atoi(optarg);
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
	if(settings.targetip != 0){ // join to the skip graph
		fprintf(stderr,"send to %s:%d\n\n",my_ntoa(settings.targetip),settings.targetport);
		targetsocket = create_tcpsocket();
		newAddress = new address(targetsocket,settings.targetip,settings.targetport);
		gAddressList.push_back(*newAddress);
	}else{ // I am master
		sg_Node* minnode;
		sg_Node* maxnode;
		
		minnode = new sg_Node(min,dummy);
		maxnode = new sg_Node(max,dummy);
		// create left&right end
		
		sg_Neighbor *minpointer,*maxpointer;
		
		myself = create_tcpsocket();
		printf("loopback socket:%d\n",myself);
		myAddress = new address(myself,settings.myip,settings.myport);
		minpointer = new sg_Neighbor(min,myAddress,minnode->mId);
		maxpointer = new sg_Neighbor(max,myAddress,maxnode->mId);
		gAddressList.push_back( *myAddress );
		
		for(int i=0;i<MAXLEVEL;i++){
			minnode->mLeft[i] = NULL;
			minnode->mRight[i] = maxpointer;
			maxnode->mLeft[i] = minpointer;
			maxnode->mRight[i] = NULL;
		}
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
	
	int memcachesocket = create_tcpsocket();
	set_reuse(memcachesocket);
	bind_inaddr_any(memcachesocket,settings.memcacheport);
	listen(memcachesocket,2048);
	memcached.SetAcceptSocket(memcachesocket);
	memcached.SetCallback(memcached_thread);
	memcached.run();
	pthread_t memcache_worker;
	pthread_create(&memcache_worker,NULL,memcached_work,NULL);
	
	if(settings.targetip != 0){
		connect_port_ip(targetsocket,settings.targetip,settings.targetport);
		mulio.SetSocket(targetsocket);
	}else{
		connect_port_ip(myself,settings.myip,settings.myport);
		mulio.SetSocket(myself);
	}
	
	threads = (pthread_t*)malloc((settings.threads-1)*sizeof(pthread_t));
	for(int i=0;i<settings.threads-1;i++){
		//pthread_create(&threads[i],NULL,worker,NULL);
	}
	if(settings.verbose>1){
		printf("start warking as skipgraph server...\n");
		printf("myIP:%s myport:%d\ntargetIP:%s targetport:%d\nverbose:%d\nthreads:%d\nVector:%llx\n\n",
			   my_ntoa(settings.myip),settings.myport,my_ntoa(settings.targetip),settings.targetport,
			   settings.verbose,settings.threads,myVector.mVector);
	}
	
	mulio.eventloop();
}

void print_usage(void){
	std::cout << "-a [xxx.xxx.xxx.xxx]:target IP" << std::endl;
	std::cout << "-p [x]              :target port. Default:10005" << std::endl;
	std::cout << "-m [x]              :memcached port. Default:11211" << std::endl;
	std::cout << "-t [x]              :number of threads" << std::endl;
	std::cout << "-v                  :verbose mode" << std::endl;
	std::cout << "-P [x]              :use port" << std::endl;
	std::cout << "-h                  :print this message" << std::endl;
}
void settings_init(void){
	settings.myport = 10005;
	settings.memcacheport = 11211;
	settings.targetip = 0;
	settings.targetport = 10005;
	settings.verbose = 3;
	settings.threads = 4;
}
