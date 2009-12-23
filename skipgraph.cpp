

#include "skipgraph.h"
#include <pthread.h>

#include <iostream>

#include <list>
#include <set>
#include <map>
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
#include "memcache_buffer.h"
#include "mytcplib.h"
#include "aso.hpp"

#ifdef DEBUG
//void* patient;
#endif


typedef sg_neighbor<defkey> sg_Neighbor;
typedef neighbor_list<defkey> neighbor_List;
mulio mulio,memcached;
aso gAsync_out;
membership_vector myVector;
neighbor_list<defkey> gNeighborList;
node_list<defkey,defvalue> gNodeList;
std::list<address> gAddressList;
std::multimap<defkey,suspend<defkey,defvalue>*> gStoreSuspend;


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
	
	//fprintf(stderr,"new address %s:%d socket:%d\n",my_ntoa(ad->mIP),ad->mPort,ad->mSocket);
	return ad;
}

void print_usage(void);
void settings_init(void);


int send_to_address(const address* ad,const char* buff,const int bufflen){
	int sendsize;
	assert(ad->mSocket != 0);
	
	sendsize = write(ad->mSocket,buff,bufflen);
	if(sendsize<=0){
		//fprintf(stderr,"send_to_address:failed to write %d\n",ad->mSocket);
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
	sg_Node *targetnode;//,*newnode;
	char left_or_right;
	std::list<address>::iterator AddressIt;
	std::multimap<defkey,suspend<defkey,defvalue>*>::iterator SuspendIt;
	address *targetaddress,*originaddress;
	
	defkey rKey;//received Key
	defvalue rValue;//received Value
	
	/*
	if(settings.verbose>1)
		fprintf(stderr,"socket:%d ",socket);
	*/
	
	int EndFlag = 0,DeleteFlag = 0;
	while(EndFlag == 0){
		chklen = read(socket,&op,1);
		if(chklen <= 0){
			//fprintf(stderr," closed %d",socket);
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
				fprintf(stderr,"Search op\n");
			}
			read(socket,&targetid,8);
			targetnode = gNodeList.search_by_id(targetid);
			assert(targetnode!=NULL);
			
			rKey.Receive(socket);
			read(socket,&targetlevel,4);
			read(socket,&targetip,4);
			read(socket,&targetport,2); 
			//fprintf(stderr,"received searchkey:%s mykey:%s\n",rKey.toString(),targetnode->getKey().toString());
			
			if(rKey == targetnode->getKey()){
				//send FoundOP
				if(settings.verbose>1)
					fprintf(stderr,"found in ID:%lld\n",targetnode->mId);
				//prepare buffer
				buffindex = 0;
				bufflen = 1 + targetnode->getKey().size() + targetnode->getValue().size();
				buff = (char*)malloc(bufflen);
				//serialize
				buff[buffindex++] = FoundOp;
				buffindex+=targetnode->getKey().Serialize(&buff[buffindex]);
				buffindex+=targetnode->getValue().Serialize(&buff[buffindex]);
				
				connect_send_close(targetip,targetport,buff,bufflen);
				free(buff);
			}else{
				//fprintf(stderr,"%s : %s ?\n",rKey.toString(), targetnode->getKey().toString());
				if(rKey > targetnode->getKey()){
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
					bufflen = 1 + rKey.size() + targetnode->getKey().size();
					buff = (char*)malloc(bufflen);
					//serialize
					buff[buffindex] = NotfoundOp;
					buffindex+=1;
					buffindex+=rKey.Serialize(&buff[buffindex]);
					buffindex+=targetnode->getKey().Serialize(&buff[buffindex]);
	  
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
				fprintf(stderr,"LinkOP");
			
			read(socket,&targetid,8);
			targetnode = gNodeList.search_by_id(targetid);
			
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
			
			
			if(targetlevel == 0){
				SuspendIt = gStoreSuspend.find(targetnode->getKey());
				SuspendIt->second->decrement_cnt();
				//fprintf(stderr,"suspend decremented:%d\n",SuspendIt->second->getCounter());
				if(SuspendIt->second->send_if_can()){
					gStoreSuspend.erase(SuspendIt);
					//fprintf(stderr,"done\n\n");
				}
			}
			
			if(settings.verbose>1)
				fprintf(stderr,"target:%s from:%s   targetlevel:%d\n",targetnode->getKey().toString(),rKey.toString(),targetlevel);
			
			//EndFlag = 1;
			break;
		case FoundOp:
			rKey.Receive(socket);
			SuspendIt = gStoreSuspend.find(rKey);
			SuspendIt->second->receive_value(socket,rKey);
			if(SuspendIt->second->send_if_can()){
				gStoreSuspend.erase(SuspendIt);
			}
			
			//fprintf(stderr,"key:%s found!  value:%s\n",rKey.toString(),rValue.toString());
			EndFlag = 1;
			break;
		case NotfoundOp:
			rKey.Receive(socket);
			SuspendIt = gStoreSuspend.find(rKey);
			SuspendIt->second->decrement_cnt();
			if(SuspendIt->second->send_if_can()){
				gStoreSuspend.erase(SuspendIt);
			}
			
			//fprintf(stderr,"key:%s not found ! ",rKey.toString());
			rKey.Receive(socket);
			//fprintf(stderr,"nearest key:%s\n",rKey.toString());
			EndFlag = 1;
			break;
		case SetOp:
			if(settings.verbose>1)
				fprintf(stderr,"SetOP ");
			
			rKey.Receive(socket);
			rValue.Receive(socket);
			
			targetnode = gNodeList.search_by_key(rKey);
			if(targetnode->getKey() == rKey){
				targetnode->changeValue(rValue);
			}
			//targetnode = gNodeList.set_to_graph(rKey,rValue,0);//the node to introduce to
			/*
			// Build up list with memvership vector
			buffindex = 0;
			bufflen = 1+8+newnode->getKey().size()+4+8+2+8;
			buff = (char*)malloc(bufflen);
			buff[buffindex++] = TreatOp;
			
			if(targetnode){
				serialize_longlong(buff,&buffindex,targetnode->mId);
			}else{
				serialize_longlong(buff,&buffindex,0);
			}
			buffindex += newnode->getKey().Serialize(&buff[buffindex]);
			serialize_int(buff,&buffindex,settings.myip);
			serialize_longlong(buff,&buffindex,newnode->mId);
			serialize_short(buff,&buffindex,settings.myport);
			serialize_longlong(buff,&buffindex,myVector.mVector);
			assert(bufflen == buffindex && "buffsize ok?");
			chklen = 0;
			if(targetnode){
				if(targetnode->mRight[0]){
					send_to_address(targetnode->mRight[0]->mAddress,buff,bufflen);
				}else if(targetnode->mLeft[0]){
					send_to_address(targetnode->mLeft[0]->mAddress,buff,bufflen);
				}else{
					assert("bad node selected");
				}
			}else {
				for(AddressIt = gAddressList.begin();AddressIt != gAddressList.end();++AddressIt){
					if(AddressIt->mIP == settings.myip && gNodeList.empty()){
						continue;
					}
					//fprintf(stderr,"trying:%s .. ",my_ntoa(AddressIt->mIP));
					chklen = send_to_address(&*AddressIt,buff,bufflen);
					if(chklen > 0){
						//fprintf(stderr,"ok\n");
						break;
					}else{
						//fprintf(stderr,"NG,try next address..\n");
					}
				}
			}
			if(chklen <= 0){
				fprintf(stderr,"\n All Address tried but failed.\n");
			}
			
			free(buff);
			*/
			//fprintf(stderr,"key:%s ,value:%s set in ID:%lld\n",rKey.toString(),rValue.toString(),newnode->mId);
			gNodeList.print();
			//fprintf(stderr,"end of SetOP\n");
			
			EndFlag = 1;
			//DeleteFlag = 1;
			break;
		case TreatOp://targetid,key,originip,originid,originport
			// if you are nearest to origin, connect origin in level 0
			if(settings.verbose>1){
				fprintf(stderr,"TreatOP\n");
			}
			
			read(socket,&targetid,8);
			targetnode = gNodeList.search_by_id(targetid);
			
			rKey.Receive(socket);
			read(socket,&originip,4);
			read(socket,&originid,8);
			read(socket,&originport,2);
			read(socket,&originvector,8);
			
			
			//fprintf(stderr,"ID%lld:treating key:%s length:%d\n IP:%sn",targetnode->mId,rKey.toString(),rKey.size(),my_ntoa(originip));
			
			if(rKey == targetnode->getKey() && originip == settings.myip && originport == settings.myport && originvector == myVector.mVector){
				assert("boomerang of TreatOP");
				//TODO
			}
			if(rKey == targetnode->getKey()){
				if(settings.verbose>1)
					fprintf(stderr,"received key:%s but I already have it\n",rKey.toString());
				//over write? <- TODO
				
			}else{
				targetlevel = MAXLEVEL-1;
				if(rKey > targetnode->getKey()){
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
						//fprintf(stderr,"TreatOP to address %s:%d socket:%d\n",my_ntoa(targetnode->mRight[targetlevel]->mAddress->mIP),targetnode->mRight[targetlevel]->mAddress->mPort,targetnode->mRight[targetlevel]->mAddress->mSocket);
					}else{
						send_to_address(targetnode->mLeft[targetlevel]->mAddress,buff,bufflen);
						//fprintf(stderr,"TreatOP to address %s:%d socket:%d\n",my_ntoa(targetnode->mLeft[targetlevel]->mAddress->mIP),targetnode->mLeft[targetlevel]->mAddress->mPort,targetnode->mLeft[targetlevel]->mAddress->mSocket);
					}
					
					
					free(buff);
					EndFlag = 1;
				}else{
					if(settings.verbose>1)
						fprintf(stderr,"finally treated by ID:%lld key:%s\n",targetnode->mId,targetnode->getKey().toString());
					
					// begin treating new node
					
					//send IntroduceOP to opposite site
					targetlevel = 0;
					buffindex = 0;
					bufflen = 1+8+rKey.size()+4+8+2+4+8;// [OP ID key originIP originID originPort level vector]
					buff = (char*)malloc(bufflen);
					buff[buffindex++] = IntroduceOp;
					if(left_or_right == Left && targetnode->mLeft[0]){
						//fprintf(stderr,"target ID:%lld\n",targetnode->mLeft[0]->mId);
						serialize_longlong(buff,&buffindex,targetnode->mLeft[0]->mId);
					}else if(left_or_right == Right && targetnode->mRight[0]) {
						//fprintf(stderr,"target ID:%lld\n",targetnode->mRight[0]->mId);
						serialize_longlong(buff,&buffindex,targetnode->mRight[0]->mId);
					}else{
						//fprintf(stderr,"no node to introduce from %s\n",targetnode->getKey().toString());
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
							fprintf(stderr,"sending introduceOp from %s to %s by socket:%d\n",targetnode->getKey().toString(),targetnode->mLeft[0]->mKey.toString(),targetnode->mLeft[0]->mAddress->mSocket);
						}else if(left_or_right == Right){
							fprintf(stderr,"sending introduceOp from %s to %s by socket:%d\n",targetnode->getKey().toString(),targetnode->mRight[0]->mKey.toString(),targetnode->mRight[0]->mAddress->mSocket);
						}
					}
					free(buff);
	  
					//LinkOp to new node
					
					//decide how much level to link
					targetlevel = myVector.compare(originvector);
					//fprintf(stderr,"vector1:%llx\nvector2:%llx\n%d bit equal\n",myVector.mVector,originvector,targetlevel);
					bufflen = 1+8+targetnode->getKey().size()+4+8+2+4+1;
					buff = (char*)malloc(bufflen);
					buffindex = 0;
					//serialize
					buff[buffindex++] = LinkOp;
					serialize_longlong(buff,&buffindex,originid);
					buffindex += targetnode->getKey().Serialize(&buff[buffindex]);
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
					//fprintf(stderr,"LinkOP to address %s:%d socket:%d\n",my_ntoa(targetaddress->mIP),targetaddress->mPort,targetaddress->mSocket);
					
					for(int i=0;i<=targetlevel;i++){
						serialize_int(buff,&buffindex,i);
						buff[buffindex++] = left_or_right == Left ? Right : Left;
						send_to_address(targetaddress,buff,bufflen);
						buffindex -= sizeof(targetlevel)+1;
						
						if(left_or_right == Left){
							targetnode->mLeft[i] = gNeighborList.retrieve(rKey,originid,targetaddress);
						}else{
							targetnode->mRight[i] = gNeighborList.retrieve(rKey,originid,targetaddress);
						}
						if(settings.verbose>1){
							fprintf(stderr,"Link from %s to %s at level %d\n",targetnode->getKey().toString(),rKey.toString(),i);
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
			targetnode = gNodeList.search_by_id(targetid);
			if(targetnode == NULL){
				//fprintf(stderr,"there is no node such ID:%lld\n",targetid);
				break;
			}
			//fprintf(stderr,"found %lld key:%s\n",targetnode->mId,targetnode->getKey().toString());
			
			rKey.Receive(socket);
			read(socket,&originip,4);
			read(socket,&originid,8);
			read(socket,&originport,2);
			read(socket,&originlevel,4);
			read(socket,&originvector,8);
			
			if(rKey < targetnode->getKey()){
				left_or_right = Left;
			}else {
				left_or_right = Right;
			}
			//LinkOp to new node
			targetlevel = myVector.compare(originvector);
			
			bufflen = 1+8+targetnode->getKey().size()+4+8+2+4+1;
			buff = (char*)malloc(bufflen);
			//prepare
			buffindex = 0;
			//serialize
			buff[buffindex++] = LinkOp;
			serialize_longlong(buff,&buffindex,originid);
			buffindex += targetnode->getKey().Serialize(&buff[buffindex]);
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
				//printf("Link from %s to %s at level %d in socket:%d\n",targetnode->getKey().toString(),rKey.toString(),i,targetaddress->mSocket);
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
					/*
					if(left_or_right == Left && targetnode->mRight[targetlevel]){
						fprintf(stderr,"relay IntroduceOP from %s to %s at level %d in socket:%d\n",targetnode->getKey().toString(),targetnode->mRight[targetlevel]->mKey.toString(),targetlevel,targetnode->mRight[targetlevel]->mAddress->mSocket);
					}else if(left_or_right == Right && targetnode->mLeft[targetlevel]){
						fprintf(stderr,"relay IntroduceOP from %s to %s at level %d in socket:%d\n",targetnode->getKey().toString(),targetnode->mLeft[targetlevel]->mKey.toString(),targetlevel,targetnode->mLeft[targetlevel]->mAddress->mSocket);
					}else{
						fprintf(stderr,"end of relay\n");
					}
					//*/
				}
				
			}
			//fprintf(stderr,"end of Introduce Op\n");
			EndFlag = 1;
			break;
		case ViewOp:
			//fprintf(stderr,"view\n");
			gNodeList.print();
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



/*
	TOKENMAX,
	SET_KEY,
	SET_FLAGS,
	SET_EXPTIME,
	SET_LENGTH,
	SET_VALUE,
	GET_KEY,
	DELETE_KEY,
	
	state_free,
	state_set,
	state_get,
	state_delete,
	state_value, // wait until n byte receive
	state_continue, // not all data received
	state_close,
	state_error,
*/

std::map<int,memcache_buffer*> gMemcachedSockets;

int memcached_thread(const int socket){
	std::map<int,memcache_buffer*>::iterator bufferIt;
	memcache_buffer* buf;
	char* buff;
	int bufflen,chklen;
	int tokennum;
	int DeleteFlag;
	defkey* targetkey;
	defvalue* targetvalue;
	sg_Node *targetnode,*newnode;
	std::multimap<defkey,suspend<defkey,defvalue>*>::iterator suspendIt;
	suspend<defkey,defvalue>* newSuspend;
	std::list<address>::iterator AddressIt;
	
	//test
	char* data;
	int datalen,dataindex;
	
	if(settings.verbose > 2)
		fprintf(stderr,"memcached client arrived socket:%d\n",socket);
	bufferIt = gMemcachedSockets.find(socket);
	if(bufferIt == gMemcachedSockets.end()){
		buf = new memcache_buffer(socket);
		gMemcachedSockets.insert(std::pair<int,memcache_buffer*>(socket,buf));
	}else {
		buf = bufferIt->second;
	}
	
	tokennum = buf->receive();
	
	DeleteFlag = 0;
	switch(buf->getState()){
	case memcache_buffer::state_set:
		//fprintf(stderr,"set!![%lld] ",gId);
		
		targetkey = new defkey(buf->tokens[SET_KEY].str,buf->tokens[SET_KEY].length);
		targetvalue = new defvalue(buf->tokens[SET_VALUE].str,buf->tokens[SET_VALUE].length);
		
		//fprintf(stderr,"key:%s, value:%s, length:%s\n",buf->tokens[SET_KEY].str,buf->tokens[SET_VALUE].str,buf->tokens[SET_LENGTH].str);
		
		// Build up list with memvership vector
		targetnode = gNodeList.search_by_key(*targetkey);
		if(targetnode->getKey() == *targetkey){
			//already that key exists
			targetnode->changeValue(*targetvalue);
			delete targetkey;
			delete targetvalue;
			//fprintf(stderr,"not saved\n");
			write(socket,"STORED\r\n",8);
			buf->ParseOK();
			break;
		}
		newnode = new sg_Node(*targetkey,*targetvalue);
		
		// search suspending socket
		suspendIt = gStoreSuspend.find(*targetkey);
		if(suspendIt == gStoreSuspend.end()){
			newSuspend = new suspend<defkey,defvalue>(socket,2);
			gStoreSuspend.insert(std::pair<defkey,suspend<defkey,defvalue>*>(*targetkey,newSuspend));
			//fprintf(stderr,"new suspend counter:%d\n",newSuspend->getCounter());
		}else {
			newSuspend = suspendIt->second; // if exist, append new object.
		}
		newSuspend->add("STORED\r\n");
		
		//send the data
		gNodeList.insert(newnode);
		if(targetnode){
			bufflen = create_treatop(&buff,targetnode->mId,targetkey,newnode->mId,myVector.mVector);
			if(targetnode->mRight[0]){
				send_to_address(targetnode->mRight[0]->mAddress,buff,bufflen);
			}else if(targetnode->mLeft[0]){
				send_to_address(targetnode->mLeft[0]->mAddress,buff,bufflen);
			}else{
				assert("bad node selected");
			}
		}else{
			bufflen = create_treatop(&buff,0,targetkey,newnode->mId,myVector.mVector);
			for(AddressIt = gAddressList.begin();AddressIt != gAddressList.end();++AddressIt){
				if(AddressIt->mIP == settings.myip && gNodeList.empty()){
					continue;
				}
				//fprintf(stderr,"trying:%s .. ",my_ntoa(AddressIt->mIP));
				chklen = send_to_address(&*AddressIt,buff,bufflen);
				if(chklen > 0){
					//fprintf(stderr,"ok\n");
					break;
				}else{
					//fprintf(stderr,"NG,try next address..\n");
				}
			}
			if(chklen <= 0){
				//fprintf(stderr,"\n All Address tried but failed.\n");
			}
		}
		free(buff);
		
		delete targetkey;
		delete targetvalue;
		//gNodeList.print();

		//gAsync_out.send(socket,"STORED\r\n",8);
		
		buf->ParseOK();
		break;
	case memcache_buffer::state_get:
		//fprintf(stderr,"get!!\n");
		//fprintf(stderr,"key:%s\n",buf->tokens[GET_KEY].str);
		
		// search suspending socket
		suspendIt = gStoreSuspend.find(socket);
		if(suspendIt == gStoreSuspend.end()){
			newSuspend = new suspend<defkey,defvalue>(socket);
			gStoreSuspend.insert(std::pair<defkey,suspend<defkey,defvalue>*>(socket,newSuspend));
		}else {
			newSuspend = suspendIt->second;
		}
		
		for(int i=0;i<tokennum;i++){
			buf->tokens[i].str[buf->tokens[i].length] = '\0';
			targetkey = new defkey(buf->tokens[i].str);
			
			targetnode = gNodeList.search_by_key(*targetkey);
			//fprintf(stderr,"key:%s search\n",targetkey->toString());
			if(targetnode->getKey() > *targetkey){ // not found!
				fprintf(stderr,"nearest key:%s\n",targetnode->getKey().toString());
				//sending SearchOp
				dataindex = 0;
				datalen = 1 + 8 + 4 + buf->tokens[GET_KEY].length + 4 + 4 + 2;
				data = (char*)malloc(datalen);
				data[dataindex++] = SearchOp;
				serialize_longlong(data,&dataindex,0);
				serialize_int(data,&dataindex,buf->tokens[i].length);
				memcpy(&data[dataindex],buf->tokens[i].str,buf->tokens[GET_KEY].length);
				serialize_int(data,&dataindex,MAXLEVEL);
				serialize_int(data,&dataindex,settings.myip);
				serialize_short(data,&dataindex,settings.memcacheport);
				gAsync_out.send(targetnode->mLeft[0]->mAddress->mSocket,data,dataindex);
				
				newSuspend->add(*targetkey);
			}else{
				newSuspend->add(targetnode->getValue().mValue);
				//fprintf(stderr,"key:%s found\n",buf->tokens[i].str);
				newSuspend->add("\r\n");
			}
			delete targetkey;
		}
		newSuspend->add("END\r\n");
		
		if(newSuspend->send_if_can() == true){
			gStoreSuspend.erase(socket);
			delete newSuspend;
		}
		
		buf->ParseOK();
		break;
		
	case memcache_buffer::state_delete:
		//fprintf(stderr,"delete!!\n");
		buf->ParseOK();
		break;
	case memcache_buffer::state_close:
		//fprintf(stderr,"close!!\n");
		delete buf;
		gMemcachedSockets.erase(socket);
		close(socket);
		DeleteFlag = 1;
		fprintf(stderr,"closed\n");
		break;
	case memcache_buffer::state_error:
		fprintf(stderr,"error!!\n");
		gMemcachedSockets.erase(socket);
		close(socket);
		DeleteFlag = 1;
		break;
	}
	return DeleteFlag;
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
	defvalue dummy("hoge");
	
	settings_init();
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
	
	// set accepting thread
	int listening = create_tcpsocket();
	set_reuse(listening);
	bind_inaddr_any(listening,settings.myport);
	listen(listening,2048);
	mulio.SetAcceptSocket(listening);
	mulio.SetCallback(main_thread);
	mulio.run();// accept thread
	
	if(settings.targetip == 0){ // I am master
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
		gNodeList.insert(minnode);
		gNodeList.insert(maxnode);
		fprintf(stderr,"min:ID%lld  max:ID%lld level:%d\n",minnode->mId,maxnode->mId,MAXLEVEL);
		
		connect_port_ip(myself,settings.myip,settings.myport);
		mulio.SetSocket(myself);
	}
	gNodeList.print();
	
	gAsync_out.run(2); // Asynchronous output manager with 2 threads
	
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
		fprintf(stderr,"master is %s:%d\n\n",my_ntoa(settings.targetip),settings.targetport);
		targetsocket = create_tcpsocket();
		newAddress = new address(targetsocket,settings.targetip,settings.targetport);
		gAddressList.push_back(*newAddress);
		connect_port_ip(targetsocket,settings.targetip,settings.targetport);
		mulio.SetSocket(targetsocket);
	}else{
		// set myself
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
	settings.verbose = 0;
	settings.threads = 4;
}
