

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
#include "memcache_buffer.h"
#include "aso.hpp"

#ifdef DEBUG
//void* patient;
#endif

//#define DEBUG_MODE
#ifdef DEBUG_MODE
#define DEBUG_OUT(...) fprintf(stderr,__VA_ARGS__)
#else
#define DEBUG_OUT(...)
#endif


typedef sg_neighbor<defkey> sg_Neighbor;
typedef neighbor_list<defkey> neighbor_List;
mulio mulio,memcached;
aso gAsync_out;
membership_vector myVector;
neighbor_list<defkey> gNeighborList;
node_list<defkey,defvalue> gNodeList;
std::list<address> gAddressList;
rquery_list gRangeQueryList;
suspend_list<defkey,defvalue> gSuspendList;

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
	suspend<defkey,defvalue>* sus;
	address *targetaddress,*originaddress;
	char left_closed,right_closed;
	range_query range_found,range_search;
	
	defkey rKey;// received Key
	defkey sKey;// secondary received Key
	
	defvalue rValue;//received Value
	int tempFlag;
	
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
			//DEBUG_OUT("received searchkey:%s mykey:%s\n",rKey.toString(),targetnode->getKey().toString());
			
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
				buffindex += targetnode->getKey().Serialize(&buff[buffindex]);
				buffindex += targetnode->getValue().Serialize(&buff[buffindex]);
				assert(buffindex == bufflen);
				
				originaddress = search_from_addresslist(targetip,targetport);
				if(originaddress != NULL){
					send_to_address(originaddress,buff,bufflen);
				}else{
					connect_send_close(targetip,targetport,buff,bufflen);
				}
				free(buff);
			}else{
				DEBUG_OUT("%s : %s ?\n",rKey.toString(), targetnode->getKey().toString());
				if(rKey > targetnode->getKey()){
					//send SearchOP to Rightside
					left_or_right = Right;
					targetnode->dump();
					for(;targetlevel>=0;targetlevel--){
						if(!(targetnode->mRight[targetlevel])) continue;
						DEBUG_OUT("r compare:%s : %s ?\n" ,targetnode->mRight[targetlevel]->mKey.toString(),rKey.toString());
						if(!(targetnode->mRight[targetlevel]->mKey > rKey)) break;
					}
				}else{
					//send SearchOcP to Leftside
					left_or_right = Left;
					for(;targetlevel>=0;targetlevel--){
						if(!(targetnode->mLeft[targetlevel]))continue;
						DEBUG_OUT("l compare:%s : %s ?\n",rKey.toString(), targetnode->mLeft[targetlevel]->mKey.toString());
						if(!(targetnode->mLeft[targetlevel]->mKey < rKey)) break;
					}
				}
				DEBUG_OUT("level:%d\n",targetlevel);
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
		case RangeOp:// [op] id level [beginkey] [endkey] [left_closed] [right_closed] [originip] [originport] [origin_range_query]
			if(settings.verbose>1)
				fprintf(stderr,"RangeOP ");
			
			read(socket,&targetid,8);
			targetnode = gNodeList.search_by_id(targetid);
			assert(targetnode!=NULL);
			read(socket,&targetlevel,4);
			
			rKey.Receive(socket); // begin key
			sKey.Receive(socket); // end key
			
			read(socket,&left_closed,1); // 0->open(<) 1->closed(<=)
			read(socket,&right_closed,1); // 0->open(<) 1->cloaed(<=)
			
			// managers address
			read(socket,&targetip,4);
			read(socket,&targetport,2);
			
			// devide tag
			range_search.receive(socket);
			
			if(settings.verbose > 4){
				DEBUG_OUT("received range ");
				print_range(rKey,sKey,left_closed,right_closed);
				DEBUG_OUT(" myKey:%s level:%d ",targetnode->getKey().toString(),targetlevel);
			}
			
			// range forward query
			originlevel = targetlevel;
			if(sKey < targetnode->getKey()){// range is out of mine
				tempFlag = 0;
				for(int i=originlevel;i>=0;i--){
					if(!targetnode->mLeft[i] || targetnode->mLeft[i]->mKey < rKey) continue;
					range_forward(originlevel,targetnode->mLeft[i]->mId,*(targetnode->mLeft[i]->mAddress),rKey,sKey,left_closed,right_closed,targetip,targetport,&range_search,true);
					tempFlag = 1;
					break;
				}
				if(tempFlag == 0){ // not found
					buffindex = 0;
					buff = (char*)malloc(1+range_search.size());
					buff[buffindex++] = RangeNotFoundOp;
					buffindex += range_search.Serialize(&buff[buffindex]);
					
					originaddress = search_from_addresslist(targetip,targetport);
					if(originaddress != NULL){
						send_to_address(originaddress,buff,buffindex);
					}else{
						connect_send_close(targetip,targetport,buff,buffindex);
					}
					free(buff);
				}
				EndFlag = 1;
				break; // end in this process
			}else if(targetnode->getKey() < rKey){
				tempFlag = 0;
				for(int i=originlevel;i>=0;i--){
					if(!targetnode->mRight[i] || sKey < targetnode->mRight[i]->mKey) continue;
					range_forward(originlevel,targetnode->mRight[i]->mId,*(targetnode->mRight[i]->mAddress),rKey,sKey,left_closed,right_closed,targetip,targetport,&range_search,true);
					tempFlag = 1;
					break;
				}
				if(tempFlag == 0){ // not found
					DEBUG_OUT("pass with not found {%s}\n",range_search.toString());
					buffindex = 0;
					buff = (char*)malloc(1+range_search.size());
					buff[buffindex++] = RangeNotFoundOp;
					buffindex += range_search.Serialize(&buff[buffindex]);
					
					DEBUG_OUT("devided tag:");
					range_search.mTag.dump();
					
					originaddress = search_from_addresslist(targetip,targetport);
					if(originaddress != NULL){
						send_to_address(originaddress,buff,buffindex);
					}else{
						connect_send_close(targetip,targetport,buff,buffindex);
					}
					free(buff);
				}
				EndFlag = 1;
				break;
			}
			
			//left side
			//DEBUG_OUT("left side\n");
			if(targetnode->mLeft[originlevel]) // cut query by my edge 
				if(rKey < targetnode->mLeft[originlevel]->mKey || (left_closed && rKey == targetnode->mLeft[originlevel]->mKey)){
					DEBUG_OUT("cut forward to %s ",targetnode->mLeft[originlevel]->mKey.toString());
					range_forward(originlevel,targetnode->mLeft[originlevel]->mId,*(targetnode->mLeft[originlevel]->mAddress),rKey,targetnode->mLeft[originlevel]->mKey,left_closed,1,targetip,targetport,&range_search,false);
					left_closed = 0;
					rKey = targetnode->mLeft[originlevel]->mKey;
				}
			for(int i=originlevel-1 ;i>=0; i--){
				if(!(targetnode->mLeft[i]) || !(targetnode->mLeft[i]->mKey > rKey)) continue;
				DEBUG_OUT("range forward to %s ",targetnode->mLeft[i]->mKey.toString());
				range_forward(i,targetnode->mLeft[i]->mId,*(targetnode->mLeft[i]->mAddress),rKey,targetnode->mLeft[i]->mKey,left_closed,1,targetip,targetport,&range_search,false);
				left_closed = 0;
				rKey = targetnode->mLeft[i]->mKey;
			}
			if(left_closed && rKey == targetnode->mLeft[0]->mKey){
				range_forward(0,targetnode->mLeft[0]->mId,*(targetnode->mLeft[0]->mAddress),rKey,rKey,1,1,targetip,targetport,&range_search,false);
			}
			//right side
			if(targetnode->mRight[originlevel]) // cut query by my edge 
				if(targetnode->mRight[originlevel]->mKey < sKey || (right_closed && sKey == targetnode->mRight[originlevel]->mKey)){
					DEBUG_OUT("cut forward to %s ",targetnode->mRight[originlevel]->mKey.toString());
					range_forward(originlevel,targetnode->mRight[originlevel]->mId,*(targetnode->mRight[originlevel]->mAddress),targetnode->mRight[originlevel]->mKey,sKey,1,right_closed,targetip,targetport,&range_search,false);
					right_closed = 0;
					sKey = targetnode->mRight[originlevel]->mKey;
				}
			for(int i=originlevel-1 ;i>=0; i--){
				if(!(targetnode->mRight[i]) || !(sKey > targetnode->mRight[i]->mKey)) continue;
				DEBUG_OUT("range forward to %s ",targetnode->mRight[i]->mKey.toString());
				range_forward(i,targetnode->mRight[i]->mId,*(targetnode->mRight[i]->mAddress),targetnode->mRight[i]->mKey,sKey,1,right_closed,targetip,targetport,&range_search,false);
				right_closed = 0;
				sKey = targetnode->mRight[i]->mKey;
			}
			if(right_closed && sKey == targetnode->mRight[0]->mKey){
				range_forward(0,targetnode->mRight[0]->mId,*(targetnode->mRight[0]->mAddress),sKey,sKey,1,1,targetip,targetport,&range_search,false);
			}
			
			// send RangeFoundOp to origin
			buffindex = 0;
			bufflen = 1 + targetnode->getKey().size() + targetnode->getValue().size() + range_search.size();
			buff = (char*)malloc(bufflen);
			buff[buffindex++] = RangeFoundOp;
			buffindex += range_search.Serialize(&buff[buffindex]);
			buffindex += targetnode->getKey().Serialize(&buff[buffindex]);
			buffindex += targetnode->getValue().Serialize(&buff[buffindex]);
			
			assert(buffindex == bufflen);
			
			
			originaddress = search_from_addresslist(targetip,targetport);
			if(originaddress != NULL){
				send_to_address(originaddress,buff,bufflen);
			}else{
				connect_send_close(targetip,targetport,buff,bufflen);
			}
			free(buff);
			DEBUG_OUT("found %s\n",targetnode->getKey().toString());
			if(sKey == rKey) {
				EndFlag = 1;
				break;
			}
			
			EndFlag = 1;
			break;
		case LinkOp://id,key,originip,originid,originport,level,LorR
			if(settings.verbose>1)
				fprintf(stderr,"LinkOP ");
 
			read(socket,&targetid,8);
			targetnode = gNodeList.search_by_id(targetid);
			
			rKey.Receive(socket);
			read(socket,&originip,4);
			read(socket,&originid,8);
			read(socket,&originport,2); 
			read(socket,&targetlevel,4);
			read(socket,&left_or_right,1);
			if(targetnode == NULL){
				DEBUG_OUT("ID:%lld not found.\n",targetid);
				DEBUG_OUT("key:%s level:%d\n",rKey.toString(),targetlevel);
				gNodeList.print();
			}
			
			originaddress = search_from_addresslist(originip,originport);
			if(originaddress == NULL){
				originaddress = add_new_address(socket,originip,originport);
				mulio.SetSocket(socket);
			}
			
			DEBUG_OUT("target:%s from:%s\ttargetlevel:%d\n",targetnode->getKey().toString(),rKey.toString(),targetlevel);
			
			if(left_or_right == Left){
				targetnode->mLeft[targetlevel] = gNeighborList.retrieve(rKey,originid,originaddress);
			}else{
				targetnode->mRight[targetlevel] = gNeighborList.retrieve(rKey,originid,originaddress);
			}
			
			if(targetlevel == 0){
				sus = gSuspendList.search(targetnode->getKey());
				if(sus){
					sus->decrement_cnt();
					if(sus->send_if_can()){
						gSuspendList.erase(targetnode->getKey());
						//fprintf(stderr,"done\n\n");
					}
				}else{
					DEBUG_OUT("suspend not found!\n");
				}
			}
			if(settings.verbose>1)
				fprintf(stderr,"target:%s from:%s   targetlevel:%d\n",targetnode->getKey().toString(),rKey.toString(),targetlevel);
			
			EndFlag = 1;
			break;
		case FoundOp:
			if(settings.verbose>1)
				fprintf(stderr,"  FoundOP ");
			rKey.Receive(socket);
			sus = gSuspendList.search(rKey);
			if(sus != NULL){
				sus->receive_value(socket,rKey);
				if(sus->send_if_can()){
					gSuspendList.erase(rKey);
				}
			}else{
				rValue.Receive(socket);
			}
			fprintf(stderr,"key:%s found!\n",rKey.toString());
			EndFlag = 1;
			break;
		case RangeFoundOp:// [op] [range_query] [Foundkey] [FoundValue]
			if(settings.verbose>1)
				fprintf(stderr,"  RangeFoundOP ");
			range_found.receive(socket);
			DEBUG_OUT("range %s  ",range_found.toString());
			range_found.mTag.dump();
			gRangeQueryList.found(range_found,socket);
			
			EndFlag = 1;
			break;
		case RangeNotFoundOp:// [op] [range_query]
			if(settings.verbose>1)
				fprintf(stderr,"  RangeNotFoundOP ");
			range_found.receive(socket);
			gRangeQueryList.notfound(range_found);
			
			EndFlag = 1;
			break;
		case NotfoundOp:
			if(settings.verbose>1)
				fprintf(stderr,"NotfoundOP ");
			rKey.Receive(socket);
			
			sus = gSuspendList.search(rKey);
			assert(sus != NULL);
			sus->decrement_cnt();
			if(sus->send_if_can()){
				gSuspendList.erase(rKey);
			}
			
			//fprintf(stderr,"key:%s not found ! ",rKey.toString());
			rKey.Receive(socket);
			//fprintf(stderr,"nearest key:%s\n",rKey.toString());
			EndFlag = 1;
			DEBUG_OUT("ok\n");
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
				assert(!"boomerang of TreatOP");
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
					if(left_or_right == Left && targetnode->mLeft[0]){
						DEBUG_OUT("opposite introduceOp from %s to %s by socket:%d\n",targetnode->getKey().toString(),targetnode->mLeft[0]->mKey.toString(),targetnode->mLeft[0]->mAddress->mSocket);
						send_introduceop(*targetnode->mLeft[0]->mAddress,targetnode->mLeft[0]->mId,rKey,originid,originip,originport,-1,originvector);
					}else if(left_or_right == Right && targetnode->mRight[0]) {
						DEBUG_OUT("opposite introduceOp from %s to %s by socket:%d\n",targetnode->getKey().toString(),targetnode->mRight[0]->mKey.toString(),targetnode->mRight[0]->mAddress->mSocket);
						send_introduceop(*targetnode->mRight[0]->mAddress,targetnode->mRight[0]->mId,rKey,originid,originip,originport,-1,originvector);
					}else{
						assert(targetnode->getKey().isMaximum() || targetnode->getKey().isMinimum());
					}
					
					//LinkOp to new node
					//decide how much level to link
					targetlevel = myVector.compare(originvector);
					myVector.printVector();
					DEBUG_OUT("level:%d matched\n",targetlevel);
					targetlevel = targetlevel < MAXLEVEL-1 ? targetlevel : MAXLEVEL-1;
					
					// retrieve address
					targetaddress = search_from_addresslist(originip,originport);
					if(targetaddress == NULL){
						newsocket = create_tcpsocket();
						connect_port_ip(newsocket,originip,originport);
						targetaddress = add_new_address(newsocket,originip,originport);
						mulio.SetSocket(newsocket);
					}
					
					DEBUG_OUT("LinkOP to address %s:%d socket:%d\n",my_ntoa(targetaddress->mIP),targetaddress->mPort,targetaddress->mSocket);
					for(int i=0;i<=targetlevel;i++){
						send_linkop(*targetaddress,originid,targetnode->getKey(),targetnode->mId,i,inverse(left_or_right));
						if(left_or_right == Left){
							targetnode->mLeft[i] = gNeighborList.retrieve(rKey,originid,targetaddress);
						}else{
							targetnode->mRight[i] = gNeighborList.retrieve(rKey,originid,targetaddress);
						}
						if(settings.verbose>1){
							fprintf(stderr,"Link from %s to %s at level %d/%d\n",targetnode->getKey().toString(),rKey.toString(),i,targetlevel);
						}
					}
					
					if(targetlevel < MAXLEVEL-1){
						if(left_or_right == Left){
							if(targetnode->mRight[targetlevel]){
								DEBUG_OUT("start IntroduceOP from %s to %s at level %d in socket:%d\n",targetnode->getKey().toString(),targetnode->mRight[targetlevel]->mKey.toString(),targetlevel,targetnode->mRight[targetlevel]->mAddress->mSocket);
								send_introduceop(*targetnode->mRight[targetlevel]->mAddress,targetnode->mRight[targetlevel]->mId,rKey,originid,originip,originport,targetlevel,originvector);
							}else{
								DEBUG_OUT("end of rightside\n");
							}
						}else{
							if(targetnode->mLeft[targetlevel]){
								DEBUG_OUT("start IntroduceOP from %s to %s at level %d in socket:%d\n",targetnode->getKey().toString(),targetnode->mLeft[targetlevel]->mKey.toString(),targetlevel,targetnode->mLeft[targetlevel]->mAddress->mSocket);
								send_introduceop(*targetnode->mLeft[targetlevel]->mAddress,targetnode->mLeft[targetlevel]->mId,rKey,originid,originip,originport,targetlevel,originvector);
							}else{
								DEBUG_OUT("end of leftside\n");
							}
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
			DEBUG_OUT("to %lld: ",targetid);
			targetnode = gNodeList.search_by_id(targetid);
			if(targetnode == NULL) {
				DEBUG_OUT("not found!\n");
				gNodeList.print();
				assert(!"arienai");
			}
			DEBUG_OUT("[%s] ",targetnode->getKey().toString());
			
			rKey.Receive(socket);
			read(socket,&originip,4);
			read(socket,&originid,8);
			read(socket,&originport,2);
			read(socket,&originlevel,4);
			read(socket,&originvector,8);
			DEBUG_OUT("arrival ID:%lld,key:%s\n",originid,rKey.toString());
			
			left_or_right = direction(targetnode->getKey(), rKey);
			
			//LinkOp to new node
			targetlevel = myVector.compare(originvector);
			targetlevel = targetlevel < MAXLEVEL-1 ? targetlevel : MAXLEVEL-1;
			
			// retrieve address
			targetaddress = search_from_addresslist(originip,originport);
			if(targetaddress == NULL){
				newsocket = create_tcpsocket();
				connect_port_ip(newsocket,originip,originport);
				targetaddress = add_new_address(newsocket,originip,originport);
				mulio.SetSocket(newsocket);
			}
			
			if(left_or_right == Left){
				for(int i=originlevel+1;i<=targetlevel;i++){
					send_linkop(*targetaddress,originid,targetnode->getKey(),targetnode->mId,i,Right);
					targetnode->mLeft[i] = gNeighborList.retrieve(rKey,originid,targetaddress);
					DEBUG_OUT("Link to rightside from %s to %s at level %d in socket:%d\n",targetnode->getKey().toString(),rKey.toString(),i,targetaddress->mSocket);
				}
			}else{
				for(int i=originlevel+1;i<=targetlevel;i++){
					send_linkop(*targetaddress,originid,targetnode->getKey(),targetnode->mId,i,Left);
					targetnode->mRight[i] = gNeighborList.retrieve(rKey,originid,targetaddress);
					DEBUG_OUT("Link to leftside from %s to %s at level %d in socket:%d\n",targetnode->getKey().toString(),rKey.toString(),i,targetaddress->mSocket);
				}
			}
			
			if(targetlevel < MAXLEVEL-1){
				if(left_or_right == Left){
					if(targetnode->mRight[targetlevel]){
						DEBUG_OUT("relay IntroduceOP from %s to %s at level %d in socket:%d\n",targetnode->getKey().toString(),targetnode->mRight[targetlevel]->mKey.toString(),targetlevel,targetnode->mRight[targetlevel]->mAddress->mSocket);
						send_introduceop(*targetnode->mRight[targetlevel]->mAddress,targetnode->mRight[targetlevel]->mId,rKey,originid,originip,originport,targetlevel,originvector);
					}else{
						DEBUG_OUT("end of rightside\n");
					}
				}else{
					if(targetnode->mLeft[targetlevel]){
						fprintf(stderr,"relay IntroduceOP from %s to %s at level %d in socket:%d\n",targetnode->getKey().toString(),targetnode->mLeft[targetlevel]->mKey.toString(),targetlevel,targetnode->mLeft[targetlevel]->mAddress->mSocket);
						send_introduceop(*targetnode->mLeft[targetlevel]->mAddress,targetnode->mLeft[targetlevel]->mId,rKey,originid,originip,originport,targetlevel,originvector);
					}else{
						DEBUG_OUT("end of leftside\n");
					}
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
	int bufflen,chklen,buffindex;
	int tokennum;
	int DeleteFlag;
	defkey* targetkey,*beginkey,*endkey;
	defvalue* targetvalue;
	sg_Node *targetnode,*newnode;
	std::multimap<defkey,suspend<defkey,defvalue>*>::iterator suspendIt;
	suspend<defkey,defvalue>* suspending;
	std::list<address>::iterator AddressIt;
	range_query origin_query;
	
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
		fprintf(stderr,"set!![%lld] ",gId);
		
		targetkey = new defkey(buf->tokens[SET_KEY].str,buf->tokens[SET_KEY].length);
		targetvalue = new defvalue(buf->tokens[SET_VALUE].str,buf->tokens[SET_VALUE].length);
		if(settings.verbose > 1)
			fprintf(stderr,"key:%s, value:%s, length:%s\n",buf->tokens[SET_KEY].str,buf->tokens[SET_VALUE].str,buf->tokens[SET_LENGTH].str);
		
		// Build up list with memvership vector
		targetnode = gNodeList.search_by_key(*targetkey); // it finds nearest node
		if(targetnode && targetnode->getKey() == *targetkey){
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
		suspending = gSuspendList.add_or_retrieve(socket,2);
		suspending->add("STORED\r\n");
		gSuspendList.setKey(socket,*targetkey);
		
		//send the data
		gNodeList.insert(newnode);
		if(targetnode){
			if(targetnode->getKey() < *targetkey){
				bufflen = create_treatop(&buff,targetnode->mRight[0]->mId,targetkey,newnode->mId,myVector.mVector);
				DEBUG_OUT("set::target node:%lld's right %s\n",targetnode->mId,targetnode->mRight[0]->mKey.toString());
				send_to_address(targetnode->mRight[0]->mAddress,buff,bufflen);
			}else{
				bufflen = create_treatop(&buff,targetnode->mLeft[0]->mId,targetkey,newnode->mId,myVector.mVector);
				DEBUG_OUT("set::target node:%lld's left %s\n",targetnode->mId,targetnode->mLeft[0]->mKey.toString());
				send_to_address(targetnode->mLeft[0]->mAddress,buff,bufflen);
			}
		}else{
			bufflen = create_treatop(&buff,0,targetkey,newnode->mId,myVector.mVector);
			if(AddressIt != gAddressList.end()){
				for(AddressIt = gAddressList.begin();AddressIt != gAddressList.end();++AddressIt){
					if(AddressIt->mIP == settings.myip && gNodeList.empty()){
						continue; // do not boomerang
					}
					DEBUG_OUT("trying:%s .. ",my_ntoa(AddressIt->mIP));
					chklen = send_to_address(&*AddressIt,buff,bufflen);
					if(chklen > 0){
						DEBUG_OUT("ok  ");
						break;
					}else{
						DEBUG_OUT("NG,try next address..\n");
					}
				}
				if(chklen <= 0){
					fprintf(stderr,"\n All Address tried but failed.\n");
				}
			}
		}
		free(buff);
		
		delete targetkey;
		delete targetvalue;
		buf->ParseOK();
		DEBUG_OUT("set end\n");
		break;
	case memcache_buffer::state_get:
		// search suspending socket
		
		suspending = gSuspendList.add_or_retrieve(socket);
		for(int i=0;i<tokennum;i++){// multiget ok
			buf->tokens[i].str[buf->tokens[i].length] = '\0';
			targetkey = new defkey(buf->tokens[i].str);
			targetnode = gNodeList.search_by_key(*targetkey);
			
			if(targetnode->getKey() > *targetkey){ // not found! search for other node
				if(settings.verbose > 3)
					fprintf(stderr,"nearest key:%s\n",targetnode->getKey().toString());
				//sending SearchOp
				dataindex = 0;
				datalen = 1 + 8 + 4 + buf->tokens[i].length + 4 + 4 + 2;
				data = (char*)malloc(datalen);
				data[dataindex++] = SearchOp;
				serialize_longlong(data,&dataindex,targetnode->mLeft[0]->mId);
				
				serialize_int(data,&dataindex,buf->tokens[i].length);
				memcpy(&data[dataindex],buf->tokens[i].str,buf->tokens[i].length);
				dataindex += buf->tokens[i].length;
				
				serialize_int(data,&dataindex,MAXLEVEL-1);
				serialize_int(data,&dataindex,settings.myip);
				serialize_short(data,&dataindex,settings.targetport);
				assert(dataindex == datalen);
				gAsync_out.send(targetnode->mLeft[0]->mAddress->mSocket,data,dataindex);
				
				suspending->add(*targetkey);
				gSuspendList.setKey(socket,*targetkey);
			}else{
				suspending->add("VALUE");
				suspending->add(targetkey->mKey);
				suspending->add(" 0 ");
				suspending->add(targetnode->getValue().mLength);
				suspending->add("\r\n");
				suspending->add(targetnode->getValue().mValue);
				suspending->add("\r\n");
			}
			if(i < tokennum-1){
				delete targetkey;
			}
		}
		suspending->add("END\r\n");
		
		if(suspending->send_if_can()){
			gSuspendList.erase(*targetkey);
		}
		delete targetkey;
		
		buf->ParseOK();
		break;
	case memcache_buffer::state_rget:
		if(settings.verbose > 3)
			fprintf(stderr,"range get query\n");
		beginkey = new defkey(buf->tokens[RGET_BEGIN].str);
		DEBUG_OUT("begin:%s\n",beginkey->toString());
		endkey = new defkey(buf->tokens[RGET_END].str);
		DEBUG_OUT("end:%s\n",endkey->toString());
		
		buffindex = 0;
		buff = (char*)malloc(beginkey->size() + endkey->size() + 5);
		memcpy(&buff[buffindex],buf->tokens[RGET_BEGIN].str,strlen(buf->tokens[RGET_BEGIN].str));
		buffindex += strlen(buf->tokens[RGET_BEGIN].str);
		buff[buffindex++] = ' ';
		memcpy(&buff[buffindex],buf->tokens[RGET_END].str,strlen(buf->tokens[RGET_END].str));
		buffindex += strlen(buf->tokens[RGET_END].str);
		buff[buffindex++] = ' ';
		buff[buffindex++] = buf->tokens[RGET_LEFT_CLOSED].str[0];
		buff[buffindex++] = ' ';
		buff[buffindex++] = buf->tokens[RGET_RIGHT_CLOSED].str[0];
		buff[buffindex] = '\0';
		
		origin_query.mTag.setZero();
		origin_query.set(buff,buffindex);
		gRangeQueryList.set_queue(socket,&origin_query);
		DEBUG_OUT("set query [%s]\n",origin_query.toString());
		origin_query.mTag.init();
		range_forward(MAXLEVEL-1,0,gAddressList.front(),*beginkey,*endkey,buf->tokens[RGET_LEFT_CLOSED].str[0] - '0',buf->tokens[RGET_RIGHT_CLOSED].str[0] - '0', settings.myip, settings.targetport, &origin_query,true);
		free(buff);
		delete beginkey;
		delete endkey;
		buf->ParseOK();
		break;
	case memcache_buffer::state_delete:
		//fprintf(stderr,"delete!!\n");
		buf->ParseOK();
		break;
	case memcache_buffer::state_stats:
		gNodeList.print();
		buf->ParseOK();
		break;
	case memcache_buffer::state_close:
		//fprintf(stderr,"close!!\n");
		delete buf;
		gMemcachedSockets.erase(socket);
		close(socket);
		DeleteFlag = 1;
		if(settings.verbose > 2)
			fprintf(stderr,"closed\n");
		break;
	case memcache_buffer::state_error:
		int i = 0;
		while(i < 5)
			fprintf(stderr,"%d,",buf->tokens[0].str[i++]);
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
	if(settings.verbose > 2)
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
	memcached.run();//memcached accept start.
	pthread_t memcache_worker;
	pthread_create(&memcache_worker,NULL,memcached_work,NULL);//dispatch the worker
	
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
	
	
	printf("ok\n");
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
