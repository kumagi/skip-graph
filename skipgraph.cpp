

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

#ifdef DEBUG
//void* patient;
#endif

//#define NDEBUG


typedef sg_neighbor<defkey> sg_Neighbor;
typedef neighbor_list<defkey> neighbor_List;
mulio mulio,memcached;
membership_vector myVector;
neighbor_list<defkey> gNeighborList;
node_list<defkey,defvalue> gNodeList;
address_list gAddressList;
rquery_list gRangeQueryList;
suspend_list<defkey,defvalue> gSuspendList;

unsigned int serialize_int_with_str(char* buff,int data){
	unsigned int length = 0,caster = 1000000000;
	while(data/caster == 0) caster /= 10;
	while(data > 0){
		buff[length] = (char)(data/caster + '0');
		length++;
		caster /= 10;
		if(caster){
			data = data%caster;
		}else {
			break;
		}
	}
	return length;
}
void print_usage(void);
void settings_init(void);

int main_thread(const int socket){
	// communication management
	int chklen,newsocket;
	char op;
	
	// buffer management
	char* buff;
	int bufflen,buffindex;
	int targetip,originip;
	unsigned long long targetid,originid;
	unsigned short targetport,originport;
	int targetlevel,originlevel;
	membership_vector originvector; //myVector is global
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
	
	//*
	if(settings.verbose>1)
		fprintf(stderr,"socket:%d ",socket);
	// */
		
	int DeleteFlag;
	DeleteFlag = 0;
	chklen = read(socket,&op,1);
	if(chklen <= 0){
		fprintf(stderr," close %d",socket);
		perror("  ");
		close(socket);
		gAddressList.erase(socket);
		DeleteFlag = 1;
		return DeleteFlag;
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
				
			originaddress = gAddressList.find(targetip,targetport);
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
				originaddress = gAddressList.find(targetip,targetport);
				if(originaddress != NULL){
					send_to_address(originaddress,buff,bufflen);
				}else{
					connect_send_close(targetip,targetport,buff,bufflen);
				}
				free(buff);
			}
		}
		break;
	case RangeOp:// [op] id level [beginkey] [endkey] [left_closed] [right_closed] [originip] [originport] [origin_range_query]
		if(settings.verbose>1)
			fprintf(stderr,"RangeOP ");
			
		read(socket,&targetid,8);
		targetnode = gNodeList.search_by_id(targetid);
			
		if(targetnode == NULL) {
			DEBUG_OUT("ID:%lld not found!\n", targetid);
			gNodeList.print();
			assert(!"arienai");
		}
		read(socket,&targetlevel,4);
			
		rKey.Receive(socket); // begin key
		sKey.Receive(socket); // end key
			
		read(socket,&left_closed,1); // 0->open(<) 1->closed(<=)
		read(socket,&right_closed,1); // 0->open(<) 1->cloaed(<=)
			
		// managers address
		read(socket,&targetip,4);
		read(socket,&targetport,2);
		
		// identifier and devided tag
		range_search.receive(socket);
		
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
			if(tempFlag == 1){
				break;
			}
			
			// not found
			DEBUG_OUT(" :RangeNotFoundOp\n");
			buffindex = 0;
			buff = (char*)malloc(1+range_search.size());
			buff[buffindex++] = RangeNotFoundOp;
			buffindex += range_search.Serialize(&buff[buffindex]);
					
			originaddress = gAddressList.find(targetip,targetport);
			if(originaddress != NULL){
				send_to_address(originaddress,buff,buffindex);
			}else{
				connect_send_close(targetip,targetport,buff,buffindex);
			}
			free(buff);
			break;
		}else if(targetnode->getKey() < rKey){
			tempFlag = 0;
			for(int i=originlevel;i>=0;i--){
				if(!targetnode->mRight[i] || sKey < targetnode->mRight[i]->mKey) continue;
				range_forward(originlevel,targetnode->mRight[i]->mId,*(targetnode->mRight[i]->mAddress),rKey,sKey,left_closed,right_closed,targetip,targetport,&range_search,true);
				tempFlag = 1;
				break;
			}
			if(tempFlag == 1){
				break;
			}
			
			// not found
			DEBUG_OUT(" :RangeNotFoundOp\n");
			buffindex = 0;
			buff = (char*)malloc(1+range_search.size());
			buff[buffindex++] = RangeNotFoundOp;
			buffindex += range_search.Serialize(&buff[buffindex]);
			
			originaddress = gAddressList.find(targetip,targetport);
			if(originaddress != NULL){
				send_to_address(originaddress,buff,buffindex);
			}else{
				connect_send_close(targetip,targetport,buff,buffindex);
			}
			free(buff);
			break;
		}
			
		//left side
		//DEBUG_OUT("left side\n");
		if( rKey < targetnode->getKey()){
			for(int i=originlevel ;i>=0; i--){
				if(!(targetnode->mLeft[i]) || ((!left_closed) && targetnode->mLeft[i]->mKey == rKey) || targetnode->mLeft[i]->mKey < rKey) continue;
				DEBUG_OUT(" range forward to %s ",targetnode->mLeft[i]->mKey.toString());
				range_forward(i,  targetnode->mLeft[i]->mId,  *(targetnode->mLeft[i]->mAddress), rKey, targetnode->mLeft[i]->mKey, left_closed,  1 , targetip, targetport, &range_search, false);
				left_closed = 0;
				rKey = targetnode->mLeft[i]->mKey;
			}
		}
		//right side
		if( targetnode->getKey() < sKey){
			for(int i=originlevel ;i>=0; i--){
				if(!(targetnode->mRight[i]) || ((!right_closed) && targetnode->mRight[i]->mKey == sKey) || sKey < targetnode->mRight[i]->mKey) continue;
				DEBUG_OUT(" range forward to %s ",targetnode->mRight[i]->mKey.toString());
				range_forward(i, targetnode->mRight[i]->mId, *(targetnode->mRight[i]->mAddress), targetnode->mRight[i]->mKey, sKey, 1, right_closed, targetip, targetport, &range_search, false);
				right_closed = 0;
				sKey = targetnode->mRight[i]->mKey;
			}
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
			
		DEBUG_OUT("\nsending RangeFoundOp %s\n to ",targetnode->getKey().toString());
		originaddress = gAddressList.find(targetip,targetport);
		if(originaddress != NULL){
			
			send_to_address(originaddress,buff,bufflen);
		}else{
			newsocket = create_tcpsocket();
			connect_port_ip(newsocket,targetip,targetport);
			mulio.SetSocket(newsocket);
			originaddress = gAddressList.add(newsocket,targetip,targetport);
			assert(gAddressList.find(targetip,targetport) != NULL);
			
			send_to_address(originaddress,buff,bufflen);
		}
		free(buff);
		DEBUG_OUT("RangeOp end  ");
		break;
	case PrepareOp:
			
			
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
		if(targetnode == NULL) {
			DEBUG_OUT("ID:%lld not found!\n", targetid);
			DEBUG_OUT("key:%s level:%d\n",rKey.toString(),targetlevel);
			gNodeList.print();
			assert(!"arienai");
		}
		
		originaddress = gAddressList.find(originip,originport);
		if(originaddress == NULL){
			originaddress = gAddressList.add(socket,originip,originport);
			DEBUG(originaddress->dump());
		}
			
		DEBUG_OUT("target:%s from:%s\ttargetlevel:%d\n",targetnode->getKey().toString(),rKey.toString(),targetlevel);
			
		if(left_or_right == Left){
			DEBUG_OUT(" %s -> %s\t targetlevel:%d\n",rKey.toString(),targetnode->getKey().toString(),targetlevel);
			targetnode->mLeft[targetlevel] = gNeighborList.retrieve(rKey,originid,originaddress);
		}else{
			DEBUG_OUT(" %s <- %s\t targetlevel:%d\n",targetnode->getKey().toString(),rKey.toString(),targetlevel);
			targetnode->mRight[targetlevel] = gNeighborList.retrieve(rKey,originid,originaddress);
		}
		
		if(targetlevel == 0){
			DEBUG(targetnode->dump());
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
		
		DEBUG_OUT("end of LinkOp\n");
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
		break;
	case RangeFoundOp:// [op] [range_query] [Foundkey] [FoundValue]
		if(settings.verbose>1)
			fprintf(stderr,"  RangeFoundOP ");
		range_found.receive(socket);
		DEBUG_OUT(" query is {%s} ",range_found.toString());
		DEBUG(range_found.mTag.dump());
		gRangeQueryList.found(range_found,socket);
		if(range_found.mTag.isComplete()){
			DEBUG_OUT("----------range query done.-------------\n");
		}
		DEBUG_OUT("\n");
		break;
	case RangeNotFoundOp:// [op] [range_query]
		if(settings.verbose>1)
			fprintf(stderr,"  RangeNotFoundOP ");
		range_found.receive(socket);
		DEBUG_OUT(" query is {%s} ",range_found.toString());
		DEBUG(range_found.mTag.dump());
		gRangeQueryList.notfound(range_found);
		DEBUG_OUT("\n");
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
			
		//DeleteFlag = 1;
		break;
	case TreatOp://targetid,key,originip,originid,originport,originvector
		// if you are nearest to origin, connect origin in level 0
		if(settings.verbose>1){
			fprintf(stderr,"TreatOP\n");
		}
			
		read(socket,&targetid,8);
		targetnode = gNodeList.search_by_id(targetid);
		if(targetnode == NULL) {
			DEBUG_OUT("ID:%lld not found!\n", targetid);
			gNodeList.print();
			assert(!"arienai");
		}
			
		rKey.Receive(socket);
		read(socket,&originip,4);
		read(socket,&originid,8);
		read(socket,&originport,2);
		originvector.receive(socket);
		
		
		DEBUG_OUT("%lld:%s treating key:%s\n IP:%s\n",
				  targetnode->mId,targetnode->getKey().toString(),rKey.toString(),my_ntoa(originip));
			
		if(rKey == targetnode->getKey() && originip == settings.myip && originport == settings.myport && originvector == myVector.mVector){
			assert(!"boomerang of TreatOP");
		}
		if(rKey == targetnode->getKey()){
			if(settings.verbose>1)
				fprintf(stderr,"received key:%s but I already have it\n",rKey.toString());
			//FIXME: move the node
				
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
				//pass LinkOP
				if(settings.verbose>1)
					fprintf(stderr,"passing TreatOP at level:%d\n",targetlevel);
				
				if(left_or_right == Right){
					send_treatop(*targetnode->mRight[targetlevel]->mAddress,targetnode->mRight[targetlevel]->mId,rKey,originip,originid,originport,originvector);
				}else{
					send_treatop(*targetnode->mLeft[targetlevel]->mAddress,targetnode->mLeft[targetlevel]->mId,rKey,originip,originid,originport,originvector);
				}
			}else{
				if(settings.verbose>1)
					fprintf(stderr,"finally treated by ID:%lld key:%s\n",targetnode->mId,targetnode->getKey().toString());
					
				// begin treating new node
				//send IntroduceOP to opposite site
				if(left_or_right == Left && targetnode->mLeft[0]){
					DEBUG_OUT("opposite introduceOp from %s to left %s by socket:%d\n",targetnode->getKey().toString(),targetnode->mLeft[0]->mKey.toString(),targetnode->mLeft[0]->mAddress->mSocket);
					send_introduceop(*targetnode->mLeft[0]->mAddress,targetnode->mLeft[0]->mId,rKey,originid,originip,originport,-1,originvector);
				}else if(left_or_right == Right && targetnode->mRight[0]) {
					DEBUG_OUT("opposite introduceOp from %s to Right %s by socket:%d\n",targetnode->getKey().toString(),targetnode->mRight[0]->mKey.toString(),targetnode->mRight[0]->mAddress->mSocket);
					send_introduceop(*targetnode->mRight[0]->mAddress,targetnode->mRight[0]->mId,rKey,originid,originip,originport,-1,originvector);
				}else{
					assert(targetnode->getKey().isMaximum() || targetnode->getKey().isMinimum());
				}
					
				//LinkOp to new node
				//decide how much level to link
				targetlevel = myVector.compare(originvector);
				//myVector.printVector();
				DEBUG_OUT("level:%d matched\n",targetlevel);
				targetlevel = targetlevel < MAXLEVEL-1 ? targetlevel : MAXLEVEL-1;
				
				// retrieve address
				targetaddress = gAddressList.find(originip,originport);
				if(targetaddress == NULL){
					newsocket = create_tcpsocket();
					connect_port_ip(newsocket,originip,originport);
					mulio.SetSocket(newsocket);
					targetaddress = gAddressList.add(newsocket,originip,originport);
					assert(gAddressList.find(originip,originport) != NULL);
				}
				
				for(int i=0;i<=targetlevel;i++){
					send_linkop(*targetaddress,originid,targetnode->getKey(),targetnode->mId,i,inverse(left_or_right));
					if(left_or_right == Left){
						DEBUG_OUT(":Link %s <- %s   level %d/%d by socket:%d\n",rKey.toString(),targetnode->getKey().toString(),i,targetlevel,targetaddress->mSocket);
						targetnode->mLeft[i] = gNeighborList.retrieve(rKey,originid,targetaddress);
					}else{
						DEBUG_OUT(":Link %s -> %s   level %d/%d by socket:%d\n",targetnode->getKey().toString(),rKey.toString(),i,targetlevel,targetaddress->mSocket);
						targetnode->mRight[i] = gNeighborList.retrieve(rKey,originid,targetaddress);
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
		break;
	case IntroduceOp:
		if(settings.verbose>1)
			fprintf(stderr,"IntroduceOP ");
			
		read(socket,&targetid,8);
		targetnode = gNodeList.search_by_id(targetid);
			
		rKey.Receive(socket);
		read(socket,&originip,4);
		read(socket,&originid,8);
		read(socket,&originport,2);
		read(socket,&originlevel,4);
		read(socket,&originvector,8);
		
		if(targetnode == NULL) {
			DEBUG_OUT("ID:%lld not found!\n", targetid);
			DEBUG_OUT("key:%s level:%d\n",rKey.toString(),targetlevel);
			gNodeList.print();
			assert(!"arienai");
		}
		
		DEBUG_OUT("new ID:%lld,key:%s\n",originid,rKey.toString());
			
		left_or_right = direction(targetnode->getKey(), rKey);
			
		//LinkOp to new node
		targetlevel = myVector.compare(originvector);
		targetlevel = targetlevel < MAXLEVEL-1 ? targetlevel : MAXLEVEL-1;
			
		// retrieve address
		targetaddress = gAddressList.find(originip,originport);
		if(targetaddress == NULL){
			newsocket = create_tcpsocket();
			DEBUG_OUT("new socket %d: ",newsocket);
			connect_port_ip(newsocket,originip,originport);
			mulio.SetSocket(newsocket);
			targetaddress = gAddressList.add(newsocket,originip,originport);
		}
			
		if(left_or_right == Left){
			for(int i=originlevel+1;i<=targetlevel;i++){
				DEBUG_OUT(":Link %s <- %s   at level %d by socket:%d\n",rKey.toString(),targetnode->getKey().toString(),i, targetaddress->mSocket);
				send_linkop(*targetaddress,originid,targetnode->getKey(),targetnode->mId,i,Right);
				targetnode->mLeft[i] = gNeighborList.retrieve(rKey,originid,targetaddress);
			}
		}else{
			for(int i=originlevel+1;i<=targetlevel;i++){
				DEBUG_OUT(":Link %s -> %s   at level %d by socket:%d\n",targetnode->getKey().toString(),rKey.toString(),i, targetaddress->mSocket);
				send_linkop(*targetaddress,originid,targetnode->getKey(),targetnode->mId,i,Left);
				targetnode->mRight[i] = gNeighborList.retrieve(rKey,originid,targetaddress);
			}
		}
			
		if(targetlevel < MAXLEVEL-1){
			if(left_or_right == Left){
				if(targetnode->mRight[targetlevel]){
					DEBUG_OUT("relay IntroduceOP from %s to %s at level %d by socket:%d\n",targetnode->getKey().toString(),targetnode->mRight[targetlevel]->mKey.toString(),targetlevel,targetnode->mRight[targetlevel]->mAddress->mSocket);
					send_introduceop(*targetnode->mRight[targetlevel]->mAddress,targetnode->mRight[targetlevel]->mId,rKey,originid,originip,originport,targetlevel,originvector);
				}else{
					DEBUG_OUT("end of rightside\n");
				}
			}else{
				if(targetnode->mLeft[targetlevel]){
					DEBUG_OUT("relay IntroduceOP from %s to %s at level %d by socket:%d\n",targetnode->getKey().toString(),targetnode->mLeft[targetlevel]->mKey.toString(),targetlevel,targetnode->mLeft[targetlevel]->mAddress->mSocket);
					send_introduceop(*targetnode->mLeft[targetlevel]->mAddress,targetnode->mLeft[targetlevel]->mId,rKey,originid,originip,originport,targetlevel,originvector);
				}else{
					DEBUG_OUT("end of leftside\n");
				}
			}
		}else{
			DEBUG_OUT("end of this side\n");
		}
		DEBUG_OUT("end of IntroduceOp\n");
		break;
	case ViewOp:
		//fprintf(stderr,"view\n");
		gNodeList.print();
		break;
	default:
		fprintf(stderr,"error: undefined operation %d.\n",op);
	}
	//*
	if(settings.verbose>1)
		fprintf(stderr,"socket:%d end\n",socket);
	//*/
	//mulio.printSocketList();
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

class memcached_clients{
private:
	enum constant{
		SIZE = 64,
	};
	std::list<memcache_buffer*> hashMap[SIZE];
	
public:
	memcache_buffer* retrieve(const int socket){
		int pos = murmurhash_int(socket)%SIZE;
		memcache_buffer* ans;
		std::list<memcache_buffer*>& list = hashMap[pos];
		std::list<memcache_buffer*>::iterator it;
		for(it = list.begin();it!=list.end();++it){
			if((*it)->getSocket() == socket){
				break;
			}
		}
		if(it == list.end()){// new creation
			ans = new memcache_buffer(socket);
			list.push_back(ans);
		}else{//
			return *it;
		}
		return ans;
	}
	void erase(const int socket){
		int pos = murmurhash_int(socket)%SIZE;
		std::list<memcache_buffer*>& list = hashMap[pos];
		std::list<memcache_buffer*>::iterator it;
		for(it = list.begin();it!=list.end();++it){
			if((*it)->getSocket() == socket){
				delete *it;
				list.erase(it);
				break;
			}
		}
		return;
	}
} gMemcachedSockets; 

int memcached_thread(const int socket){
	std::map<int,memcache_buffer*>::iterator bufferIt;
	memcache_buffer* buf;
	char* buff;
	int chklen,buffindex;
	int tokennum,targetflag;
	int DeleteFlag;
	defkey* targetkey,*beginkey,*endkey;
	defvalue* targetvalue;
	sg_Node *targetnode,*newnode;
	std::multimap<defkey,suspend<defkey,defvalue>*>::iterator suspendIt;
	suspend<defkey,defvalue>* suspending;
	address* add;
	range_query origin_query;
	
	int datalen,dataindex;
	
	//if(settings.verbose > 2)
		//	fprintf(stderr,"memcached client arrived socket:%d\n",socket);
	buf = gMemcachedSockets.retrieve(socket);
	
	tokennum = buf->receive();
	
	DeleteFlag = 0;
	if(tokennum < 0){
		gMemcachedSockets.erase(socket);
		//DeleteFlag = 1;
		//return;
	}
	
	switch(buf->getState()){
	case memcache_buffer::state_set:
		DEBUG_OUT("set!![%lld] ",gId);
		
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
			deep_write(socket,"STORED\r\n",8);
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
			targetflag = 0;
			if(targetnode->getKey() < *targetkey){
				for(int i=MAXLEVEL-1;i>=0;--i){
					if(!targetnode->mRight[i] || *targetkey < targetnode->mRight[i]->mKey){ continue; }
					chklen = send_treatop(*targetnode->mRight[i]->mAddress,targetnode->mRight[i]->mId,*targetkey,settings.myip,newnode->mId,settings.myport,myVector);
					DEBUG_OUT("set::target node:%lld's right %s\n",targetnode->mId,targetnode->mRight[i]->mKey.toString());
					DEBUG(targetnode->mRight[0]->dump());
					targetflag = 1;
					break;
				}
			}else{
				for(int i=MAXLEVEL;i>=0;--i){
					if(!targetnode->mLeft[i] || targetnode->mLeft[i]->mKey < *targetkey){ continue; }
					chklen = send_treatop(*targetnode->mLeft[i]->mAddress,targetnode->mLeft[i]->mId,*targetkey,settings.myip,newnode->mId,settings.myport,myVector);
					DEBUG_OUT("set::target node:%lld's left %s\n",targetnode->mId,targetnode->mLeft[i]->mKey.toString());
					DEBUG(targetnode->mLeft[0]->dump());
					targetflag = 1;
					break;
				}
			}
			if(targetflag == 0){
				add = gAddressList.find(settings.myip,settings.myport);
				chklen = send_treatop(*add,targetnode->mId,*targetkey,settings.myip,newnode->mId,settings.myport,myVector);
			}
		}else{
			add = gAddressList.get_else(settings.myip,settings.myport);
			assert(add != NULL);
			chklen = send_treatop(*add,0,*targetkey,settings.myip,newnode->mId,settings.myport,myVector);
			DEBUG_OUT("send TreatOp to ");
			DEBUG(add->dump());
			
			if(chklen <= 0){
				fprintf(stderr,"\n All Address tried but failed.\n");
			}
		}
		
		delete targetkey;
		delete targetvalue;
		buf->ParseOK();
		DEBUG_OUT("set end\n");
		break;
	case memcache_buffer::state_get:
		// search suspending socket
		
		suspending = gSuspendList.add_or_retrieve(socket);
		if(tokennum < 1)
			break;
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
				buff = (char*)malloc(datalen);
				buff[dataindex++] = SearchOp;
				serialize_longlong(buff,&dataindex,targetnode->mLeft[0]->mId);
				
				serialize_int(buff,&dataindex,buf->tokens[i].length);
				memcpy(&buff[dataindex],buf->tokens[i].str,buf->tokens[i].length);
				dataindex += buf->tokens[i].length;
				
				serialize_int(buff,&dataindex,MAXLEVEL-1);
				serialize_int(buff,&dataindex,settings.myip);
				serialize_short(buff,&dataindex,settings.targetport);
				assert(dataindex == datalen);
				
				for(int i = MAXLEVEL-1; i>=0; --i){
					if(*targetkey < targetnode->mLeft[i]->mKey ){
						deep_write(targetnode->mLeft[0]->mAddress->mSocket,buff,dataindex);
						suspending->add(*targetkey);
						gSuspendList.setKey(socket,*targetkey);
						break;
					}
				}
				free(buff);
			}else{
				dataindex = 0;
				buff = (char*)malloc(6 + targetkey->mLength + 3 + 10 + 2 + targetnode->getValue().mLength + 2);
				memcpy(&buff[dataindex],"VALUE ", 6); dataindex+=6;
				memcpy(&buff[dataindex],targetkey->mKey, targetkey->mLength); dataindex+=targetkey->mLength;
				memcpy(&buff[dataindex]," 0 ", 3); dataindex+=3;
				dataindex += serialize_int_with_str(&buff[dataindex],targetnode->getValue().mLength);
				memcpy(&buff[dataindex],"\r\n", 2); dataindex+=2;
				memcpy(&buff[dataindex],targetnode->getValue().mValue,targetnode->getValue().mLength); dataindex+=targetnode->getValue().mLength;
				memcpy(&buff[dataindex],"\r\n", 2); dataindex+=2;
				suspending->add(buff);
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
			fprintf(stderr,"\r\rrget!! ");
		beginkey = new defkey(buf->tokens[RGET_BEGIN].str);
		endkey = new defkey(buf->tokens[RGET_END].str);
		
		buffindex = 0;
		buff = (char*)malloc(beginkey->size() + endkey->size() + 5);
		
		// begin key
		memcpy(&buff[buffindex],buf->tokens[RGET_BEGIN].str,strlen(buf->tokens[RGET_BEGIN].str));
		buffindex += strlen(buf->tokens[RGET_BEGIN].str);
		buff[buffindex++] = ' ';
		// end key
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
		origin_query.mTag.init();
		
		targetnode = gNodeList.search_by_key(*beginkey);
		if(targetnode == NULL){
			assert(!"save some key before range query!");
		}
		if(targetnode->getKey() < *beginkey){
			for(int i = MAXLEVEL-1;i>=0;--i){
				if(!targetnode->mRight[i] || *beginkey < targetnode->mRight[i]->mKey){ continue; }
				range_forward(MAXLEVEL-1,0,*targetnode->mRight[i]->mAddress,*beginkey,*endkey,(char)atoi(buf->tokens[RGET_LEFT_CLOSED].str),(char)atoi(buf->tokens[RGET_RIGHT_CLOSED].str),settings.myip,settings.targetport,&origin_query,true);	
				break;
			}
		}else if(*endkey < targetnode->getKey()){
			for(int i = MAXLEVEL-1;i>=0;--i){
				if(!targetnode->mLeft[i] || *beginkey < targetnode->mLeft[i]->mKey){ continue; }
				range_forward(MAXLEVEL-1,0,*targetnode->mLeft[i]->mAddress,*beginkey,*endkey,(char)atoi(buf->tokens[RGET_LEFT_CLOSED].str),(char)atoi(buf->tokens[RGET_RIGHT_CLOSED].str),settings.myip,settings.targetport,&origin_query,true);
				break;
			}
		}else{
			add = gAddressList.find(settings.myip,settings.myport);
			range_forward(MAXLEVEL-1,0,*add,*beginkey,*endkey,(char)atoi(buf->tokens[RGET_LEFT_CLOSED].str),(char)atoi(buf->tokens[RGET_RIGHT_CLOSED].str),settings.myip,settings.targetport,&origin_query,true);
		}
		
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
		gMemcachedSockets.erase(socket);
		//*
		if(settings.verbose > 2)
			fprintf(stderr," client closed %d\n",socket);
		//*/
		break;
	case memcache_buffer::state_error:
		int i = 0;
		while(i < 5)
			fprintf(stderr,"%d,",buf->tokens[0].str[i++]);
		fprintf(stderr,"error!!\n");
		gMemcachedSockets.erase(socket);
		close(socket);
		break;
	}
	return DeleteFlag;
}


void* worker(void*){
	mulio.eventloop();// accept thread
	return NULL;
}
void* memcached_work(void*){
	if(settings.verbose > 2)
		fprintf(stderr,"memcached thread start\n");
	memcached.eventloop();
	return NULL;
}

int main(int argc,char** argv){
	srand(sysrand());
	pthread_t* threads;
	int c;
	defkey min,max;
	int myself;//loopback socket
	int targetsocket,tmp;
	address* myAddress;
	
	DEBUG(fprintf(stderr,"It's debug mode...\n"););
	
	//initialize
	min.Minimize();
	max.Maximize();
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
			tmp = atoi(optarg);
			if(0 < tmp && tmp < 65535){
				settings.targetport = (unsigned short)tmp;
			}else {
				fprintf(stderr,"illegal port %d\n",tmp);
				exit(1);
			}
			break;
		case 'm':
			tmp = atoi(optarg);
			if(0 < tmp && tmp < 65535){
				settings.memcacheport = (unsigned short)tmp;
			}else {
				fprintf(stderr,"illegal port %d\n",tmp);
				exit(1);
			}
			break;
		case 'P':
			tmp = atoi(optarg);
			if(0 < tmp && tmp < 65535){
				settings.myport = (unsigned short)tmp;
			}else {
				fprintf(stderr,"illegal port %d\n",tmp);
				exit(1);
			}
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
	listen(listening,1);
	mulio.SetAcceptSocket(listening);
	mulio.SetCallback(main_thread);
	//DEBUG(mulio.setverbose(512));
	mulio.run();// accept thread
	
	myself = create_tcpsocket();
	if(settings.targetip == 0){ // I am master
		sg_Node* minnode;
		sg_Node* maxnode;
		
		minnode = new sg_Node(min,dummy);
		maxnode = new sg_Node(max,dummy);
		
		// create left&right end
		sg_Neighbor *minpointer,*maxpointer;
		
		printf("loopback socket:%d\n",myself);
		myAddress = new address(myself,settings.myip,settings.myport);
		minpointer = new sg_Neighbor(min,myAddress,minnode->mId);
		maxpointer = new sg_Neighbor(max,myAddress,maxnode->mId);
		gAddressList.add(myself,settings.myip,settings.myport);
		
		for(int i=0;i<MAXLEVEL;i++){
			minnode->mLeft[i] = NULL;
			minnode->mRight[i] = maxpointer;
			maxnode->mLeft[i] = minpointer;
			maxnode->mRight[i] = NULL;
		}
		gNodeList.insert(minnode);
		gNodeList.insert(maxnode);
		fprintf(stderr,"min:ID%lld  max:ID%lld level:%d\n",minnode->mId,maxnode->mId,MAXLEVEL);
	}else {
		targetsocket = create_tcpsocket();
		gAddressList.add(targetsocket,settings.targetip,settings.targetport);
		fprintf(stderr,"master is socket:%d  %s:%d\n\n",targetsocket,my_ntoa(settings.targetip),settings.targetport);
		connect_port_ip(targetsocket,settings.targetip,settings.targetport);
		mulio.SetSocket(targetsocket);
	}
	gNodeList.print();
	
	connect_port_ip(myself,settings.myip,settings.myport);
	gAddressList.add(myself,settings.myip,settings.myport);
	mulio.SetSocket(myself);
	
	int memcachesocket = create_tcpsocket();
	set_reuse(memcachesocket);
	bind_inaddr_any(memcachesocket,settings.memcacheport);
	listen(memcachesocket,2048);
	memcached.SetAcceptSocket(memcachesocket);
	
	DEBUG(memcached.setverbose(1));
	  
	memcached.SetCallback(memcached_thread);
	memcached.run();//memcached accept start.
	pthread_t memcache_worker;
	pthread_create(&memcache_worker,NULL,memcached_work,NULL);//dispatch the worker
	
	
	threads = (pthread_t*)malloc((settings.threads-1)*sizeof(pthread_t));
	while(1){
		break;
		DEBUG(break);
		for(int i=0;i<settings.threads-1;i++){
			pthread_create(&threads[i],NULL,worker,NULL);
		}
		break;
	}
	
	if(settings.verbose>1){
		printf("start warking as skipgraph server...\n");
		printf("myIP:%s myport:%d\ntargetIP:%s targetport:%d\nverbose:%d\nthreads:%d\nVector:%llx\n\n",
			   my_ntoa(settings.myip),settings.myport,my_ntoa(settings.targetip),settings.targetport,
			   settings.verbose,settings.threads,myVector.mVector);
	}
	DEBUG_OUT("Ready\n");
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
