
#include "skipgraph.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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


typedef class sg_node<intkey,intvalue> sg_Node;
typedef class sg_neighbor<intkey> sg_Neighbor;
class mulio mulio;
class membership_vector myVector;
std::list<sg_Node> NodeList;

//param
std::list<sg_Node>::iterator NoNode = (std::list<sg_Node>::iterator)NULL;

void print_nodelist(void){
	std::list<sg_Node>::iterator it = NodeList.begin();
	while(it != NodeList.end() ){
		//if((*it)->mId != (*it)->pPointer.mId){printf("not sanity\n");}
		printf("%lld: key=%d  value=%x",it->mId,it->mKey.mKey,it->mValue.mValue);
		if(it->mLeft[0]){
			printf("  left=%d ",it->mLeft[0]->mKey.mKey);
		}else{
			printf("  left=no connect");
		}
		if(it->mRight[0]){
			printf("  right=%d\n",it->mRight[0]->mKey.mKey);
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

template<typename KeyType>
sg_Node* search_node_by_key(KeyType key){
	std::list<sg_Node>::iterator it = NodeList.begin();
	while(it != NodeList.end() ){
		if(it->mKey == key){
			return *it;
		}
		++it;
	}
	return NodeList.front();
}


std::list<sg_Node>::iterator search_node_by_id(long long id){
	//printf("search_node_by_id began, id:%lld\n",id);
	if(NodeList.empty()){
		return NoNode;
	}
	std::list<sg_Node>::iterator it = NodeList.begin();
	while(it != NodeList.end() ){
		if(it->mId == id){
			return it;
		}
		++it;
	}
	return NoNode;
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
int send_with_connect(sg_neighbor<intkey>* sgn,const char* buff,const int bufflen){
	int sendsize;
	if(sgn->mAddress->mSocket != 0){
		sendsize = write(sgn->mAddress->mSocket,buff,bufflen);
		if(sendsize<=0){
			fprintf(stderr,"send_with_connect:error\n");
		}
	}else{
		std::list<address>::iterator it = gAddressList.begin();
		for(;it!=gAddressList.end();it++){
			if(it->mIP==sgn->mAddress->mIP && it->mPort==sgn->mAddress->mPort){
				break;
			}
		}
		if(it!=gAddressList.end()){
			sendsize = connect_send(&sgn->mAddress->mSocket,sgn->mAddress->mIP,sgn->mAddress->mPort,buff,bufflen);
			if(sendsize>0){
				mulio.SetSocket(sgn->mAddress->mSocket);
				gAddressList.push_back(address(sgn->mAddress->mSocket,sgn->mAddress->mIP,sgn->mAddress->mPort));
				return sendsize;
			}else{
				// delete the address from listit = gAddressList.begin();
				for(;it!=gAddressList.end();it++){
					if(it->mIP==sgn->mAddress->mIP && it->mPort==sgn->mAddress->mPort){
						gAddressList.erase(it);
						break;
					}
				}
			}
		}
	}
	return 0;
}
int main_thread(int s){
	int socket = s,tmpsocket;
	char op;
	int chklen;
	unsigned long long targetid,originid;
	long long originvector;
	std::list<sg_Node>::iterator tmpnode;
	sg_Node* newnode;
	int targetlevel,tmplevel,targetip,originip;
	unsigned short targetport,originport;
	char* buff;
	int bufflen,buffindex;
	char left_or_right;
	std::list<class address>::iterator AddressIt;
	class address* newAddress;
	
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
						fprintf(stderr,"%d > %d ?\n",tmpnode->mRight[targetlevel]->mKey.mKey , tmpkey.mKey);
						if(tmpnode->mRight[targetlevel]){
							continue;
						}
						fprintf(stderr,"%d > %d ?\n",tmpnode->mRight[targetlevel]->mKey.mKey , tmpkey.mKey);
						if(!(tmpnode->mRight[targetlevel]->mKey > tmpkey)){
							break;
						}
					}
				}else{
					//send SearchOcP to Leftside
					left_or_right = Left;
					for(;targetlevel>=0;targetlevel--){
						if(tmpnode->mLeft[targetlevel]){
							continue;
						}
						if(!(tmpnode->mLeft[targetlevel]->mKey < tmpkey)){
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
						serialize_longlong(buff,&buffindex,tmpnode->mRight[targetlevel]->mId);
					}else{
						serialize_longlong(buff,&buffindex,tmpnode->mLeft[targetlevel]->mId);
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
			
			
			newAddress = new address(socket,originip,originport);
			for(AddressIt=gAddressList.begin();AddressIt!=gAddressList.end();++AddressIt){
				if(AddressIt->mIP == originip && AddressIt->mPort == originport){
					break;
				}
			}
			if(AddressIt == gAddressList.end()){
				newAddress = new address(socket,originip,originport);
				gAddressList.push_back(*newAddress);
			}else{
				newAddress = &*AddressIt;
			}
			
			if(left_or_right == Left){
				if(tmpnode->mLeft[targetlevel]){
					tmpnode->mLeft[targetlevel]->set(socket,tmpkey,newAddress,originid);
				}else{
					tmpnode->mLeft[targetlevel] = new sg_Neighbor(socket,tmpkey,newAddress,originid);
				}
			}else{
				if(tmpnode->mRight[targetlevel]){
					tmpnode->mRight[targetlevel]->set(socket,tmpkey,newAddress,originid);
				}else{
					tmpnode->mRight[targetlevel] = new sg_Neighbor(socket,tmpkey,newAddress,originid);
				}
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
			if(settings.verbose>1){
				fprintf(stderr,"SetOP ");
			}
			tmpkey.Receive(socket);
			tmpvalue.Receive(socket);
			newnode = new sg_Node(tmpkey,tmpvalue);
			
			// Build up list with memvership vector
			//prepare buffer
			buffindex = 0;
			bufflen = 1+8+tmpnode->mKey.size()+4+8+2+8;
			buff = (char*)malloc(bufflen);
			buff[buffindex++] = TreatOp;
			if(NodeList.size()>0){
				serialize_longlong(buff,&buffindex,NodeList.front().mId);
			}else{
				targetid=0;
				serialize_longlong(buff,&buffindex,targetid);
			}
			buffindex += newnode->mKey.Serialize(&buff[buffindex]);
			serialize_int(buff,&buffindex,settings.myip);
			serialize_longlong(buff,&buffindex,newnode->mId);
			serialize_short(buff,&buffindex,settings.myport);
			serialize_longlong(buff,&buffindex,myVector.mVector);
			
			NodeList.push_back(*newnode);
			delete newnode;
			
			assert(bufflen == buffindex && "buffsize ok");
			
			chklen = 0;
			printf("addresses:%d\n",gAddressList.size());;
			for(AddressIt = gAddressList.begin();AddressIt != gAddressList.end();++AddressIt){
				printf("trying:%s\n",my_ntoa(AddressIt->mIP));
				if(AddressIt->mIP == settings.myip && NodeList.empty()){++AddressIt; continue;}
				chklen = connect_send_close(AddressIt->mIP,AddressIt->mPort,buff,buffindex);//TreatOp
				if(chklen > 0){
					break;
				}
			}
			if(chklen <= 0){
				fprintf(stderr,"\n All Address tried but failed.\n");
			}
			
			free(buff);
			fprintf(stderr,"key:%d ,value:%d set in ID:%lld\n",tmpkey.mKey,tmpvalue.mValue,newnode->mId);
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
			
			if(tmpkey == tmpnode->mKey && originip == settings.myip && originport == settings.myport && originvector == myVector.mVector){
				
				buffindex = 0;
				bufflen = 1+8+tmpnode->mKey.size()+4+8+2+8;
				buff = (char*)malloc(bufflen);
				buff[buffindex++] = TreatOp;
				if(NodeList.size()>0){
					serialize_longlong(buff,&buffindex,NodeList.front().mId);
				}else{
					targetid=0;
					serialize_longlong(buff,&buffindex,targetid);
				}
				buffindex += tmpkey.Serialize(&buff[buffindex]);
				serialize_int(buff,&buffindex,settings.myip);
				serialize_longlong(buff,&buffindex,tmpnode->mId);
				serialize_short(buff,&buffindex,settings.myport);
				serialize_longlong(buff,&buffindex,myVector.mVector);
				
				chklen = 0;
				AddressIt = gAddressList.begin();
				while(AddressIt != gAddressList.end()){
					if(AddressIt->mIP == settings.myip && AddressIt->mPort == settings.myport){++AddressIt; continue;}
					chklen = connect_send_close(AddressIt->mIP,AddressIt->mPort,buff,buffindex);//TreatOp
					if(chklen > 0){
						break;
					}
					++AddressIt;
				}
				if(chklen <= 0){
					fprintf(stderr,"no SkipGraph server detected\n");
				}
				free(buff); 
			}
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
						if(!tmpnode->mRight[tmplevel]) continue;
						if(tmpnode->mRight[tmplevel]->mKey < tmpkey){
							break;
						}
					}
				}else{
					//send TreatOP to Leftside
					left_or_right = Left;
					for(;tmplevel>=0;tmplevel--){
						if(!tmpnode->mLeft[tmplevel]) continue;
						if(tmpnode->mLeft[tmplevel]){
							continue;
						}
						if(tmpnode->mLeft[tmplevel]->mKey > tmpkey){
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
						serialize_longlong(buff,&buffindex,tmpnode->mRight[tmplevel]->mId);
					}else{
						serialize_longlong(buff,&buffindex,tmpnode->mLeft[tmplevel]->mId);
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
					if(settings.verbose>1)
						fprintf(stderr,"finally treated by ID:%lld key:%d\n",tmpnode->mId,tmpnode->mKey.mKey);
					
					//sent IntroduceOP to opposite site
					if(settings.verbose>1){
						if(left_or_right == Left && tmpnode->mLeft[0]){
							fprintf(stderr,"sending introduceOp from %d to %d\n",tmpnode->mKey.mKey,tmpnode->mLeft[0]->mKey.mKey);
						}
					}
					if(settings.verbose>1){
						if(left_or_right == Right && tmpnode->mRight[0]){
							fprintf(stderr,"sending introduceOp from %d to %d\n",tmpnode->mKey.mKey,tmpnode->mRight[0]->mKey.mKey);
						}
					}
					targetlevel = 0;
					buffindex = 0;
					bufflen = 1+8+tmpkey.size()+4+8+2+4+8;
					buff = (char*)malloc(bufflen);
					buff[buffindex++] = IntroduceOp;
					if(left_or_right == Left && tmpnode->mLeft){
						fprintf(stderr,"target ID:%lld\n",tmpnode->mLeft[0]->mId);
						serialize_longlong(buff,&buffindex,tmpnode->mLeft[0]->mId);
					}else{
						fprintf(stderr,"target ID:%lld\n",tmpnode->mRight[0]->mId);
						serialize_longlong(buff,&buffindex,tmpnode->mRight[0]->mId);
					}
					buffindex += tmpkey.Serialize(&buff[buffindex]);
					serialize_int(buff,&buffindex,originip);
					serialize_longlong(buff,&buffindex,originid);
					serialize_short(buff,&buffindex,originport);
					serialize_int(buff,&buffindex,targetlevel);
					serialize_longlong(buff,&buffindex,originvector);
					assert(bufflen == buffindex);
					if(left_or_right == Left){
						connect_send_close(tmpnode->mLeft[0]->mAddress->mIP,tmpnode->mLeft[0]->mAddress->mPort,buff,bufflen);
					}else{
						connect_send_close(tmpnode->mRight[0]->mAddress->mIP,tmpnode->mRight[0]->mAddress->mPort,buff,bufflen);
					}
					free(buff);
					
					//LinkOp to new node
					targetlevel = myVector.compare(originvector);
					printf("vector1:%lld\nvector2:%lld\n%d bit equal\n",myVector.mVector,originvector,targetlevel);
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
					serialize_short(buff,&buffindex,settings.myport);
					for(int i=0;i<=targetlevel;i++){
						serialize_int(buff,&buffindex,i);
						buff[buffindex++] = left_or_right == Left ? Right : Left;
						connect_send(&tmpsocket,originip,originport,buff,buffindex);
						buffindex -= sizeof(int)+1;
						
						mulio.SetSocket(tmpsocket);
						if(left_or_right == Left){
							if(tmpnode->mLeft[targetlevel]){
								tmpnode->mLeft[targetlevel]->set(socket,tmpkey,newAddress,originid);
							}else{
								tmpnode->mLeft[targetlevel] = new sg_Neighbor(socket,tmpkey,newAddress,originid);
							}
						}else{
							if(tmpnode->mRight[targetlevel]){
								tmpnode->mRight[targetlevel]->set(socket,tmpkey,newAddress,originid);
							}else{
								tmpnode->mRight[targetlevel] = new sg_Neighbor(socket,tmpkey,newAddress,originid);
							}
						}
						fprintf(stderr,"Link from %d to %d at level %d\n",tmpnode->mKey.mKey,tmpkey.mKey,i);
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
							if(tmpnode->mRight[targetlevel]){
								send_with_connect(tmpnode->mRight[targetlevel],buff,bufflen);
							}
						}else{
							if(tmpnode->mLeft[targetlevel]){
								send_with_connect(tmpnode->mLeft[targetlevel],buff,bufflen);
							}
						}
						if(settings.verbose>1){
							if(left_or_right == Left){
								if(tmpnode->mRight[targetlevel]){
									fprintf(stderr,"sending introduceOp from %d to %d\n",tmpnode->mKey.mKey,tmpnode->mRight[targetlevel]->mKey.mKey);
								}
							}else{
								if(tmpnode->mLeft[targetlevel]){
									fprintf(stderr,"sending introduceOp from %d to %d\n",tmpnode->mKey.mKey,tmpnode->mLeft[targetlevel]->mKey.mKey);
								}
							}
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
			fprintf(stderr,"found %lld\n",tmpnode->mId);
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
			//LinkOp to new node
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
			serialize_short(buff,&buffindex,settings.myport);
			for(int i=0;i<=targetlevel;i++){
				serialize_int(buff,&buffindex,i);
				buff[buffindex++] = left_or_right == Left ? Right : Left;
				
				connect_send(&tmpsocket,originip,originport,buff,buffindex);
				buffindex -= sizeof(int)+1;
						
				mulio.SetSocket(tmpsocket);
				
				if(left_or_right == Left){
					if(tmpnode->mLeft[targetlevel]){
						tmpnode->mLeft[targetlevel]->set(socket,tmpkey,newAddress,originid);
					}
				}else{
					if(tmpnode->mRight[targetlevel]){
						tmpnode->mRight[targetlevel]->set(socket,tmpkey,newAddress,originid);
					}
				}
				printf("Link from %d to %d at level %d in socket:%d\n",tmpnode->mKey.mKey,tmpkey.mKey,i,socket);
						
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
					if(tmpnode->mRight[targetlevel]){
						send_with_connect(tmpnode->mRight[targetlevel],buff,bufflen);
					}
				}else{
					if(tmpnode->mLeft[targetlevel]){
						send_with_connect(tmpnode->mLeft[targetlevel],buff,bufflen);
					}
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
		minpointer = new sg_neighbor<intkey>(0,min,&myAddress,minnode->mId);
		maxpointer = new sg_neighbor<intkey>(0,max,&myAddress,maxnode->mId);
		
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
		NodeList.push_back(*minnode);
		NodeList.push_back(*maxnode);
		delete minnode;
		delete maxnode;
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
	
	char command[30];
	
	intkey key;
	intvalue value;
	char* buff;
	int bufflen,buffindex,tmptargetlevel=MAXLEVEL-1;
	long long tmpid=0;
	while(1){
		printf("command> ");
		scanf("%s ",command);
		if(strncmp("set",command,3)==0){
			scanf("%d %d",&key.mKey,&value.mValue);
			bufflen = 1 + key.size() + value.size();
			buffindex = 0;
			buff = (char*)malloc(bufflen);
			buff[buffindex++] = SetOp;
			buffindex += key.Serialize(&buff[buffindex]);
			buffindex += value.Serialize(&buff[buffindex]);
			
			assert(bufflen == buffindex && "buffsize ok");
			
			connect_send_close(settings.targetip,settings.myport,buff,buffindex);
			free(buff);
			printf("send SendOp with key:%d value:%d\n",key.mKey,value.mValue);
		}else if(strncmp("search",command,4)==0){
			scanf("%d",&key.mKey);
			printf("try to %s %d\n",command,key.mKey);
			//make SearchOp
			buffindex = 0;
			bufflen = 1+8+key.size()+4+4+2;//[OP id key level ip port]
			buff = (char*)malloc(bufflen);
			buff[buffindex++] = SearchOp;
			
			serialize_longlong(buff,&buffindex,tmpid);
			
			buffindex+=key.Serialize(&buff[buffindex]);
			serialize_int(buff,&buffindex,tmptargetlevel);
			serialize_int(buff,&buffindex,settings.myip);
			serialize_short(buff,&buffindex,settings.myport);
			
			assert(bufflen == buffindex && "buffsize ok");
			
			connect_send_close(settings.targetip,settings.myport,buff,buffindex);
			printf("send SerchOp with key:%d\n",key.mKey);
			
			free(buff);
		} 
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
