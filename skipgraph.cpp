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
#include "node.h"

struct settings{
	int useport;
	int targetip;
	int verbose;
	int threads;
}settings;

const char keyop = 7;
class node MyNode(100);


class membership_vector{
private:
	int mLength;// byte order
public:
	std::vector<int> mVector;
	int getVector(int bit){
		if( bit/32 >= mLength ){
			while(!(bit/32 < mLength) ){
				mVector.push_back((char)rand());
				mLength++;
			}
		}
		return (mVector[bit/32]>>(bit%32))&1;
	}
	
	void printVector(int length){
		printf("membership vector length:%d\n",mLength);
		printf("vector:");
		for(int i=0;i<mLength*32;i++){
			printf("%d",getVector(i));
		}
		printf("\n");
	}
	int operator[](int bit){
		return getVector(bit);
	}
	int compare(class membership_vector mv){
		int count = 0;
		int i=0;
		while(mVector[i] == mv.mVector[i]){
			i++;
			count+=32;
		}
		int maskbit = mVector[i]^mv.mVector[i];
		while(!(maskbit&1)){
			count++;
			maskbit <<= 1;
		}
		return count;
	}
	membership_vector(void){
		mVector[0]=(char)rand();
		mLength=1;
	}
};

void print_usage(void);
void settings_init(void);


enum Op{
	SearchOp,
	RangeOp,
	FoundOp,
	NotfoundOp
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
	unsigned int keylength,tmpkeylength;
	size_t tmplen;
	char* key;
	char* tmpkey;
	class node* tmpnode;
	int fromip,dummy,sendsocket;
	unsigned int level;
	int* levelptr;
	char* mes;
	char* orgmes;
	char flag;
	char operation;
	std::set<class node*>::iterator it;
	
	printf("socket:%d\n",socket);
	
	// read data from socket
	int EndFlag = 0;
	while(EndFlag == 0){
		tmplen = read(socket,&operation,1);
		if(tmplen == 0){ break; }// connection closed
		
		int maxlen,cmp;
		switch(operation){
		case 0://SearchOp
			receive_key(socket,&keylength,&key);
			
			tmplen = read(socket,&fromip,4);
			if( tmplen == 0 ){ EndFlag=1; break;}
			tmplen = read(socket,&level,5);
			if( tmplen == 0 ){ EndFlag=1; break;}
			
			tmpnode = new node(102);
			
			if(cmp == 0){
				//found!
				mes = (char*)malloc(MyNode.mKeyLength+5);
				mes[0] = 2;//FoundOp
				((int*)mes)[1] = MyNode.mKeyLength;
				memcpy(&mes[5], MyNode.mKey, MyNode.mKeyLength);
				read(socket,&fromip,4);
				read(socket,&dummy,5); // search level is not in use
				
				connect_send_close(fromip,settings.useport,MyNode.mKeyLength+5,mes);
				
				free(key);
				delete(tmpnode);
				free(mes);
				EndFlag=1;
				break;
			}else{
				//not found!
				read(socket,&fromip,4);
				read(socket,&level,4);
				read(socket,&flag,1);
				
				orgmes = mes = (char*)malloc(5+keylength+9);
				*mes = 0;//SearchOp
				mes++;
				memcpy(mes,&keylength,4);
				mes += 4;
				memcpy(&mes,key,keylength);
				mes += keylength;
				memcpy(&mes,&fromip,4);
				mes += 4;
				levelptr = (int*)mes;
				memcpy(&mes,&level,4);
				mes += 4;
				memcpy(&mes,&flag,1);
				
				if(cmp<0){
					while(level >= 0){
						if(MyNode.mRight.size() < level){
							level--;
							(*levelptr)--;
							continue;
						} 
						write(MyNode.mRight[level],&keyop,1);
						receive_key(MyNode.mRight[level],&tmpkeylength,&tmpkey);
							
						maxlen = tmpkeylength > keylength ? tmpkeylength : keylength;
						cmp = strncmp(tmpkey,key,maxlen);
						
						if(cmp <= 0){
							write(MyNode.mRight[level],mes,5+keylength+9);
							EndFlag=1;
							break;
						}else{
							level--;
							(*levelptr)--;
						}
					}
				}else{
					while(level >= 0){
						if(MyNode.mRight.size() < level){
							level--;
							(*levelptr)--;
							continue;
						}
						write(MyNode.mRight[level],&keyop,1);
						receive_key(MyNode.mRight[level],&tmpkeylength,&tmpkey);
						
						if(cmp <= 0){
							write(MyNode.mRight[level],mes,5+keylength+9);
							EndFlag=1;
							break;
						}else{
							level--;
							(*levelptr)--;
						}
					}
				}
				if(level<0){
					free(orgmes);
					mes = (char*)malloc(MyNode.mKeyLength+4);
					connect_send_close(fromip,settings.useport,MyNode.mKeyLength+4,MyNode.mKey);
					EndFlag=1;
				}
			}
			break;
		case 1://RangeOp
			
			break;
		case 2://FoundOp
			receive_key(socket,&keylength,&key);
			printf("key:%s found\n",key);
			EndFlag = 1;
			break;
		case 3://NotfoundOp
			receive_key(socket,&keylength,&key);
			printf("key not found, but %s found\n",key);
			EndFlag = 1;
			break;
		case 4://LinkOp
			read(socket,&level,4);
			read(socket,&flag,1);
			if(MyNode.mMaxLevel < level){
				MyNode.extendMaxLevel(level);
			}
			if(flag==0){
				MyNode.mLeft[level] = socket;
			}else{
				MyNode.mRight[level] = socket;
			}
			EndFlag = 1;
			break;
		case 7://KeyOp
			mes = (char*)malloc(MyNode.mKeyLength+4);
			memcpy(mes,&MyNode.mKeyLength,4);
			mes += 4;
			memcpy(mes, MyNode.mKey, MyNode.mKeyLength);
			write(socket,mes,MyNode.mKeyLength);
			
			free(key);
			free(mes);
			break;
		default:
			printf("error: undefined operation %d.\n",operation);
		}
	}
	return 0;
}

int sysrand(void){
	FILE* fp = fopen("/dev/random","r");
	int random;
	fread(&random,4,1,fp);
	return random;
}

char randchar(void){
	return (rand()%('Z'-'A')+'A');
}
char* randstring(int length){// do free();
	char* string = (char*)malloc(length+1);
	for(int i=0;i<length;i++){
		string[i]=randchar();
	}
	string[length]='\0';
	return string;
}
int my_aton(char* ipaddress){
	struct in_addr tmp_inaddr;
	int ip = 0;
	if(inet_aton(optarg,&tmp_inaddr)){
		ip = tmp_inaddr.s_addr;
	}else {
		printf("aton:address invalid\n");
	}
	return ip;
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
	class membership_vector mv;
	class node dummy(randstring(5));
	if(settings.targetip != 0){ // join to the skip graph
		if(settings.verbose>1){
			struct in_addr tmp_inaddr;
			tmp_inaddr.s_addr=settings.targetip;
			printf("send to %s:%d\n\n",inet_ntoa(tmp_inaddr),settings.useport);
		}
		//mKeyList.insert(new node("dummy"));
		
	}
	
	// set accepting thread
	class mulio mulio;
	int listening = create_tcpsocket();
	set_reuse(listening);
	bind_inaddr_any(listening,settings.useport);
	listen(listening,2048);
	mulio.SetAcceptSocket(listening);
	mulio.SetCallback(main_thread);
	mulio.run();// accept thread
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
