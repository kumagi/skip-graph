#ifndef SKIPGRAPH
#define SKIPGRAPH
#define MAXLEVEL 8
#include "mytcplib.h"
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <vector>
#include <list>
#include <set>

struct settings{
	int myip;
	unsigned short myport;
	int targetip;
	unsigned short targetport;
	int verbose;
	int threads;
} settings;
const char keyop = 7;


class address{
public:
	const int mIP;
	const unsigned short mPort;
	const int mSocket;
	int mCounter;
	address(const int s,const int i,const unsigned short p)
		:mIP(i),mPort(p),mSocket(s)
	{
		mCounter = 1;
	}
	
	~address(void){
		if(mSocket!=0){
			close(mSocket);
		}
	}
	
	address& operator--(void){
		mCounter--;
		if(mCounter <= 0 && mSocket!=0){
			close(mSocket);
		}
		return *this;
	}
	address& operator++(void){
		mCounter++;
		return *this;
	}
	bool operator==(const address& ad)const {
		return (mIP==ad.mIP && mPort==ad.mPort);
	}
};

template<typename KeyType>
class sg_neighbor{
public:
	const KeyType mKey;
	const long long mId;
	const address* mAddress;
	
	int send(void* buff,int& bufflen){
		assert(!"don't call this funcion\n");
		return 0;
	}
	sg_neighbor(const KeyType& key,const address* ad,const long long id)
		:mKey(key),mId(id),mAddress(ad)
	{
	}
	/*
	sg_neighbor(const sg_neighbor<KeyType>& ngn)
		:mAddress(ngn.ad)
	{
		mKey = ngn.mKey;
		mId = ngn.mId;
	}
	*/
};

long long gId = 0;
template<typename KeyType, typename ValueType>
class sg_node{
public:
	const KeyType mKey;
	const ValueType mValue;
	const long long mId;
	sg_neighbor<KeyType>* mLeft[MAXLEVEL];
	sg_neighbor<KeyType>* mRight[MAXLEVEL];
	
	sg_node(const KeyType& k,const ValueType& v)
		:mKey(k),mValue(v),mId(gId++)
	{
		for(int i=0;i<MAXLEVEL;i++){
			mLeft[i] = NULL;
			mRight[i] = NULL;
		}
	}
	bool operator<(const class sg_node<KeyType,ValueType>& rightside) const
	{
		return mKey < rightside.mKey;
	}
};

// key type: int
class intkey{
public :
	int mKey;
	intkey(){};
	intkey(int k);
	int Receive(const int socket);//it returns received size
	int Serialize(const void* buff)const;//it returns writed size
	bool isMaximum(void){
		return mKey == 0x7fffffff;
	}
	bool isMinimum(void){
		return mKey == (int)0x80000000;
	}
	void Maximize(void);
	void Minimize(void);
	int operator<(const intkey& rightside) const 
	{
		//fprintf(stderr,"in operator :%d < %d ?\n",mKey,rightside.mKey);
		return mKey<rightside.mKey;
	}
	int operator>(const intkey& rightside) const
	{
		//fprintf(stderr,"in operator :%d > %d ?\n",mKey,rightside.mKey);
		return mKey>rightside.mKey;
	}
	int operator==(const intkey& rightside) const
	{
		return mKey==rightside.mKey;
	}
	int size(void) const {return 4;}
};
intkey::intkey(const int k){
	mKey = k;
}
void intkey::Maximize(void){
	mKey = 0x7fffffff;
}
void intkey::Minimize(void){
	mKey = 0x80000000;
}
inline int intkey::Receive(const int socket)
{
	int chklen;
	chklen = read(socket,&mKey,4);
	return chklen;
}
inline int intkey::Serialize(const void* buf) const
{
	int* target = (int*)buf;
	*target = mKey;
	return 4;
}

/* value */
class intvalue{
public :
	int mValue;
	intvalue(void) {mValue=0;};
	intvalue(const int v);
	int Receive(const int socket);
	int Serialize(const void* buff)const;
	int size(void)const {return 4;}
};
intvalue::intvalue(const int v){
	mValue = v;
}
inline int intvalue::Receive(const int socket)
{
	int chklen;
	chklen = read(socket,&mValue,4);
	return chklen;
}
inline int intvalue::Serialize(const void* buf)const
{
	int* target = (int*)buf;
	*target = mValue;
	return 4;
}



/* random functions */
unsigned int sysrand(void){
	FILE* fp = fopen("/dev/random","r");
	int random;
	fread(&random,4,1,fp);
	printf("random:%d\n",random);
	return random;
}
long long int rand64(void){
	long long int rnd = rand();
	rnd <<= 32;
	rnd ^= rand();
	return rnd;
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

/* NodeList */
/*
templete<typename KeyType,typename ValueType>
class node_list{
public:
	std::set<sg_node<KeyType,ValueType >*> mList; 
	
	// search by Key
	
	
	// search by id
	
	}
*/

/* membership_vector */
class membership_vector{
public:
	long long mVector;
	bool getVector(const int bit) const
	{
		return (mVector>>bit)&1;
	}
	int operator[](const int bit) const
	{
		return getVector(bit);
	}
	
	void printVector(void) const
	{
		unsigned int upper = (unsigned int)(mVector>>32);
		printf("vector:%x%x\n",upper,(unsigned int)mVector);
		printf("\n");
	}
	int compare(const long long mv) const{
		int count = 0;
		long long diff = mVector^mv;
		while((diff&1)==0 && count<MAXLEVEL-1){
			count++;
			diff >>= 1;
		}
		return count;
	}
	void init(void)
	{
		mVector = rand64();
	}
	membership_vector(void){
		init();
	}
};

inline void serialize_longlong(char* buff,int* offset,const long long& data)
{
	long long* plonglong;
	plonglong = (long long*)&buff[*offset];
	*plonglong = data;
	*offset += sizeof(long long);
}
inline void serialize_int(char* buff,int* offset,const int& data)
{
	int* pint;
	pint = (int*)&buff[*offset];
	*pint = data;
	*offset += sizeof(pint);
}

inline void serialize_short(char* buff,int* offset,const short& data)
{
	short* pshort;
	pshort = (short*)&buff[*offset];
	*pshort = data;
	*offset += sizeof(short);
}

template<class KeyType>
class neighbor_list
{
public:
	typedef sg_neighbor<KeyType> neighbor;
	
	std::list<neighbor*> nList;
	
 	neighbor* retrieve(const KeyType& key, const long long id, const address* ad)
	{
		typename std::list<neighbor*>::iterator it;
		it = nList.begin();
		 
		while(it != nList.end()){
			if((*it)->mKey == key && (*it)->mId == id && *(*it)->mAddress == *ad){
			 	return *it;
			}
		}
		nList.push_back(new neighbor(key,ad,id));
		return nList.back();
	}
};

#endif
