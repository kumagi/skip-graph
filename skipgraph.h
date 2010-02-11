#ifndef SKIPGRAPH
#define SKIPGRAPH
#define MAXLEVEL 4
#include "mytcplib.h"
#include "suspend.hpp"
#include "MurmurHash2A.cpp"
#include "MurmurHash2.cpp"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <pthread.h>
#include <vector>
#include <list>
#include <set>
#include <map>

#define defkey strkey
#define defvalue strvalue

#ifndef DEBUG_MACRO
#define DEBUG_MACRO

//#define DEBUG_MODE
#ifdef DEBUG_MODE
#define DEBUG_OUT(...) fprintf(stderr,__VA_ARGS__)
#define DEBUG(x) x
#else
#define NDEBUG
#define DEBUG_OUT(...) 
#define DEBUG(...) 
#endif

#endif // DEBUG_MACRO

struct settings{
	int myip;
	unsigned short myport;
	unsigned short memcacheport;
	int targetip;
	unsigned short targetport;
	int verbose;
	int threads;
} settings;
const char keyop = 7;

enum Op{
	SearchOp,
	RangeOp,
	FoundOp,
	RangeFoundOp,
	NotfoundOp,
	RangeNotFoundOp,
	SetOp,
	LinkOp,
	TreatOp,
	IntroduceOp,
	PrepareOp,
	ApplyOp,
	ViewOp,
};

enum Left_Right{
	Left,
	Right,
};
enum closed_opened{
	Opened,
	Closed,
};
unsigned int murmurhash_int(int data){
	return MurmurHash2 (&data, 4, 31 );
}
unsigned int murmurhash_bytes(const void* data,int length){
	return MurmurHash2 (data, length, 31 );
}
template<typename obj>
class hash{
private:
	CMurmurHash2A murmur;
public:
	hash(){
	}
	inline unsigned int calc(const obj& tar){
		murmur.Begin(0);
		murmur.Add((unsigned char*)&tar,sizeof(obj));
		return murmur.End();
	}
};

class bytehash{
private:
	CMurmurHash2A murmur;
public:
	bytehash():murmur(){
	}
	inline unsigned int calc(const char* buff,const unsigned int length){
		murmur.Begin(0);
		murmur.Add((const unsigned char*)buff,length);
		return murmur.End();
	}
};
class address{
private:
	const int mIP;
	const unsigned short mPort;
	pthread_mutex_t write_mutex;
public:
	const int mSocket;
	address(const int s,const int i,const unsigned short p)
		:mIP(i),mPort(p),write_mutex(PTHREAD_MUTEX_INITIALIZER),mSocket(s) {
	}
	
	~address(void){
		if(mSocket!=0){
			close(mSocket);
		}
	}
	void dump(void) const{
		fprintf(stderr,"address:[%d] %s:%d\n",mSocket,my_ntoa(mIP),mPort);
	}
	bool operator==(const address& ad)const {
		return (mIP==ad.mIP && mPort==ad.mPort);
	}
	
	int send(const char* buff,const int length) const {
		int sendsize;
		assert(mSocket>8 && "GORUA!");
		pthread_mutex_lock(const_cast<pthread_mutex_t*>(&write_mutex));
		sendsize = deep_write(mSocket,buff,length);
		
		DEBUG_OUT("send %d byte! for ",sendsize);
		DEBUG(dump());
		
		pthread_mutex_unlock(const_cast<pthread_mutex_t*>(&write_mutex));
		return sendsize;
	}
};

class address_list{
private:
	hash<int> hash_func;
	std::list<address*> hashMap[512];
	std::map<int,int> sockets;// socket and hashnum
public:
	address_list(void):hash_func(),sockets(){
		for(int i=0;i<512;i++){
			hashMap[i].clear();
		}
	}
	address* add(const int socket,const int ip,const unsigned short port){
		address* newaddress = new address(socket,ip,port);
		const int hash = hash_func.calc(ip) & 511;
		hashMap[hash].push_back(newaddress);
		
		sockets.insert(std::pair<int,int>(socket,hash));
		
		return newaddress;
	}
	address* find(const int ip,const unsigned short port){
		const int hash = hash_func.calc(ip) & 511;
		std::list<address*>& list = hashMap[hash];
		for(std::list<address*>::iterator it = list.begin();it != list.end(); ++it){
			if((**it) == address(0,ip,port) ){
				return *it;
			}
		}
		return NULL;
	}
	address* get_else(const int ip,const unsigned short port){
		for(int i=0;i<512;i++){
			std::list<address*>* list = &hashMap[i];
			for(std::list<address*>::iterator it = list->begin();it != list->end(); ++it){
				if(!((**it) == address(0,ip,port))){
					return *it;
				}
			}
		}
		return NULL;
	}
	address* get_some(void){
		for(int i=0;i<512;i++){
			std::list<address*>* list = &hashMap[i];
			for(std::list<address*>::iterator it = list->begin();it != list->end(); ++it){
				return *it;
			}
		}
		assert(!"yokunai!");
		return NULL; 
	}
	void erase(const int socket){
		std::pair<const int,int>* entry = &*sockets.find(socket);
		std::list<address*>* list = &hashMap[entry->second];
		std::list<address*>::iterator it = list->begin();
		while(it != list->end()){
			if((*it)->mSocket == socket){
				list->erase(it);
				break;
			}
		}
		sockets.erase(socket);
		return;
	}
};
/* key */

class AbstractKey {
public:
	virtual int Receive(const int socket) = 0;
	virtual int Serialize(const void* buf) const = 0;
	virtual ~AbstractKey(void){};
	virtual bool isMaximum(void) const = 0;
	virtual bool isMinimum(void) const = 0;
	virtual void Maximize(void) = 0;
	virtual void Minimize(void) = 0;
	virtual int size(void) const = 0;
	virtual const char* toString(void) const = 0;
	// virtual operator<();
	// virtual operator>();
	// virtual operator==();
};

// key type: int
class intkey : public AbstractKey{
private:
	//char buff[11];
public :
	int mKey;
	intkey():mKey(0){ };
	intkey(const int k):mKey(k){ }
	int Receive(const int socket){
		int chklen;
		chklen = deep_read(socket,&mKey,4);
		return chklen;
	}
	int Serialize(const void* buf) const{
		int* target = (int*)buf;
		*target = mKey;
		return 4;
	}
	bool isMaximum(void) const {
		return mKey == 0x7fffffff;
	}
	bool isMinimum(void) const {
		return mKey == (int)0x80000000;
	}
	void Maximize(void){
		mKey = 0x7fffffff;
	}
	void Minimize(void){
		mKey = 0x80000000;
	}
	int size(void) const {return 4;}
	const char* toString(void) const {
		static char buff[11] = "";
		unsigned int tmpkey;
		int caster = 1000000000;
		char* pchar = buff;
		if(mKey<0) {
			*pchar++ = '-';
			tmpkey = -mKey;
		}else{
			tmpkey = mKey;
		}
		while(tmpkey/caster == 0) {
			caster /= 10;
		}
		while(tmpkey != 0){
			*pchar++ = (char)(tmpkey/caster + '0');
			tmpkey = tmpkey%caster;
			caster /= 10;
		}
		*pchar = '\0';
		return buff;
	}
	bool operator<(const intkey& rightside) const {
		return mKey < rightside.mKey;
	}
	bool operator>(const intkey& rightside) const {
		return mKey > rightside.mKey;
	}
	bool operator==(const intkey& rightside) const {
		return mKey == rightside.mKey;
	}
};
// key type: char[]
class strkey : public AbstractKey{
public:
	unsigned int mLength;
	char *mKey;
	strkey():mLength(1),mKey(NULL){ }
	strkey(char* k):mLength(strlen(k)),mKey((char*)malloc(mLength+1)){
		memcpy(mKey,k,mLength);
		mKey[mLength] = '\0';
	}
	strkey(char* k,int length):mLength(length),mKey((char*)malloc(length+1)){
		memcpy(mKey,k,mLength);
		mKey[mLength] = '\0';
	}
	strkey(const strkey& k):mLength(k.mLength),mKey((char*)malloc(k.mLength+1)){
		memcpy(mKey,k.mKey,mLength);
		mKey[mLength] = '\0';
	}
	strkey(int tmpkey):mLength(0),mKey((char*)malloc(11)){
		int caster = 1000000000;
		char* pchar = mKey;
		if(tmpkey<0) {
			*pchar++ = '-';
			tmpkey = -tmpkey;
			mLength++;
		}
		while(tmpkey/caster == 0) {
			caster /= 10;
		}
		while(tmpkey != 0){
			*pchar++ = (char)(tmpkey/caster + '0');
			mLength++;
			tmpkey = tmpkey%caster;
			caster /= 10;
		}
		*pchar = '\0';
	}
	int Receive(const int socket){
		if(mKey != NULL) {
			free(mKey);
			mKey = NULL;
		}
		read(socket,&mLength,4);
		if(mLength == 0){
			mKey = NULL;
			return 4;
		}
		mKey = (char*)malloc(mLength+1);
		read(socket,mKey,mLength);
		if(mLength == 1 && mKey[0] == '\0'){
			Minimize();
			return 5;
		}
		
		mKey[mLength] = '\0';
		return mLength + 4;
	}
	int Serialize(const void* buf) const {
		int* intptr;
		char* charptr;
		if(isMaximum()){
			intptr = (int*)buf;
			*intptr = 0;
			return 4;
		}else if(isMinimum()){
			intptr = (int*)buf;
			*intptr = 1;
			charptr = (char*)buf+4;
			*charptr = '\0';
			return 5;
		}
		intptr = (int*)buf;
		*intptr = mLength;
		charptr = (char*)buf;
		charptr += 4;
		memcpy(charptr,mKey,mLength);
		return mLength + 4;
	}
	void Maximize(void){
		if(mKey != NULL){
			free(mKey);
		}
		mKey = NULL;
		mLength = 0;
	}
	void Minimize(void){
		if(mKey != NULL){
			free(mKey);
		}
		mKey = (char*)malloc(1);
		*mKey = '\0';
		mLength=1;
	}
	bool isMaximum(void) const {
		return mLength == 0;
	}
	bool isMinimum(void) const {
		if(mKey == NULL) {
			return 0;
		}
		return *mKey == '\0' && mLength == 1;
	}
	int size(void)const{
		return mLength + 4;
	}
	const char* toString(void) const{
		if(isMaximum()){
			return "*MAX*";
		}else if(isMinimum()){
			return "*MIN*";
		}
		return mKey;
	}
	~strkey(void){
		if(mKey != NULL){
			free(mKey);
		}
		mKey = NULL;
	}
	bool operator<(const strkey& right) const {
		if((!isMaximum() && right.isMaximum()) || (isMinimum() && !right.isMinimum())) {
			return true;
		}else if(isMaximum() || right.isMinimum()){
			return false;
		}
		return strcmp(mKey,right.mKey) < 0;
	}
	bool operator>(const strkey& right) const {
		if((!isMinimum() && right.isMinimum()) || (isMaximum() && !right.isMaximum())) {
			return true;
		}else if(isMinimum() || right.isMaximum()){
			return false;
		}
		//fprintf(stderr,"%s > %s ?\n",toString(),right.toString());
		return strcmp(mKey,right.mKey) > 0;
	}
	bool operator==(const strkey& right) const{
		if((isMinimum() && right.isMinimum()) || (isMaximum() && right.isMaximum())){
			return true;
		}else if(isMinimum() || isMaximum() || right.isMinimum() || right.isMaximum()){
			return false;
		}
		return (mLength == right.mLength) && (strncmp(mKey,right.mKey,mLength) == 0); 
	}
	strkey& operator=(const strkey& rhs) {
		if(mKey != NULL){
			free(mKey);
		}
		mLength = rhs.mLength;
		mKey = (char*)malloc(rhs.mLength+1);
		memcpy(mKey,rhs.mKey,mLength);
		mKey[mLength] = '\0';
		return *this;
	}
};
class AbstractValue{
public:
	virtual int Receive(const int socket) = 0;
	virtual int Serialize(const void* buf) const = 0;
	virtual int size(void) const = 0;
	virtual const char* toString(void) const = 0;
	virtual ~AbstractValue(void){};
};

// value type: int
class intvalue : public AbstractValue{
public :
	int mValue;
	intvalue(void):mValue(0){}
	intvalue(const int v):mValue(v){}
	
	int Receive(const int socket){
		int chklen;
		chklen = read(socket,&mValue,4);
		return chklen;
	}
	int Serialize(const void* buf)const {
		int* target = (int*)buf;
		*target = mValue;
		return 4;
	}
	const char* toString(void) const {
		static char buff[11] = "";
		int tmpvalue = mValue;
		int caster = 1000000000;
		char* pchar = buff;
		if(tmpvalue<0) {
			*pchar++ = '-';
			tmpvalue = -tmpvalue;
		}
		while(tmpvalue/caster == 0) {
			caster /= 10;
		}
		while(tmpvalue != 0){
			*pchar++ = (char)(tmpvalue/caster + '0');
			tmpvalue = tmpvalue%caster;
			caster /= 10;
		}
		*pchar = '\0';
		return buff;		
	}
	int size(void)const {return 4;}
};
// value type: char[]
class strvalue : public AbstractValue{
public:
	unsigned int mLength;
	char* mValue;
	strvalue(void):mLength(0),mValue(NULL) {}
	strvalue(const char* v):mLength(strlen(v)),mValue((char*)malloc(mLength+1)){
		memcpy(mValue,v,mLength);
		mValue[mLength] = '\0';
	}
	strvalue(const char* v,const int len):mLength(len),mValue((char*)malloc(mLength+1)){
		memcpy(mValue,v,mLength);
		mValue[mLength] = '\0';
	}
	strvalue(const strvalue& v):mLength(v.mLength),mValue((char*)malloc(v.mLength+1)){
		memcpy(mValue,v.mValue,mLength);
		mValue[mLength] = '\0';
	}
	strvalue(int v):mLength(0),mValue((char*)malloc(11)){
		int caster = 1000000000;
		char* pchar = mValue;
		if(v<0) {
			*pchar++ = '-';
			v = -v;
			mLength++;
		}
		while(v/caster == 0) {
			caster /= 10;
		}
		while(v != 0){
			*pchar++ = (char)(v/caster + '0');
			mLength++;
			v = v%caster;
			caster /= 10;
		}
		*pchar = '\0';
	}
	int Receive(const int socket){
		if(mValue != NULL) {
			free(mValue);
		}
		read(socket,&mLength,4);
		mValue = (char*)malloc(mLength+1);
		deep_read(socket,mValue,mLength);
		mValue[mLength] = '\0';
		return mLength + 4;
	}
	int Serialize(const void* buf) const {
		int* intptr = (int*)buf;
		*intptr = mLength;
		char* charptr = (char*)buf;
		charptr += 4;
		memcpy(charptr,mValue,mLength);
		return mLength + 4;
	}
	strvalue& operator=(const strvalue& v){
		mLength = v.mLength;
		mValue = (char*)malloc(mLength+1);
		strcpy(mValue,v.mValue);
		return *this;
	}
	const char* toString(void) const {
		if(mLength==0){
			return "";
		}
		return mValue;
	}
	int size(void) const {
		return mLength + 4; 
	}
};

template<typename KeyType>
class sg_neighbor{
private:
	sg_neighbor(void);
	// disable to copy
	sg_neighbor(const sg_neighbor&);
	sg_neighbor& operator=(const sg_neighbor&);
public:
	const KeyType mKey;
	const long long mId;
	const address* mAddress;
	bool mValid;
	
	sg_neighbor(const KeyType& key,const address* ad,const long long id)
		:mKey(key),mId(id),mAddress(ad){
		mValid=1;
	}
	void dump(void)const{
		fprintf(stderr,"Neighbor: ID:%lld, key:%s ",mId,mKey.toString());
		mAddress->dump();
	}
};

	long long gId = 0;
template<typename KeyType, typename ValueType>
class sg_node{
private:
	KeyType mKey;
	ValueType mValue;
	
	// forbids copy
	sg_node(void);
	sg_node(const sg_node&);
	sg_node& operator=(const sg_node&);
	//bool prepare_flag;
public:
	const long long mId;
	sg_neighbor<KeyType>* mLeft[MAXLEVEL];
	sg_neighbor<KeyType>* mRight[MAXLEVEL];
	
	sg_node(const KeyType& k,const ValueType& v)
		:mKey(k),mValue(v),mId(gId++){//,prepare_flag(false){
		for(int i=0;i<MAXLEVEL;i++){
			mLeft[i] = NULL;
			mRight[i] = NULL;
		}
	}
	inline const KeyType& getKey(void){
		return mKey;
	}
	inline const ValueType& getValue(void){
		return mValue;
	}
								  
	void changeValue(const ValueType& v){
		mValue = v;
	}
	void dump(void) const{
		fprintf(stderr,"ID%lld:Key:%s\tValue:%s\n",mId,mKey.toString(),mValue.toString());
		fprintf(stderr,"\tLeft : ");
		for(int i=0;i<MAXLEVEL;i++){
			if(mLeft[i]) fprintf(stderr," %s",mLeft[i]->mKey.toString());
			else fprintf(stderr," None");
		}
		fprintf(stderr,"\n\tRight: ");
		for(int i=0;i<MAXLEVEL;i++){
			if(mRight[i]) fprintf(stderr," %s",mRight[i]->mKey.toString());
			else fprintf(stderr," None");
		}
		fprintf(stderr,"\n");
			
	}
	bool operator<(const class sg_node<KeyType,ValueType>& rightside) const
	{
		return mKey < rightside.mKey;
	}
	/*
	void easydump(void)const {
		fprintf(stderr,"%lld:%s\n",mId,mKey.toString());
	}
	*/
};
typedef sg_node<defkey,defvalue> sg_Node;

/* random functions */
unsigned int sysrand(void){
	FILE* fp = fopen("/dev/random","r");
	int random;
	fread(&random,4,1,fp);
	return random;
}
long long int rand64(void){
	long long int rnd = rand();
	rnd <<= 32;
	rnd ^= rand();
	return rnd;
}

char randchar(void){
	return (char)(rand()%('Z'-'A')+'A');
}
char* randstring(int length){// do free();
	char* string = (char*)malloc(length+1);
	for(int i=0;i<length;i++){
		string[i]=randchar();
	}
	string[length]='\0';
	return string;
}



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
	membership_vector(void):mVector(0){}
	operator long long(){
		return mVector;
	}
	
	int receive(const int socket){
		return deep_read(socket,&mVector,8);
	}
	
	bool operator==(membership_vector rightside){
		return mVector == rightside.mVector;
	}
	bool operator==(long long rightside){
		return mVector == rightside;
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
	*offset += sizeof(int);
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
	
	hash<KeyType> hash_func;
	std::list<neighbor*> hashMap[512];
	
 	neighbor* retrieve(const KeyType& key, const long long id, const address* ad)
	{
		std::list<neighbor*>* list = &hashMap[hash_func.calc(key)&511];
		typename std::list<neighbor*>::iterator it = list->begin();
		 
		while(it != list->end()){
			if((*it)->mId == id && (*it)->mKey == key && *(*it)->mAddress == *ad){
			 	return *it;
			}
			++it;
		}
		neighbor* new_neighbor = new neighbor(key,ad,id); 
		list->push_back(new_neighbor);
		return new_neighbor;
	}
};


/* NodeList */
template<typename KeyType,typename ValueType>
class node_list{
public:
	typedef sg_node<KeyType,ValueType> sg_Node;
	std::list<sg_Node*> nodeList;
	node_list(void){
		nodeList.clear();
	}
	~node_list(void){
		nodeList.clear();
	}
	bool empty(void){
		return nodeList.empty();
	}
	void print(void){
		typename std::list<sg_Node*>::iterator it = nodeList.begin();
		while(it != nodeList.end() ){
			(*it)->dump();
			++it;
		}
	}
	unsigned int size(void) const {
		return nodeList.size();
	}
	void insert(sg_Node* newnode){
		typename std::list<sg_Node*>::iterator it = nodeList.begin();
		if(nodeList.empty()){
			nodeList.push_back(newnode);
		}else{
			while(it != nodeList.end() && (*it)->getKey() < newnode->getKey()){
				++it;
			}
			nodeList.insert(it,newnode);
		}
		return;
	}
	sg_Node* search_by_id(const long long id){
		if(nodeList.empty()){
			return NULL;
		}
		typename std::list<sg_Node*>::iterator it = nodeList.begin();
		while(it != nodeList.end() ){
			if((*it)->mId == id){
				return *it;
			}
			++it;
		}
		return NULL;
	}
	sg_Node* search_by_key(const KeyType& key){// it may return nearest neighbor
		if(nodeList.empty()){
			return NULL;
		}
		typename std::list<sg_Node*>::iterator it = nodeList.begin();
		while( it != nodeList.end() && (*it)->getKey() < key ){
			++it;
		}
		if (it == nodeList.end()){
			--it;
		}
		return *it;
	}
	sg_Node* get_next(const sg_Node& node) const {
		
		return NULL;
	}
};


// devided board to completeclass devided {
class deviding_tag {
private:
	unsigned int length;
	unsigned char* data;
	enum defaultsize{
		BUFFSIZE = 8,
	};
public:
	deviding_tag(void):length(0),data(NULL){
	}
	deviding_tag(const deviding_tag& d):length(d.length),data((unsigned char*)malloc(d.length)){
		if(length > 0){
			memcpy(data,d.data,length);
		}
	}
	~deviding_tag(){
		if(data){
			free(data);
		}
	}
	bool isComplete(void) const{
		for(int i=length-1;i>0;i--){
			if(data[i] != 0)
				return 0;
		}
		return data[0] == 1;
	}
	void dump(void) const {
		unsigned int flag = 0;
		fprintf(stderr,"%dx",length);
		for(int i=length-1;i>=0;i--){
			if(flag == 1 || (data[i] != 0)){
				fprintf(stderr,"%02x",data[i]);
				flag = 1;
			}
		}
		fprintf(stderr,"$");
	}
	void setZero(void){
		length = 0;
		data = NULL;
	}
	void init(void){
		length = BUFFSIZE;
		data = (unsigned char*)malloc(length);		
		for(unsigned int i=length-1;i>0;--i){
			data[i] = 0;
		}
		data[0] = 1;
	}
	
	void receive(const int socket){
		read(socket,&length,sizeof(unsigned int));
		if(data != NULL){
			free(data);
		}
		data = (unsigned char*)malloc(length);
		if(length > 0){
			deep_read(socket,data,length);
		}
	}
	int Serialize(char* buff) const {
		unsigned int* intptr = (unsigned int*)buff;
		*intptr = length;
		unsigned char* charptr = (unsigned char*)buff;
		if(length > 0){
			memcpy(&charptr[4],data,length);
		}
		return 4+length;
	}
	
	deviding_tag& operator=(deviding_tag& rhs){
		assert(data == NULL && length == 0);
		if(length < rhs.length){
			if(data){
				free(data);
			}
			data = (unsigned char*)malloc(rhs.length);
		}else if(rhs.length < length ){
			for(unsigned int i = length; i<rhs.length; i++){
				data[i] = 0;
			}
		}
		length = rhs.length;
		for(unsigned int i=0;i < rhs.length;i++){
			data[i] = rhs.data[i];
		}
		return *this;
	}
	
	deviding_tag& operator/=(deviding_tag& rhs){
		assert(data == NULL && length == 0);
		if(rhs.data[rhs.length-1]&(1<<7)){
			rhs.length *= 2;
			rhs.data = (unsigned char*)realloc(rhs.data,rhs.length);
			for(unsigned int i = rhs.length/2;i<rhs.length;i++){
				rhs.data[i] = 0;
			}
		}
		unsigned char carry = 0,_carry;
		for(unsigned int i = 0;i<rhs.length;++i){
			if(carry == 0 && rhs.data[i] == 0) continue;
			_carry = carry;
			carry = (char)(rhs.data[i]>>7);
			rhs.data[i] = (char)((rhs.data[i]<<1) | _carry);
			if(carry == 0) break;
		}
		return *this = rhs;
	}
	
	deviding_tag& operator+=(deviding_tag& rhs){
		unsigned char carry = 0;
		if(length < rhs.length){
			if(data){
				data = (unsigned char*)realloc(data,rhs.length);
				for(unsigned int i = length;i<rhs.length;i++){
					data[i] = 0;
				}
			}else{
				data = (unsigned char*)malloc(rhs.length);
				for(unsigned int i = 0;i<rhs.length;i++){
					data[i] = 0;
				}
			}
			length = rhs.length;
		}
		int shorter_length = length < rhs.length ? length : rhs.length;
		
		for(int i = shorter_length-1; i>=0 ;i--){
			if(rhs.data[i] == 0 && carry==0) continue;
			unsigned char hit = data[i] & rhs.data[i];
			
			if(hit == 0){
				data[i] |= rhs.data[i];
				
				unsigned char carryhit = (unsigned char)(data[i] & (carry<<7));
				if(carryhit == 0){
					data[i] |= (unsigned char)(carry<<7);
					break;
				}else{
					while(1){
						if(data[i] & carryhit){
							data[i] ^= carryhit;
							carryhit >>= 1;
							continue;
						}
						if(carryhit == 0){
							carry = 1;
							break;
						}
						data[i] |= carryhit;
						carry = 0;
						break;
					}
				}
			}else {
				while(1){
					if(data[i] & hit){// || rhs.data[i] & hit){
						data[i] ^= hit;
						hit >>= 1;
						continue;
					}
					if(hit == 0){
						carry = 1;
						break;
					}
					data[i] |= hit;
					carry = 0;
					break;
				}
			}
		}
		return *this;
	}
	
	unsigned int size(void) const{
		return sizeof(int) + length;
	}
};


	// range query identifier
class range_query{ // [length] [beginkey] [endkey] [begin_closed] [end_closed] [devided_tag] with string
private:
	unsigned int mSize;
	char* mQuery;
	int mSocket;
	
public:
	deviding_tag mTag;
	range_query(const range_query& rhs):mSize(rhs.mSize),mQuery((char*)malloc(mSize+1)),mSocket(rhs.mSocket),mTag(rhs.mTag){
		memcpy(mQuery,rhs.mQuery,mSize);
		mQuery[mSize] = '\0';
	}
	range_query(void):mSize(0),mQuery(NULL),mSocket(0),mTag(){ };
	range_query(const char* query):mSize(strlen(query)),mQuery((char*)malloc(mSize)),mSocket(0),mTag(mTag){
		memcpy(mQuery,query,mSize);
		mQuery[mSize] = '\0';
	}
	void set(const char* query,const int size){
		if(mQuery != NULL){
			free(mQuery);
		}
		mSize =  size;
		mQuery = (char*)malloc(mSize+1);
		memcpy(mQuery,query,mSize);
		mQuery[mSize] = '\0';
	}
	
	range_query(const int socket) :mSize(0),mQuery(NULL),mSocket(0),mTag(){
		receive(socket);
	}
	
	void hashing(void){
		mSize++;
		mQuery = (char*)realloc(mQuery,mSize);
		mQuery[mSize-1] = '*';
		mQuery[mSize] = '\0';
	}
	
	void receive(const int socket){
		if(mQuery != NULL){
			free(mQuery);
			mQuery = NULL;
		}
		deep_read(socket,(char*)&mSize,sizeof(int));
		mQuery = (char*)malloc(mSize+1);
		deep_read(socket,mQuery,mSize);
		mQuery[mSize] = '\0';
		mTag.receive(socket);
	}
	int Serialize(char* buff)const{
		unsigned int* intptr = (unsigned int*)buff;
		*intptr = mSize;
		memcpy(&buff[sizeof(int)],mQuery,mSize);
		mTag.Serialize(&buff[sizeof(int)+mSize]);
		return sizeof(int) + mSize + mTag.size();
	}
	const char* toString(void) const{
		return mQuery;
	}
	range_query& operator+=(range_query& rhs){
		// marge
		assert( strcmp(mQuery,rhs.mQuery) == 0 );
		mTag += rhs.mTag;
		return *this;
	}
	
	void setSocket(const int socket){
		mSocket = socket;
	}
	int getSocket(void) const {
		return mSocket;
	}
	
	int getLength(void) const{
		return mSize;
	}
	const char* getData(void) const{
		return mQuery;
	}
	
	bool operator==(const range_query& rhs) const {
		return (strcmp(mQuery,rhs.mQuery) == 0);
	}
	bool operator<(const range_query& rhs) const {
		return (strcmp(mQuery,rhs.mQuery) < 0);
	}
	range_query& operator=(const range_query& rhs) {
		if(mQuery != NULL){
			free(mQuery);
		}
		mSize = rhs.mSize;
		mQuery = (char*)malloc(mSize);
		strcpy(mQuery,rhs.mQuery);
		mQuery[mSize] = '\0';
		mSocket = rhs.mSocket;
		return *this;
	}
	~range_query(void){
		if(mQuery != NULL){
			free(mQuery);
		}
	}
	bool isComplete(void) const{
		return mTag.isComplete();
	}
	unsigned int size(void) const {
		return 4 + mSize + mTag.size();
	}
};
	// container of range queue value
class queue_buffer{
private:
	char* data;
	int size;
public:
	queue_buffer(void):data(NULL) { }
	queue_buffer(const queue_buffer& buff){
		size = buff.size;
		data = (char*)malloc(size + 1);
		strncpy(data,buff.data,size);
		data[size] = '\0';
	}
	queue_buffer(const int socket){
		this->receive(socket);
	}
	queue_buffer(const char* string){
		data = NULL;
		this->set(string);
	}
	int receive(const int socket){
		// | 4  | nbyte |
		// |size| data  |
		int chklen;
		deep_read(socket,&size,4);
		data = (char*)malloc(size);
		chklen = deep_read(socket,data,size);
		return chklen;
	}
	void set(const char* string){
		size = strlen(string);
		if(data!=NULL){
			free(data);
		}
		data = (char*)malloc(size);
		strcpy(data,string);
		data[size] = '\0';
	}
	int send(const int socket) const{
		return deep_write(socket,data,size);
	}
	queue_buffer& operator=(queue_buffer& rhs){
		/* caution
		 * It's movement, not copy */
		data = rhs.data;
		size = rhs.size;
		rhs.data = NULL;
		rhs.size = 0;
		return *this;
	}
	~queue_buffer(void){
		if(data!= NULL){
			free(data);
		}
	}
};
class queue_buffer_list{
private:
	std::list<queue_buffer> list;
	
	queue_buffer_list& operator=(queue_buffer_list&);
public:
	queue_buffer_list(void){
		list.clear();
	}
	/*
	  queue_buffer_list(const queue_buffer_list& rhs){
	  assert(rhs.list.empty());
	  }
	*/
	void push_back(const int socket){
		list.push_back(queue_buffer(socket));
	}
	void push_back(const char* string){
		//fprintf(stderr,"key:%s\n",string);
		list.push_back(queue_buffer(string));
	}
	void send_all(const int socket){
		while(list.size()>0){
			list.front().send(socket);
			list.pop_front();
		}
	}
};

class rquery_list{
private:	
	std::list<std::pair<range_query, queue_buffer_list> > hashMap[512];
	
	pthread_mutex_t mapmutex;
	bytehash hash_func;
	
	DEBUG(int foundnum);
public:
	rquery_list(void){
		DEBUG(foundnum = 0;)
			pthread_mutex_init(&mapmutex,NULL);
	}
	bool found(range_query& query,const int socket){
		unsigned int hashed = murmurhash_bytes(query.getData(),query.getLength()) & 511;
		std::list<std::pair<range_query, queue_buffer_list> >& list = hashMap[hashed];
		std::list<std::pair<range_query, queue_buffer_list> >::iterator it = list.begin();
		if(list.empty()){
			fprintf(stderr,"not found query [%s]\n",query.toString());
			assert(!"arienai");
		}
		while(it != list.end()){
			if(it->first == query){
				break;
			}
			++it;
		}
		assert(it!=list.end());
		
		DEBUG(foundnum++;);
		DEBUG_OUT("foundnum %d\n",foundnum);
		
		*(const_cast<range_query*>(&it->first)) += query;
		receive_and_set_kvp(socket,&(it->second));
		DEBUG_OUT("marged tag ");
		DEBUG(it->first.mTag.dump());
		
		if(it->first.isComplete()){
			//fprintf(stderr,"start to send\n");
			it->second.push_back("END\r\n");
			it->second.send_all(it->first.getSocket());
			list.erase(it);
			
			DEBUG_OUT("range query %s ok!\n",query.toString());
			DEBUG(foundnum =0);
		}
		return true;
	}
	bool notfound(range_query& query){
		pthread_mutex_lock(&mapmutex);
		unsigned int hashed = murmurhash_bytes(query.getData(),query.getLength()) & 511;
		std::list<std::pair<range_query, queue_buffer_list> >* list = &hashMap[hashed];
		std::list<std::pair<range_query, queue_buffer_list> >::iterator it = list->begin();
		
		DEBUG_OUT("query set %s to %d\b",query.toString(),hashed);
		if(list->empty()){
			fprintf(stderr,"not found query [%s] hash %d\n",query.toString(),hashed);
			assert(!"arienai");
		}
		while(it != list->end()){
			if(it->first == query){
				break;
			}
			++it;
		}
		assert(it!=list->end());
		
		*(const_cast<range_query*>(&it->first)) += query;
		DEBUG_OUT("marged tag ");
		DEBUG(it->first.mTag.dump());
		
		if(it->first.isComplete()){
			//fprintf(stderr,"start to send\n");
			it->second.push_back("END\r\n");
			it->second.send_all(it->first.getSocket());
			list->erase(it);
			pthread_mutex_unlock(&mapmutex);
			DEBUG(foundnum =0;)
				return true;
		}
		pthread_mutex_unlock(&mapmutex);
		return true;
	}
	void set_queue(const int socket,range_query* query){
		unsigned int hashed;
		query->setSocket(socket);
		pthread_mutex_lock(&mapmutex);
		
		while(1){
			hashed = murmurhash_bytes(query->getData(),query->getLength()) & 511;
			std::list<std::pair<range_query, queue_buffer_list> >& list = hashMap[hashed];
			std::list<std::pair<range_query, queue_buffer_list> >::iterator it = list.begin();
			while(it != list.end()){
				if(it->first == *query){
					query->hashing();
					hashed = hash_func.calc(query->getData(),query->getLength()) & 511;
					DEBUG_OUT("hashing!!! to %s\n",query->toString());
					break;
				}
				++it;
			}
			if(it == list.end()){
				break;
			}
		}
		DEBUG_OUT("query set %s to %d",query->toString(),hashed);
		hashMap[hashed].push_back(std::pair<range_query, queue_buffer_list>(*query,queue_buffer_list()));
		pthread_mutex_unlock(&mapmutex);
	}
private:
	int count_digit(int counted) const {
		int cnt = 0;
		while(counted != 0){
			counted /= 10;
			cnt++;
		}
		return cnt;
	}
	int set_digit(char* buff,int param) const {
		int caster = 100000000,length = 0;
		while(param/caster == 0) caster /= 10;
		while(param != 0){
			buff[length] = (char)(param/caster + '0');
			param = param%caster;
			caster /= 10;
			length++;
		}
		return length;
	}
	void receive_and_set_kvp(const int socket,queue_buffer_list* list){// create me
		// it receive left part of RangeFoundOp
		int length,offset=0;
		char* buff;
		deep_read(socket, &length, sizeof(int));
		buff= (char*)malloc(6 + length + 3 + 12);
		memcpy(&buff[offset],"VALUE ",6);   offset+=6;
		offset += read(socket, &buff[offset], length);
		DEBUG(buff[offset] = '\0');
		DEBUG_OUT("key:%s",&buff[offset-length]);
		memcpy(&buff[offset]," 0 ",3);  offset+=3;
		deep_read(socket, &length,sizeof(int));
		offset += set_digit(&buff[offset], length);
		memcpy(&buff[offset], "\r\n", 2); offset += 2;
		buff[offset] = '\0';
		list->push_back(buff);
		buff= (char*)malloc(6 + length + 3 + 10); offset = 0;
		offset += deep_read(socket, &buff[offset], length);
		memcpy(&buff[offset], "\r\n", 2); offset+=2;
		buff[offset] = '\0';
		list->push_back(buff);
		free(buff);
	}
};

template<typename Keytype,typename Valuetype>
class suspend_list{
private:
	typedef suspend<Keytype,Valuetype> Suspend; 
	
	class entry{
	private:
		int socket;
		std::list<Keytype> key;
		Suspend sus;
	public:
		entry(const int sock,const int counter):socket(sock),sus(sock,counter){
			key.clear();
		};
		inline Suspend& getSuspend(void) {
			return this->sus;
		}
		inline std::list<Keytype>& getKeyList(void){
			return this->key;
		}
		int getSocket(void) const {
			return this->socket;
		}
	};
	
	std::list<entry*> mList; // raw data
	
	std::multimap<int, entry*> mSocketMap;
	std::multimap<Keytype, entry*> mKeyMap;
	
	// disable copy
	suspend_list(const suspend_list<Keytype,Valuetype>&);
	suspend_list<Keytype,Valuetype>& operator=(const suspend_list<Keytype,Valuetype>&);
public:
	suspend_list(void){
		mList.clear();
	}
	Suspend* search(const int socket) {
		typename std::multimap<int, entry*>::iterator it = mSocketMap.find(socket);
		if(it != mSocketMap.end()){
			return &mSocketMap.find(socket)->second->getSuspend();
		}else{
			return NULL;
		}
	}
	Suspend* search(const Keytype& key) {
		typename std::multimap<Keytype, entry*>::iterator it = mKeyMap.find(key);
		if(it != mKeyMap.end()){
			return &mKeyMap.find(key)->second->getSuspend();
		}else{
			return NULL;
		}
	}
	Suspend* add_or_retrieve(const int socket, const int counter=0){
		typename std::multimap<int, entry*>::iterator it = mSocketMap.find(socket);
		entry* newEntry;
		if(it != mSocketMap.end()){ // exists
			newEntry = it->second;
			newEntry->getSuspend().addCounter(counter);
		}else{
			newEntry = new entry(socket,counter);
			mSocketMap.insert(std::pair<int,entry*>(socket,newEntry));
		}
		return &newEntry->getSuspend();
	}
	Suspend* setKey(const int socket, const Keytype& key){
		typename std::multimap<int, entry*>::iterator it = mSocketMap.find(socket);
		entry* foundEntry;
		if(it != mSocketMap.end()){ // exists
			foundEntry = it->second;
			foundEntry->getKeyList().push_back(key);
			mKeyMap.insert(std::pair<Keytype,entry*>(key,foundEntry));
			return &foundEntry->getSuspend();
		}else {
			return NULL;
		}
	}
		
	void erase(const Keytype& key){
		typename std::multimap<Keytype, entry*>::iterator it = mKeyMap.find(key);
		if(it != mKeyMap.end()){
			typename std::multimap<int, entry*>::iterator sockIt = mSocketMap.find(it->second->getSocket());
			typename std::list<Keytype>::iterator keyIt = sockIt->second->getKeyList().begin();
			while(keyIt != sockIt->second->getKeyList().end()){
				mKeyMap.erase(*keyIt);
				++keyIt;
			}
			mSocketMap.erase(sockIt->first);
			delete sockIt->second;
		}
	}
};
int send_to_address(const address* ad,const char* buff,const int bufflen){
	int sendsize = 0,sentbuf = 0, leftbuf = bufflen;
	assert(ad->mSocket != 0);
	
	while(leftbuf > 0){
		sendsize = ad->send(&buff[sentbuf],leftbuf);
		if(sendsize>0){
			sentbuf += sendsize;
			leftbuf -= sendsize;
		}else{
			ad->dump();
			perror("send to address");
			exit(1);
		}
	}
	return sendsize;
}

int send_treatop(const address& aAddress,const long long targetId,const strkey& key, const int originip, const long long originid, const unsigned short originport,const membership_vector& mv){
	char *buff;
	int buffindex,bufflen;
	buffindex = 0;
	bufflen = 1+8+key.size()+4+8+2+8;
	buff = (char*)malloc(bufflen);
	buff[buffindex++] = TreatOp;
	
	serialize_longlong(buff,&buffindex,targetId);
	buffindex += key.Serialize(&buff[buffindex]);
	serialize_int(buff,&buffindex,originip);
	serialize_longlong(buff,&buffindex,originid);
	serialize_short(buff,&buffindex,originport);
	serialize_longlong(buff,&buffindex,mv.mVector);
	assert(bufflen == buffindex && "buffsize ok?");
	
	int sentsize = send_to_address(&aAddress,buff,bufflen);
	if(sentsize != buffindex){
		fprintf(stderr,"could not send treatOp\n");
		assert("could not treat!");
	}
	free(buff);
	return sentsize;
}
int send_linkop(const address& aAddress,const long long& targetid,const strkey& originkey,const long long originid,int targetlevel,char left_or_right){
	int buffindex=0;
	int bufflen = 1+8+originkey.size()+4+8+2+4+1;
	char* buff = (char*)malloc(bufflen);
	buffindex = 0;
	//serialize
	buff[buffindex++] = LinkOp;
	serialize_longlong(buff,&buffindex,targetid);
	buffindex += originkey.Serialize(&buff[buffindex]);
	serialize_int(buff,&buffindex,settings.myip);
	serialize_longlong(buff,&buffindex,originid);
	serialize_short(buff,&buffindex,settings.myport);
	serialize_int(buff,&buffindex,targetlevel);
	buff[buffindex++] = left_or_right;
	assert(buffindex == bufflen);
	int sentsize = send_to_address(&aAddress,buff,bufflen);
	if(sentsize != buffindex){
		fprintf(stderr,"could not send LinkOP\n");
		assert(!"could not link!");
	}
	free(buff);
	return sentsize;
}
int send_introduceop(const address& aAddress,const long long targetid,const strkey& originkey,const long long originid,const int originip, const short originport, const int targetlevel, const long long originvector){
	int buffindex=0;
	int bufflen = 1+8+originkey.size()+4+8+2+4+8;
	char* buff = (char*)malloc(bufflen);
	
	
	buff[buffindex++] = IntroduceOp;
	serialize_longlong(buff,&buffindex,targetid);
	buffindex += originkey.Serialize(&buff[buffindex]);
	serialize_int(buff,&buffindex,originip);
	serialize_longlong(buff,&buffindex,originid);
	serialize_short(buff,&buffindex,originport);
	serialize_int(buff,&buffindex,targetlevel);
	serialize_longlong(buff,&buffindex,originvector);
	
	assert(bufflen==buffindex);
	int sentsize = send_to_address(&aAddress,buff,bufflen);
	if(sentsize != bufflen){
		fprintf(stderr,"could not send introduceOP\n");
	}
	free(buff);
	return sentsize;
}

void print_range(const AbstractKey& begin,const AbstractKey& end,const char left_closed,const char right_closed){
	fprintf(stderr,"%s%s-%s%s",left_closed==1 ?"[":"(",begin.toString(),end.toString(),right_closed==1?"]":")");
}
void range_forward(const unsigned int level,
				   const long long targetid,
				   const address& ad,
				   const strkey& begin,
				   const strkey& end,
				   const char left_closed,
				   const char right_closed,
				   const int originip,
				   const unsigned short originport,
				   range_query* query,
				   const bool alltag_send)
{
	deviding_tag newtag;
	DEBUG_OUT("rangeing tag");
	DEBUG(query->mTag.dump());
	if(!alltag_send){
		newtag /= query->mTag;
	}else{
		newtag = query->mTag;
	}
	DEBUG_OUT("->");
	DEBUG(newtag.dump());
	DEBUG_OUT("\n");
	const int bufflen = 1 + 8 + 4 + begin.size() + end.size() + 1 + 1 + 4 + 2 + query->size();
	int buffindex = 0;
	char* buff = (char*)malloc(bufflen);
	//fprintf(stderr,"range forward:[%s-%s] %s\n",begin.toString(),end.toString(),query->toString());
	
	
	if(settings.verbose > 3){
		print_range(begin,end,left_closed,right_closed);
		fprintf(stderr," in level:%d\n",level);
	}
	
	buff[buffindex++] = RangeOp;
	serialize_longlong(buff,&buffindex,targetid);
	serialize_int(buff,&buffindex,level);
	buffindex += begin.Serialize(&(buff[buffindex]));
	buffindex += end.Serialize(&(buff[buffindex]));
	buff[buffindex++] = left_closed;
	buff[buffindex++] = right_closed;
	serialize_int(buff,&buffindex,originip);
	serialize_short(buff,&buffindex,originport);
	buffindex += query->Serialize(&(buff[buffindex]));
	
	assert(buffindex == bufflen);
	send_to_address(&ad,buff,buffindex);
	free(buff);
	DEBUG_OUT(" send RangeOp to ");
	DEBUG(ad.dump());
	DEBUG_OUT(" range ");
	DEBUG(print_range(begin,end,left_closed,right_closed));
	DEBUG_OUT(" from{%s}\n",query->toString());
}


	// get left or right
char direction(const strkey& fromKey ,const strkey& toKey){
	return fromKey < toKey ? Right : Left;
}
char inverse(const char left_or_right){
	return left_or_right == Left ? Right : Left; 
}

#endif
