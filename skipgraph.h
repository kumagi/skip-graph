#ifndef SKIPGRAPH
#define SKIPGRAPH
#define NDEBUG
#define MAXLEVEL 4
#include "mytcplib.h"
#include "suspend.hpp"
#include "MurmurHash2A.cpp"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <vector>
#include <list>
#include <set>
#include <map>

#define defkey strkey
#define defvalue strvalue

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
	intkey(){};
	intkey(const int k){
		mKey = k;
	}
	int Receive(const int socket){
		int chklen;
		chklen = read(socket,&mKey,4);
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
			*pchar++ = tmpkey/caster + '0';
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
	char *mKey;
	unsigned int mLength;
	strkey(){
		mKey=NULL;
		mLength=1;
	}
	strkey(char* k){
		mLength = strlen(k);
		mKey = (char*)malloc(mLength+1);
		memcpy(mKey,k,mLength);
		mKey[mLength] = '\0';
	}
	strkey(char* k,int length){
		mLength = length;
		mKey = (char*)malloc(mLength+1);
		memcpy(mKey,k,mLength);
		mKey[mLength] = '\0';
	}
	strkey(const strkey& k){
		mLength = k.mLength;
		mKey = (char*)malloc(mLength+1);
		memcpy(mKey,k.mKey,mLength);
		mKey[mLength] = '\0';
	}
	strkey(int tmpkey){
		mKey = (char*)malloc(11);
		mLength = 0;
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
			*pchar++ = tmpkey/caster + '0';
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
		//fprintf(stderr,"$$[%d]$$",mLength+4);
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
		if(!isMaximum() && right.isMaximum() || isMinimum() && !right.isMinimum()) {
			return true;
		}else if(isMaximum() || right.isMinimum()){
			return false;
		}
		return strcmp(mKey,right.mKey) < 0;
	}
	bool operator>(const strkey& right) const {
		if(!isMinimum() && right.isMinimum() || isMaximum() && !right.isMaximum()) {
			return true;
		}else if(isMinimum() || right.isMaximum()){
			return false;
		}
		//fprintf(stderr,"%s > %s ?\n",toString(),right.toString());
		return strcmp(mKey,right.mKey) > 0;
	}
	bool operator==(const strkey& right) const{
		if((isMinimum() && right.isMinimum()) || isMaximum() && right.isMaximum()){
			return true;
		}
		return (mLength == right.mLength) && (strncmp(mKey,right.mKey,mLength) == 0); 
	}
	strkey& operator=(const strkey& rhs) {
		if(mKey != NULL){
			free(mKey);
		}
		mLength = rhs.mLength;
		mKey = (char*)malloc(rhs.mLength+1);
		strcpy(mKey,rhs.mKey);
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
	intvalue(void) {
		mValue=0;
	}
	intvalue(const int v){
		mValue = v;
	}
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
			*pchar++ = tmpvalue/caster + '0';
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
	char* mValue;
	unsigned int mLength;
	strvalue(void) {
		mValue = NULL;
		mLength = 0;
	}
	strvalue(const char* v){
		mLength = strlen(v);
		mValue = (char*)malloc(mLength+1);
		strcpy(mValue,v);
	}
	strvalue(const char* v,const int len){
		mLength = len;
		mValue = (char*)malloc(mLength+1);
		strcpy(mValue,v);
	}
	strvalue(const strvalue& v){
		mLength = v.mLength;
		mValue = (char*)malloc(mLength+1);
		strcpy(mValue,v.mValue);
	}
	strvalue(int v){
		mValue = (char*)malloc(11);
		mLength = 0;
		int tmpvalue = v;
		int caster = 1000000000;
		char* pchar = mValue;
		if(tmpvalue<0) {
			*pchar++ = '-';
			tmpvalue = -tmpvalue;
			mLength++;
		}
		while(tmpvalue/caster == 0) {
			caster /= 10;
		}
		while(tmpvalue != 0){
			*pchar++ = tmpvalue/caster + '0';
			mLength++;
			tmpvalue = tmpvalue%caster;
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
		read(socket,mValue,mLength);
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
	void operator=(const strvalue& v){
		mLength = v.mLength;
		mValue = (char*)malloc(mLength+1);
		strcpy(mValue,v.mValue);
		return;
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
	
	int send(void* buff,int& bufflen){
		assert(!"don't call this funcion\n");
		return 0;
	}
	sg_neighbor(const KeyType& key,const address* ad,const long long id)
		:mKey(key),mId(id),mAddress(ad){
		mValid=1;
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
public:
	const long long mId;
	sg_neighbor<KeyType>* mLeft[MAXLEVEL];
	sg_neighbor<KeyType>* mRight[MAXLEVEL];
	
	sg_node(const KeyType& k,const ValueType& v)
		:mKey(k),mValue(v),mId(gId++){
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
	operator const long long(){
		return mVector;
	}
	bool operator==(membership_vector rightside){
		return mVector == rightside.mVector;
	}
	bool operator==(long long rightside){
		return mVector == mVector;
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
	
	std::list<neighbor*> nList;
	
 	neighbor* retrieve(const KeyType& key, const long long id, const address* ad)
	{
		typename std::list<neighbor*>::iterator it;
		it = nList.begin();
		 
		while(it != nList.end()){
			if((*it)->mKey == key && (*it)->mId == id && *(*it)->mAddress == *ad){
			 	return *it;
			}
			++it;
		}
		nList.push_back(new neighbor(key,ad,id));
		return nList.back();
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
	sg_Node* search_by_key(const KeyType& key){// it may  neighbor
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
};


// devided board to completeclass devided {

class devided {
private:
	unsigned long long mData;
	
	void merge(devided* data){
		merge(&data->mData);
	}
	void merge(unsigned long long* data){
		unsigned long long hit = mData & *data;
		if(!hit){
			mData |= *data;
			*data = 0;
			return;
		}
		if((hit & (hit-1)) == 0){ // 1 bit only
			mData ^= hit;
			hit >>=  1;
			merge(&hit);
			return;
		}
		assert(!"dont merge midway data each other.\n");
	}
public:
	devided(void):mData(1){ }
	bool isComplete(void) const{
		return mData == 1;
	}
	void dump(void) const {
		fprintf(stderr,"0x%llx\n",mData);
	}
	void setZero(void){
		mData = 0;
	}
	void init(void){
		mData = 1;
	}
	void receive(const int socket){
		read(socket,&mData,sizeof(devided));
	}
	int Serialize(char* buff) const {
		long long* longptr = (long long*)buff;
		*longptr = mData;
		return 8;
	}
 	devided& operator/=(devided& rhs){
		assert(mData == 1);
		rhs.mData <<= 1;
		mData = rhs.mData;
		return *this;
	}
	devided& operator+=(devided& rhs){
		merge(&rhs);
		return *this;
	}
	unsigned int size(void) const{
		return sizeof(long long int);
	}
};

// range query identifier
class range_query{ // [length] [beginkey] [endkey] [begin_closed] [end_closed] [devided_tag] with string
private:
	char* mQuery;
	unsigned int mSize;
	int mSocket;
	
public:
	devided mTag;
	range_query(const range_query& rhs):mSocket(rhs.mSocket),mTag(rhs.mTag){
		mQuery = NULL;
		set(rhs.mQuery,rhs.mSize);
	}
	range_query(void):mQuery(NULL){ };
	range_query(const char* query):mTag(mTag){
		mSize =  strlen(query);
		mQuery = (char*)malloc(mSize);
		strcpy(mQuery,query);
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
	range_query(const int socket) :mSocket(0){
		receive(socket);
	}
	void receive(const int socket){
		if(mQuery != NULL){
			free(mQuery);
			mQuery = NULL;
		}
		read(socket,&mSize,sizeof(int));
		mQuery = (char*)malloc(mSize+1);
		read(socket,mQuery,mSize);
		mQuery[mSize] = '\0';
		mTag.receive(socket);
	}
	int Serialize(char* buff)const{
		int* intptr = (int*)buff;
		*intptr = mSize;
		memcpy(&buff[sizeof(int)],mQuery,mSize);
		mTag.Serialize(&buff[sizeof(int)+mSize]);
		return this->size();
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
		read(socket,&size,4);
		data = (char*)malloc(size);
		chklen = read(socket,data,size);
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
		return write(socket,data,size);
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
	queue_buffer_list(const queue_buffer_list& rhs){
		assert(rhs.list.empty());
	}
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
	std::multimap<range_query, queue_buffer_list> rqlist;
	
	int count_digit(int counted){
		int cnt = 0;
		while(counted != 0){
			counted /= 10;
			cnt++;
		}
		return cnt;
	}
	int set_digit(char* buff,int param){
		int caster = 100000000,length = 0;
		while(param/caster != 0) caster /= 10;
		while(param != 0){
			buff[length] = param/caster + '0';
			param /= 10;
			caster /= 10;
			length++;
		}
		return length; 
	}
	void set_key(const int socket,char** key){
		int length,offset = 0;
		read(socket, &length, sizeof(int));
		
		*key = (char*)malloc(length + 12 + count_digit(length));
		memcpy(*key,"VALUE ",6); 	offset += 6;
		offset += read(socket, (*key)+offset, length); // read key
		memcpy((*key)+offset, " 0 ", 3);  offset += 3;
		offset += set_digit((*key)+offset,length);
		memcpy((*key)+offset,"\r\n",2); offset += 2;
		(*key)[offset++] = '\0';
		
		//fprintf(stderr,"## %d == %d ##",offset,length+ 14 +count_digit(length));
		assert(offset == length+12+count_digit(length));
	}
	
public:
	bool found(range_query& query,const int socket){
		std::multimap<range_query, queue_buffer_list>::iterator it = rqlist.find(query);
		if(it == rqlist.end()){
			fprintf(stderr,"not found query [%s]\n",query.toString());
			assert(!"arienai");
		}
		*(const_cast<range_query*>(&it->first)) += query;
		char* keyline;
		set_key(socket,&keyline);
		it->second.push_back(keyline);
		free(keyline);
		it->second.push_back(socket);
		it->second.push_back("\r\n");
		
		if(it->first.isComplete()){
			it->second.push_back("END\r\n");
			it->second.send_all(it->first.getSocket());
			rqlist.erase(it);
		}
		return true;
	}
	bool notfound(range_query& query){
		std::multimap<range_query, queue_buffer_list>::iterator it = rqlist.find(query);
		if(it == rqlist.end()){
			assert(!"arienai");
		}
		query.mTag.dump();
		fprintf(stderr,"+");
		it->first.mTag.dump();
		*(const_cast<range_query*>(&it->first)) += query;
		if(it->first.isComplete()){
			it->second.push_back("END\r\n");
			it->second.send_all(it->first.getSocket());
			rqlist.erase(it);
		}/*else{
			it->first.mTag.dump();
			}*/
		return true;
	}
	void set_queue(const int socket,range_query* query){
		query->setSocket(socket);
		fprintf(stderr,"begin ######################\n");
		rqlist.insert(std::pair<range_query, queue_buffer_list>(*query,queue_buffer_list()));
		fprintf(stderr,"end ######################\n");
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
	int sendsize;
	assert(ad->mSocket != 0);
	
	sendsize = write(ad->mSocket,buff,bufflen);
	if(sendsize<=0){
		//fprintf(stderr,"send_to_address:failed to write %d\n",ad->mSocket);
		exit(0);
	}
	return sendsize;
}

int create_treatop(char** buff,const long long targetId,const defkey* key, const long long myId, const long long vector){
	int buffindex,bufflen;
	buffindex = 0;
	bufflen = 1+8+key->size()+4+8+2+8;
	*buff = (char*)malloc(bufflen);
	(*buff)[buffindex++] = TreatOp;
		
	serialize_longlong(*buff,&buffindex,targetId);
	buffindex += key->Serialize(&(*buff)[buffindex]);
	serialize_int(*buff,&buffindex,settings.myip);
	serialize_longlong(*buff,&buffindex,myId);
	serialize_short(*buff,&buffindex,settings.myport);
	serialize_longlong(*buff,&buffindex,vector);
	assert(bufflen == buffindex && "buffsize ok?");
	
	return bufflen;
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
	if(sentsize<0){
		fprintf(stderr,"could not send LinkOP\n");
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
	if(sentsize<0){
		fprintf(stderr,"could not send introduceOP\n");
	}
	free(buff);
	return sentsize;
}

void print_range(const AbstractKey& begin,const AbstractKey& end,const char left_closed,const char right_closed){
	fprintf(stderr,"%s%s-%s%s",left_closed==1 ?"[":"(",begin.toString(),end.toString(),right_closed==1?"]":")");
}
void range_forward(const unsigned int level,const long long targetid,const address& ad,const strkey& begin,const strkey& end,const char left_closed,const char right_closed,const int originip, const unsigned short originport, range_query* query, bool alltag_send){
	const int bufflen = 1 + 8 + 4 + begin.size() + end.size() + 1 + 1 + 4 + 2 + query->size();
	int buffindex = 0;
	devided newtag;
	char* buff = (char*)malloc(bufflen);
	fprintf(stderr,"range forward:[%s-%s] %s\n",begin.toString(),end.toString(),query->toString());
	if(!alltag_send){
		newtag /= query->mTag;
	}
	
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
}



// get left or right
char direction(const strkey& fromKey ,const strkey& toKey){
	return fromKey < toKey ? Right : Left;
}
char inverse(const char left_or_right){
	return left_or_right == Left ? Right : Left; 
}

#endif
