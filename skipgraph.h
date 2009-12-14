#ifndef SKIPGRAPH
#define SKIPGRAPH
#define MAXLEVEL 8
#include "mytcplib.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <vector>
#include <list>
#include <set>

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
	int mLength;
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
	strkey(int k){
		mKey = (char*)malloc(11);
		mLength = 0;
		int tmpkey = k;
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
			return 1;
		}else if(isMaximum() || right.isMinimum()){
			return 0;
		}
		return strcmp(mKey,right.mKey) < 0;
	}
	bool operator>(const strkey& right) const {
		if(!isMinimum() && right.isMinimum() || isMaximum() && !right.isMaximum()) {
			return 1;
		}else if(isMinimum() || right.isMaximum()){
			return 0;
		}
		//fprintf(stderr,"%s > %s ?\n",toString(),right.toString());
		return strcmp(mKey,right.mKey) > 0;
	}
	bool operator==(const strkey& right) const{
		return (mLength == right.mLength) && (strncmp(mKey,right.mKey,mLength) == 0); 
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
	int mLength;
	strvalue(void) {
		mValue = NULL;
		mLength = 0;
	}
	strvalue(const char* v){
		mLength = strlen(v);
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
		return mLength;
	}
};

template<typename KeyType>
class sg_neighbor{
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
			printf("%lld: key=%s  value=%s",(*it)->mId,(*it)->getKey().toString(),(*it)->getValue().toString());
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
	void insert(sg_Node* newnode){//TODO
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
	sg_Node* search_by_key(const KeyType& key){// it may returns nearest neighbor
		if(nodeList.empty()){
			return NULL;
		}
		typename std::list<sg_Node*>::iterator it = nodeList.begin();
		while( it != nodeList.end() && (*it)->getKey() < key ){
			++it;
		}
		return *it;
	}
};
#endif
