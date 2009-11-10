#define MAXLEVEL 3
#include "mytcplib.h"
#include<vector>
#include<set>

struct settings{
	unsigned short useport;
	int myip;
	int targetip;
	int verbose;
	int threads;
}settings;
const char keyop = 7;


struct address{
	int ip;
	unsigned short port;
	unsigned long long id;
};
typedef address address_t;


template<typename KeyType>
class sg_neighbor{
public:
	KeyType mKey;
	long long mId;
	int mSocket;
	char mFlagValid;
	address_t mAddress;
	
	int send(void* buff,int& bufflen){
		assert(!"don't call this funcion\n");
		return 0;
	}
	void set(int socket,KeyType& key,int ip,long long id,unsigned short port){
		mKey = key;
		mId = id;
		mSocket = socket;
		mAddress.ip = ip;
		mAddress.port = port;
		mAddress.id = id;
		mFlagValid = 1;
	}
};

long long gId = 0;
template<typename KeyType, typename ValueType>
class sg_node{
public:
	KeyType mKey;
	ValueType mValue;
	unsigned int mMaxLevel;
	long long mId;
	sg_neighbor<KeyType> mPointer;
	std::vector<sg_neighbor<KeyType> > mRight;
	std::vector<sg_neighbor<KeyType> > mLeft;
	sg_node(){
		mId = gId;
		mMaxLevel = MAXLEVEL;
		mPointer.mId = gId;
		mPointer.mAddress.ip = settings.myip;
		mPointer.mAddress.port = settings.useport;
		mPointer.mAddress.id = gId++;
		mRight.reserve(mMaxLevel);
		mLeft.reserve(mMaxLevel);
	}
	sg_node(KeyType& k,ValueType& v){
		mMaxLevel = 32;
		mKey = k;
		mValue = v;
		mId = gId;
		mPointer.mKey = k;
		mPointer.mId = gId;
		mPointer.mAddress.ip = settings.myip;
		mPointer.mAddress.port = settings.useport;
		mPointer.mAddress.id = gId++;
		mRight.reserve(mMaxLevel);
		mLeft.reserve(mMaxLevel);
	}
	int operator<(sg_node<KeyType,ValueType> rightside){
		return mKey < rightside.mKey;
	}
	int operator<(const KeyType rightside){
		return mKey < rightside;
	}
	int operator>(const KeyType rightside){
		return mKey > rightside;
	}
	int operator<=(const KeyType rightside){
		return mKey <= rightside;
	}
	int operator>=(const KeyType rightside){
		return mKey >= rightside;
	}
	void setMaxLevel(unsigned int newmax){
		if(newmax>mRight.size() || newmax>mLeft.size()){
			mRight.reserve(newmax);
			mLeft.reserve(newmax);
			mMaxLevel = newmax;
		}
	}
};

// key type: int
class intkey{
public :
	int mKey;
	int mValidFlag;
	intkey(){mValidFlag = 0;};
	intkey(int k);
	int Receive(int socket);//it returns received size
	int Serialize(void* buff);//it returns writed size
	void Maximize(void);
	void Minimize(void);
	int operator<(intkey& rightside)
	{
		//fprintf(stderr,"in operator :%d < %d ?\n",mKey,rightside.mKey);
		return mKey<rightside.mKey;
	}
	int operator>(const intkey& rightside)
	{
		//fprintf(stderr,"in operator :%d > %d ?\n",mKey,rightside.mKey);
		return mKey>rightside.mKey;
	}
	int operator<=(const intkey& rightside)
	{
		//fprintf(stderr,"in operator :%d < %d ?\n",mKey,rightside.mKey);
		return mKey<=rightside.mKey;
	}
	int operator>=(const intkey& rightside)
	{
		//fprintf(stderr,"in operator :%d > %d ?\n",mKey,rightside.mKey);
		return mKey>=rightside.mKey;
	}
	int operator==(const intkey rightside)
	{
		return mKey==rightside.mKey;
	}
	int size(void){return 4;}
};
intkey::intkey(int k){
	mKey = k;
	mValidFlag = 1;
}
void intkey::Maximize(void){
	mKey = 0x7fffffff;
	mValidFlag = 1;
}
void intkey::Minimize(void){
	mKey = 0x80000000;
	mValidFlag = 1;
}
inline int intkey::Receive(int socket)
{
	int chklen;
	chklen = read(socket,&mKey,4);
	if(chklen == 4){
		mValidFlag = 1;
	}
	return chklen;
}
inline int intkey::Serialize(void* buf)
{
	if(mValidFlag==0){
		return 0;
	}
	int* target = (int*)buf;
	*target = mKey;
	return 4;
}

/* value */
class intvalue{
public :
	int mValue;
	intvalue(void){};
	intvalue(int& v);
	intvalue(int v);
	void setValue(int& v);
	int Receive(int socket);
	int Serialize(void* buff);
	int size(void){return 4;}
};
intvalue::intvalue(int& v){
	mValue = v;
}
intvalue::intvalue(int v){
	mValue = v;
}
inline void intvalue::setValue(int& v)
{
	mValue = v;
}
inline int intvalue::Receive(int socket)
{
	int chklen;
	chklen = read(socket,&mValue,4);
	return chklen;
}
inline int intvalue::Serialize(void* buf)
{
	int* target = (int*)buf;
	*target = mValue;
	return 4;
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
	bool getVector(int bit){
		return (mVector>>bit)&1;
	}
	int operator[](int bit){
		return getVector(bit);
	}
	
	void printVector(void){
		unsigned int upper = (unsigned int)(mVector>>32);
		printf("vector:%x%x\n",upper,(unsigned int)mVector);
		printf("\n");
	}
	int compare(class membership_vector mv){
		int count = 0;
		long long diff = mVector^mv.mVector;
		while(!(diff&1) && count!=MAXLEVEL-1){
			count++;
			diff >>= 1;
		}
		return count;
	}
	int compare(long long mv){
		int count = 0;
		long long diff = mVector^mv;
		while(!(diff&1) && count!=MAXLEVEL-1){
			count++;
			diff >>= 1;
		}
		return count;
	}
	membership_vector(void){
		mVector = rand64();
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
