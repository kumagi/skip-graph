#include<vector>

struct address{
	int ipaddress;
	unsigned short port;
	unsigned int id;
};
typedef address address_t;


template<typename KeyType>
struct sg_neighbor{
	KeyType key;
	int socket;
	address_t ipaddress;
};


template<typename KeyType, typename ValueType>
struct sg_node{
	KeyType key;
	ValueType value;
	int maxlevel;
	std::vector<sg_neighbor<KeyType> > right;
	std::vector<sg_neighbor<KeyType> > left;
};

// key type: int
class intkey{
public :
	int key;
	intkey(int k);
	int Receive(int socket,void* buff);//it returns received size
	int Serialize(void* buff);//it returns writed size 
};
inline int intkey::Receive(int socket,void* buff)
{
	int chklen;
	chklen = read(socket,&key,4);
	return chklen;
}
inline int intkey::Serialize(void* buf)
{
	int* target = (int*)buf;
	*target = key;
	return 4;
}

class intvalue{
public :
	int value;
	intvalue(int v);
	int Receive(int socket,void* buff);
	int Serialize(void* buff);
};
inline int intvalue::Receive(int socket,void* buff)
{
	int chklen;
	chklen = read(socket,&value,4);
	return chklen;
}
inline int intvalue::Serialize(void* buf)
{
	int* target = (int*)buf;
	*target = value;
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
		while(!(diff&1)){
			count++;
			diff >>= 1;
		}
		return count;
	}
	membership_vector(void){
		unsigned int upper = rand();
		mVector = (unsigned int)rand();
		mVector |= ((long long)upper)<<32;
	}
};

/* random functions */
unsigned int sysrand(void){
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
