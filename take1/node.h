#ifndef NODE
#define NODE

#include <list>
#include <vector>
#include <string.h>

class node{
public:
	unsigned int mKeyLength;
	char* mKey;
	
	//there is socket list
	std::vector<int> mRight;
	std::vector<int> mLeft;
	unsigned int mMaxLevel;
	int mDeleteFlag;
	node(char* key);
	~node(void);
	int operator<(const node& a)const{
		return strncmp(mKey,a.mKey,mKeyLength);
	}
	int operator<(const char* a)const{
		return strncmp(mKey,a,mKeyLength);
	}
	void extendMaxLevel(int level);
};

#endif
