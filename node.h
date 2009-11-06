#ifndef NODE
#define NODE

#include <list>
#include <vector>
#include <string.h>

class node{
public:
	long long mKey;
	
	//there is socket list
	std::vector<int> mRight;
	std::vector<long long> mRightKey;
	std::vector<int> mLeft;
	std::vector<long long> mLeftKey;
	
	unsigned int mMaxLevel;
	int mDeleteFlag;
	node(const long long key);
	~node(void);
	int operator<(const node& a)const{
		return mKey<a.mKey;
	}
	int operator<(long long a)const{
		return mKey<a;
	}
	void extendMaxLevel(int level);
};

#endif
