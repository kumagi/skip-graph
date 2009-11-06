#include "node.h"

node::node(const long long key){
	mKey=key;
	mMaxLevel = 16;
	mDeleteFlag = 0;
	
	mRight.reserve(mMaxLevel);
	mLeft.reserve(mMaxLevel);
}
node::~node(void){
	free(mKey);
	
	for(int i = mMaxLevel; i>=0 ; --i){
		if(mRight[i]) { close(mRight[i]); mRight[i]=0; }
		if(mLeft[i])  { close(mRight[i]); mLeft[i]=0; }
	}
	mRight.clear();
	mLeft.clear();
}
void node::extendMaxLevel(int level){
	while(mMaxLevel<level){
		mRight.push_back(0);
		mLeft.push_back(0);
		mMaxLevel++;
	}
}
