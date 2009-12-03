#include "memcache_buffer.h"
#include <stdlib.h>//malloc realloc
#include <string.h>//strncmp
#include <sys/socket.h>//recv
#include <errno.h>//errno
#include <unistd.h>//write
#include <assert.h>
#include <stdio.h>//fprintf

int natoi(char* str,int length){
	int ans = 0;
	while(length > 0){
		assert('0' <= *str && *str <= '9' );
		ans = ans * 10 + *str - '0';
		str++;
	}
	return ans;
}

memcache_buffer::memcache_buffer(int socket):mSocket(socket){
	assert(socket != 0);
	fprintf(stderr,"mSocket:%d\n",mSocket);
	mState = state_free;
	mSize = 128; // buffer size
	mStart = 0; // head of parse
	mRead = 0; // received data
	mChecked = 0; // checked data
	mReft = mSize; // reft buffer
	mBuff = (char*)malloc(mSize);
}
	
void memcache_buffer::ParseOK(void){
	mState = state_free;
	if(mChecked == mRead){
		if(mSize > 128){
			mBuff = (char*)realloc((void*)mBuff,128);
		}
		mChecked = mRead = mStart = 0;
		mSize = mReft = 128;
		mState = state_free;
	}
}
	
const int& memcache_buffer::getState(void) const{
	return mState;
}
	
const int& memcache_buffer::getSocket(void) const{
	return mSocket;
}

void memcache_buffer::receive(void){
	bool EndFlag = 0;
	while(!EndFlag){
		switch(mState){
		case state_free:
			mStart = mChecked;
		case state_continue:
			readmax();
			while(strncmp(&mBuff[mChecked],"\r\n",2) != 0 && mChecked < mRead){
				mChecked++;
			}
			if(mChecked == mRead){
				mState = state_continue;
				EndFlag = 1;
				break;
			}
			mBuff[mChecked] = '\0';
				
			parse(&mBuff[mStart],&mBuff[mChecked-1]);
			mStart = mChecked + 1;
			EndFlag = 1;
			break;
			
		case state_value:
			readmax();
			if (mRead - mStart < moreread){
				EndFlag = 1;
				break;
			}
			if(strncmp(&mBuff[mStart + moreread + 1],"\r\n",2) != 0){
				string_write("CLIENT_ERROR bad data chunk");
				mState = state_error;
				EndFlag = 1;
				break;
			}
			mChecked += moreread;
			tokens[SET_VALUE].str = &mBuff[mStart];
			tokens[SET_VALUE].length = moreread;
			mStart += moreread;
			mState = state_set;
			EndFlag = 1;
			break;
		}
	}
}
bool memcache_buffer::operator<(const memcache_buffer& rightside) const{
	return mSocket < rightside.mSocket;
}
void memcache_buffer::readmax(void){
	int newread;
	fprintf(stderr,"socket:%d\n",mSocket);
	do{
		if(mReft==0){
			mBuff=(char*)realloc(mBuff,mSize*2);
			mReft = mSize;
			mSize *= 2;
		}
		newread = recv(mSocket,&mBuff[mRead],mReft,MSG_DONTWAIT);
		if(newread == 0){
			mState = state_close;
			break;
		}
		fprintf(stderr,"%d\n",mSocket);
		mRead += newread;
		mReft -= newread;
	}while(errno!=EAGAIN && errno!=EWOULDBLOCK);
}
	
	inline void memcache_buffer::string_write(char* string) const{
		int len = strlen(string);
		write(mSocket,string,len);
		write(mSocket,"\r\n",2);
	}
		
inline void memcache_buffer::parse(char* start,char* end){
	int cnt;
	assert(start < end);
	if(strncmp(start,"set ",4) == 0){ // set [key] <flags> <exptime> <length>
		mState = state_value;
		start += 3;
		cnt = read_tokens(start,4);
		if(cnt < 4){
			string_write("ERROR");
			mState = state_error;
			return;
		}
		moreread = natoi(tokens[SET_LENGTH].str,tokens[SET_LENGTH].length);
	}else if(strncmp(start,"get ",4) == 0){ // get [key] ([key] ([key] ([key].......)))
		mState = state_get;
		start += 3;
		cnt = read_tokens(start,8);
	}else if(strncmp(start,"delete ",7) == 0){ // delete [key] ([key] ([key] ([key].......)))
		mState = state_delete;
		start += 6;
		cnt = read_tokens(start,8);
	}else{
		assert(!"invalid operation\n");
	}
}
		
int memcache_buffer::read_tokens(char* str,int maxtokens){
	int cnt;
	for(int i=0; i<maxtokens; i++){
		// get head of token
		while(*str == ' ' && *str != '\0') {
			str++;
		}
		tokens[i].str = str;
		if(*str == '\0'){
			return i;
		}
		// measure length of token
		cnt = 0;
		while(*str != ' ' && *str != '\0'){
			str++;
			cnt++;
		}
		tokens[i].length = cnt;
		if(*str == '\0'){
			return i+1;
		}
	}
	return maxtokens;
}
