#include "memcache_buffer.h"
#include <stdlib.h>//malloc realloc
#include <string.h>//strncmp
#include <sys/socket.h>//recv
#include <errno.h>//errno
#include <unistd.h>//write
#include <assert.h>
#include <stdio.h>//fprintf
//#define NDEBUG

static const char interrupt[] = {-1,-12,-1,-3,6};

int natoi(char* str,int length){
	int ans = 0;
	while(length > 0){
		//fprintf(stderr,"%d:%d,",length,*str);
		assert('0' <= *str && *str <= '9' );
		ans = ans * 10 + *str - '0';
		str++;
		length--;
	}
	return ans;
}

memcache_buffer::memcache_buffer(int socket):mSocket(socket),mState(state_free),mSize(128),mBuff((char*)malloc(mSize)),mStart(0),mChecked(0),mRead(0),mReft(mSize),mCloseflag(0),moreread(0),tokenmax(1){
	if(socket == 0){
		fprintf(stderr,"socket is 0, pass\n");
	}
	int result = fcntl(socket, F_SETFL, O_NONBLOCK);
	if(result < 0){
		perror("memcache server nonblocking");
	}
	//fprintf(stderr,"buffer initialized\n");
}
memcache_buffer::~memcache_buffer(void){
	if(mBuff!=NULL)
		free(mBuff);
	mBuff = NULL;
	close(mSocket);
}

void memcache_buffer::ParseOK(void){
	mState = state_free;
	
	//fprintf(stderr,"parse OK, %d buffer left\n", mRead - mChecked);
	if(mChecked == mRead){
		if(mSize > 128){
			mBuff = (char*)realloc((void*)mBuff,128);
		}
		mChecked = mRead = mStart = 0;
		mSize = mReft = 128;
		mState = state_free;
		mBuff[0] = '\0';
	}else{
		fprintf(stderr,"failed to free buffer read[%d] checked[%d]\n",mRead,mChecked);
	}
	if(mCloseflag == 1){
		close(mSocket);
		mState = state_close;
	}
	//fprintf(stderr,"freed state mRead:%d mChecked:%d\n",mRead,mChecked);
}
	
const int& memcache_buffer::getState(void) const{
	return mState;
}
	
const int& memcache_buffer::getSocket(void) const{
	return mSocket;
}

int memcache_buffer::receive(void){
	int newdata = 0;
	int tokennum = 0;
	
	switch(mState){
	case state_free:
		mStart = mChecked;
	case state_continue:
		newdata = readmax();
		if(newdata < 0){
			tokennum = -1;
			mState = state_close;
			break;
		}
		
		//fprintf(stderr,"before[%d]\n",mChecked);
		while(strncmp(&mBuff[mChecked],"\r\n",2) != 0 && mChecked < mRead){
			mChecked++;
		}
		//fprintf(stderr,"after[%d]\n",mChecked);
		if(mChecked == mRead){
			if(strncmp(&mBuff[mStart],interrupt,5) == 0 || strncmp(&mBuff[mChecked-5],interrupt,5)){
				mState = state_close;
				break;
			}
			fprintf(stderr,"interpret:%d,mChecked=%d\n ",mBuff[mChecked],mChecked);
			mState = state_continue;
			break;
		}
		mBuff[mChecked] = '\0';
		mBuff[mChecked+1] = '\0';
		
		//fprintf(stderr,"length:%d,[%s]\n",strlen(&mBuff[mStart]),&mBuff[mStart]);
		tokennum = parse(&mBuff[mStart]);
		mChecked += 2;
		mStart = mChecked;
		
		if(mState != state_value){
			break;
		}
	case state_value:
		//fprintf(stderr,"state value\n");
		newdata = readmax();
		if (mRead - mStart < moreread){
			//fprintf(stderr,"value length unsatisfied\n");
			break;
		}
		
		if(strncmp(&mBuff[mStart + moreread],"\r\n",2) != 0 ){
			string_write("CLIENT_ERROR bad data chunk");
			mState = state_error;
			while(strncmp(&mBuff[mChecked],"\r\n",2) != 0 && mChecked < mRead){
				mChecked++;
			}
			mChecked += 2;
			break;
		}
		tokennum++;
		mBuff[mStart + moreread] = '\0';
		mChecked += moreread + 2;
		tokens[SET_VALUE].str = &mBuff[mStart];
		tokens[SET_VALUE].length = moreread;
		tokens[SET_VALUE].str[moreread] = '\0';
		mStart += moreread + 2;
		mState = state_set;
		break;
	default :
		mRead = mChecked;
		mState = state_free;
		break;
	}
	//fprintf(stderr,"next state:%d\n",mState);
	return tokennum;
}

bool memcache_buffer::operator<(const memcache_buffer& rightside) const{
	return mSocket < rightside.mSocket;
}

int memcache_buffer::readmax(void){
	int newread;
	int totalread = 0;
	do{
		if(mReft==0){
			mBuff=(char*)realloc(mBuff,mSize*2);
			mReft = mSize;
			mSize *= 2;
		}
		newread = read(mSocket,&mBuff[mRead],mReft);
		if(newread > 0){
			mRead += newread;
			mReft -= newread;
			totalread += newread;
		}
	}while(errno!=EAGAIN && errno!=EWOULDBLOCK && newread != 0);
	if(newread == 0){
		mCloseflag = 1;
	}
	/*
	fprintf(stderr,"dumping mChecked:%d mRead:%d\n",mChecked,mRead);
	fprintf(stderr,"received data %d bytes:",totalread);
	for(int i=0;i<mRead;i++){
		fprintf(stderr,"%d,",mBuff[i]);
	}
	//*/
	//fprintf(stderr,"\n");
	return totalread;
}
	
inline void memcache_buffer::string_write(std::string str) const{
	write(mSocket,str.data(),str.length());
	write(mSocket,"\r\n",2);
}
		
inline int memcache_buffer::parse(char* start){
	int cnt = 0;
	//fprintf(stderr,"parse start[%s]\n",start);
	if(strncmp(start,"set ",4) == 0){ // set [key] <flags> <exptime> <length>
		start += 3;
		mState = state_value;
		cnt = read_tokens(start,4);
		if(cnt < 4){
			string_write("ERROR");
			mState = state_error;
			return cnt;
		}
		moreread = natoi(tokens[SET_LENGTH].str,tokens[SET_LENGTH].length);
		//fprintf(stderr,"waiting for value for %d length\n",moreread);
	}else if(strncmp(start,"get ",4) == 0){ // get [key] ([key] ([key] ([key].......)))
		mState = state_get;
		start += 3;
		cnt = read_tokens(start,30);
	}else if(strncmp(start,"rget ",5) == 0){ // rget [beginkey] [endkey] [left_closed] [right_closed]
		mState = state_rget;
		start += 4;
		cnt = read_tokens(start,4);
	}else if(strncmp(start,"delete ",7) == 0){ // delete [key] ([key] ([key] ([key].......)))
		mState = state_delete;
		start += 6;
		cnt = read_tokens(start,8);
	}else if(strncmp(start,"stats",4) == 0){ // stats
		mState = state_stats;
	}else if(strncmp(start,"quit",4) == 0){ // quit
		mState = state_close;
	}else{
		fprintf(stderr,"operation:%s\n",start);
		mState = state_error;
		//assert(!"invalid operation\n");
	}
	return cnt;
}

// it measures \0 to \0
int memcache_buffer::read_tokens(char* str,const int maxtokens){
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
		*str++ = '\0'; // delimiter
	}
	return maxtokens;
}
