#ifndef SUSPEND
#define SUSPEND
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "skipgraph.h"
#include <list>

class node{
public:
	virtual int send(const int socket) const = 0;
	virtual int receive(const int socket) = 0;
	virtual bool hasPair(void) = 0;
	virtual ~node(void) {};
};

template <typename keytype,typename valuetype>
class node_kvp : public node {
private:
	const keytype key;
public:
	valuetype value;
	node_kvp(const keytype& k):key(k){ }
	node_kvp(const keytype& k,const valuetype& v):key(k),value(v){ }
	node_kvp(const node_kvp& k):key(k.key),value(k.value){ }
	
	int send(const int socket) const{
		int sentlength = 0;
		if(value.mValue != NULL){
			sentlength = write(socket,value.mValue,value.mLength);
			assert(sentlength == value.mLength);
			fprintf(stderr,"send %d bytes[%s]\n",sentlength,value.mValue);
		}
		return sentlength;
	}
	
	inline int receive(const int socket){
		return value.Receive(socket);
	}
	inline bool hasPair(void){
		return true;
	}
	inline const keytype& getKey(void){
		return key;
	}
	
	~node_kvp(void){ }
	
	static node* create(const keytype& key){
		return new node_kvp<keytype,valuetype>(key);
	}
};

class node_str : public node{
private:
	int length;
	char* str;
public:
	node_str(const char* s):length(strlen(s)),str((char*)malloc(length+1)){
		strcpy(str,s);
	}
	node_str(const node_str& s):length(s.length),str((char*)malloc(length+1)){
		strcpy(str,s.str);
	}
	
	int send(const int socket) const{
		int sentlength = write(socket,str,length);
		assert(sentlength == length);
		fprintf(stderr,"send %d bytes[%s]\n",length,str);
		return sentlength;
	}
	inline int receive(const int socket){
		return 0;
	}
	
	inline bool hasPair(void){
		return false;
	}
	~node_str(void){
		free(str);
		str = NULL;
	}
	
	static node* create(const char* str ){
		return new node_str(str);
	}
};


template <typename keytype,typename valuetype>
class suspend_node{
private:
	node* item;
public:
	suspend_node(const keytype& key):item(node_kvp<keytype,valuetype>::create(key)){}
	suspend_node(const char* str):item(node_str::create(str)){}
	
	inline int receive(const int socket){
		return item->receive(socket);
	}
	inline int send(const int socket) const {
		int sentlength = item->send(socket); 
		return sentlength;
	}
	~suspend_node(void){
	}
};


template <typename keytype,typename valuetype>
class suspend{
private:
	int mSocket;
	std::list<node*> suspend_list;
	int counter;
public:

	suspend(const int s):mSocket(s),counter(0){ }
	suspend(const int s,const int c):mSocket(s),counter(c){ }
	
	inline void add(const keytype& key){
		++counter;
		suspend_list.push_back(new node_kvp<keytype,valuetype>(key));
	}
	inline void add(const char* c){
		suspend_list.push_back(new node_str(c));
	}
	
	inline void receive_value(const int socket,const keytype key){
		valuetype* value;
		value = search_by_key(key);
		if (value->mValue == NULL) {
			--counter;
		}
		value->Receive(socket);
	}
	
	inline void decrement_cnt(void){
		--counter;
	}
	
	inline bool send_if_can(void) {
		node* front;
		if(counter == 0){
			while(!suspend_list.empty()){
				front = suspend_list.front();
				front->send(mSocket);
				suspend_list.pop_front();
			}
			return true;
		}
		return false;
	}
private:
	valuetype* search_by_key(const keytype& key) {
		typename std::list<node*>::iterator it = suspend_list.begin();
		node_kvp<keytype,valuetype>* tmpkey;
		while(it != suspend_list.end()){
			fprintf(stderr,"iterating\n");
			if((*it)->hasPair()){
				tmpkey = (node_kvp<keytype,valuetype>*)*it;
				if(tmpkey->getKey() == key){
					return &tmpkey->value;
				}
			}
			++it;
		}
		return NULL;
	}
	
};

#endif
