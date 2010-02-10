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

namespace localfunc{
	char* itoa(unsigned int data){
		static char string[10];
		unsigned int caster = 1000000000;
		char* pchar = string;
		while(data/caster == 0) {
			caster /= 10;
		}
		while(data > 0){
			*pchar++ = (char)(data/caster + '0');
			data = data%caster;
			caster /= 10;
		}
		*pchar = '\0';
		return string;
	}
};
// key-value pair
template <typename keytype,typename valuetype>
class node_kvp : public node {
private:
	const keytype key;
	
	node_kvp(void);
	node_kvp(const node_kvp&);
	node_kvp& operator=(const node_kvp&);
public:
	valuetype value;
	node_kvp(const keytype& k):key(k){ }
	node_kvp(const keytype& k,const valuetype& v):key(k),value(v){ }
	//node_kvp(const node_kvp& k):key(k.key),value(k.value){ }
	
	int send(const int socket) const{
		int sentlength = 0;
		char* string;
		if(value.mValue != NULL){
			sentlength = write(socket,"VALUE ",6);
			sentlength += write(socket,key.mKey,key.mLength);
			sentlength += write(socket," 0 ",3);
			string = localfunc::itoa(value.mLength);
			sentlength += write(socket,string,strlen(string));
			sentlength += write(socket,"\r\n",2);
			sentlength += write(socket,value.mValue,value.mLength);
			sentlength += write(socket,"\r\n",2);
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
	
	node* create(const keytype& key){
		return new node_kvp<keytype,valuetype>(key);
	}
};

// plane string
class node_str : public node{
private:
	int length;
	char* str;
	
	node_str(void);
	node_str(const node_str&);
	node_str& operator=(const node_str&);
public:
	node_str(const char* s):length(strlen(s)),str((char*)malloc(length+1)){
		strcpy(str,s);
	}
	
	int send(const int socket) const{
		int sentlength = write(socket,str,length);
		assert(sentlength == length);
		//fprintf(stderr,"send %d bytes[%s]\n",length,str);
		return sentlength;
	}
	int receive(const int){
		return 0;
	}
	
	inline bool hasPair(void){
		return false;
	}
	~node_str(void){
		free(str);
		str = NULL;
	}
	
	node* create(const char* str ){
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
	const int mSocket;
	std::list<node*> suspend_list;
	int counter;
public:

	suspend(const int s):mSocket(s),counter(0){ }
	suspend(const int s,const int c):mSocket(s),counter(c){ }
	
	inline void add(const keytype& key){
		++counter;
		suspend_list.push_back(new node_kvp<keytype,valuetype>(key));
	}
	inline void add(const int data){
		suspend_list.push_back(new node_str(localfunc::itoa(data)));
	}
	inline void add(const char* c){
		suspend_list.push_back(new node_str(c));
	}
	inline int getCounter(void) const{
		return counter;
	}
	inline void addCounter(const int cnt){
		counter += cnt;
	}
	
	inline void receive_value(const int socket,const keytype key){
		valuetype* value;
		value = search_by_key(key);
		//fprintf(stderr,"received !! %d\n",counter);
		if (value->mValue == NULL) {
			--counter;
		}
		value->Receive(socket);
	}
	
	inline void decrement_cnt(void){
		//fprintf(stderr,"decremented !! %d\n",counter);
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
			//fprintf(stderr,"iterating\n");
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
