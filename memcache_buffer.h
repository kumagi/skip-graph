#include <string>

enum memcache_buffer_constants{
	TOKENMAX = 8,
	SET_KEY = 0,
	SET_FLAGS = 1,
	SET_EXPTIME = 2,
	SET_LENGTH = 3,
	SET_VALUE = 4,
	GET_KEY = 0,
	RGET_BEGIN = 0,
	RGET_END = 1,
	RGET_LEFT_CLOSED = 2,
	RGET_RIGHT_CLOSED = 3,
	DELETE_KEY = 0,
};

class memcache_buffer{
private:
	const int mSocket;
	int mState;
	int mSize;
	char* mBuff;
	int mStart;
	int mChecked;
	int mRead;
	int mReft;
	int mCloseflag;
	int moreread;
	memcache_buffer(const memcache_buffer&);
	memcache_buffer& operator=(const memcache_buffer&);
public:
 	enum state {
		state_free,
		state_set,
		state_get,
		state_rget,
		state_delete,
		state_stats, // [stats] command 
		state_value, // wait until n byte receive
		state_continue, // not all data received
		state_close,
		state_error,
	};
	struct token{
		char* str;
		int length;
	} tokens[TOKENMAX];
	int tokenmax;
	
	memcache_buffer(int socket);
	~memcache_buffer(void);
	
	void ParseOK(void);
	
	const int& getState(void) const;
	
	const int& getSocket(void) const;
	int receive(void);
	bool operator<(const memcache_buffer& rightside) const;
private:
	int readmax(void);
	inline void string_write(std::string str) const;
	inline int parse(char* start);
	int read_tokens(char* str,int maxtokens);
	void init_buffer(void);
};

int natoi(char* str,int length);
