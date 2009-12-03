
enum memcache_buffer_constants{
	TOKENMAX = 8,
	SET_KEY = 0,
	SET_FLAGS = 1,
	SET_EXPTIME = 2,
	SET_LENGTH = 3,
	SET_VALUE = 4,
	GET_KEY = 0,
	DELETE_KEY = 0,
};

class memcache_buffer{
private:
	const int mSocket;
	int mState;
	char* mBuff;
	int mSize;
	int mStart;
	int mChecked;
	int mRead;
	int mReft;
	
	int moreread;
public:
	enum state {
		state_free,
		state_set,
		state_get,
		state_delete,
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
	
	void ParseOK(void);
	
	const int& getState(void) const;
	
	const int& getSocket(void) const;
	void receive(void);
	bool operator<(const memcache_buffer& rightside) const;
private:
	void readmax(void);
	inline void string_write(char* string) const;
	inline void parse(char* start,char* end);
	int read_tokens(char* str,int maxtokens);
};

int natoi(char* str,int length);
