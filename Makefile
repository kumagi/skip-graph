OPTS = -std=c++0x -Wall -march=x86-64 -O0 -g
OBJS = kttcp.o mytcplib.o mulio.o 
WARNS = -Wextra -Wformat=2 -Wstrict-aliasing=2 -Wcast-qual -Wcast-align \
	-Wwrite-strings -Wfloat-equal -Wpointer-arith -Wswitch-enum#  -Weffc++


target:skipgraph 

skipgraph:skipgraph.o mytcplib.o mulio.o memcache_buffer.o  suspend.hpp
	g++44  skipgraph.o mytcplib.o mulio.o memcache_buffer.o -o skipgraph -pthread -lrt $(OPTS) $(WARNS)

skipgraph.o:skipgraph.cpp skipgraph.h
	g++44 skipgraph.cpp -o skipgraph.o -c $(OPTS) $(WARNS)

memcache_buffer.o:memcache_buffer.cpp memcache_buffer.h
	g++44 memcache_buffer.cpp -o memcache_buffer.o -c $(OPTS) $(WARNS)

mytcplib.o: mytcplib.cpp mytcplib.h
	g++44 mytcplib.cpp -o mytcplib.o -c $(OPTS) $(WARNS)

mulio.o:mulio.cpp mulio.h
	g++44 mulio.cpp -o mulio.o -c $(OPTS) $(WARNS)



clean :
	rm *.o