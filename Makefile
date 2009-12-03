OPTS = -Wall -O0
OBJS = kttcp.o mytcplib.o mulio.o 

target:skipgraph

skipgraph:skipgraph.o mytcplib.o mulio.o memcache_buffer.o
	g++ skipgraph.o mytcplib.o mulio.o memcache_buffer.o -o skipgraph -pthread -lrt $(OPTS)

skipgraph.o:skipgraph.cpp skipgraph.h
	g++ skipgraph.cpp -o skipgraph.o -c $(OPTS)

memcache_buffer.o:memcache_buffer.cpp memcache_buffer.h
	g++ memcache_buffer.cpp -o memcache_buffer.o -c $(OPTS)

mytcplib.o: mytcplib.cpp mytcplib.h
	g++ mytcplib.cpp -o mytcplib.o -c $(OPTS)

mulio.o:mulio.cpp mulio.h
	g++ mulio.cpp -o mulio.o -c $(OPTS)
