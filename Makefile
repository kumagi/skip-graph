OPTS = -Wall -O0 -g
OBJS = kttcp.o mytcplib.o mulio.o 

target:skipgraph 

skipgraph:skipgraph.o mytcplib.o mulio.o memcache_buffer.o aso.o suspend.hpp
	g++  skipgraph.o mytcplib.o mulio.o memcache_buffer.o aso.o -o skipgraph -pthread -lrt $(OPTS)

skipgraph.o:skipgraph.cpp skipgraph.h
	g++ skipgraph.cpp -o skipgraph.o -c $(OPTS)

memcache_buffer.o:memcache_buffer.cpp memcache_buffer.h
	g++ memcache_buffer.cpp -o memcache_buffer.o -c $(OPTS)

mytcplib.o: mytcplib.cpp mytcplib.h
	g++ mytcplib.cpp -o mytcplib.o -c $(OPTS)

mulio.o:mulio.cpp mulio.h
	g++ mulio.cpp -o mulio.o -c $(OPTS)

aso.o:aso.cpp aso.hpp
	g++ aso.cpp -o aso.o -c $(OPTS)


clean :
	rm *.o