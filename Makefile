OPTS = -O2 -Wall
OBJS = kttcp.o mytcplib.o mulio.o 

target:skipgraph

skipgraph:skipgraph.o mytcplib.o mulio.o
	g++ skipgraph.o mytcplib.o mulio.o -o skipgraph -pthread -lrt

skipgraph.o:skipgraph.cpp skipgraph.h
	g++ skipgraph.cpp -o skipgraph.o -c $(OPTS)

kttcp:$(OBJS)
	g++ $(OBJS) -o kttcp -pthread -lrt

kttcp.o:kttcp.cpp mulio.h
	g++ kttcp.cpp -o kttcp.o -c $(OPTS)

mytcplib.o: mytcplib.cpp mytcplib.h
	g++ mytcplib.cpp -o mytcplib.o -c $(OPTS)

mulio.o:mulio.cpp mulio.h
	g++ mulio.cpp -o mulio.o -c $(OPTS)
