OPTS = -O4 -Wall
OBJS = kttcp.o mytcplib.o mulio.o 

target:kttcp

skipgraph:skipgraph.o mytcplib.o mulio.o node.o
	g++ skipgraph.o mytcplib.o mulio.o node.o -o skipgraph -pthread -lrt -Wall

skipgraph.o:skipgraph.cpp
	g++ skipgraph.cpp -o skipgraph.o -c $(OPTS)

node.o:node.cpp
	g++ node.cpp -o node.o -c $(OPTS)

kttcp:$(OBJS)
	g++ $(OBJS) -o kttcp -pthread -lrt -Wall

kttcp.o:kttcp.cpp mulio.h
	g++ kttcp.cpp -o kttcp.o -c $(OPTS)

mytcplib.o: mytcplib.cpp mytcplib.h
	g++ mytcplib.cpp -o mytcplib.o -c $(OPTS)

mulio.o:mulio.cpp mulio.h
	g++ mulio.cpp -o mulio.o -c $(OPTS)
