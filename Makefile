OPTS = -Wall -O0
OBJS = kttcp.o mytcplib.o mulio.o 

target:skipgraph
target:tester

tester:tester2.o mytcplib.o mulio.o
	g++ tester2.o mytcplib.o mulio.o -o tester -pthread -lrt
tester2.o:tester2.cpp
	g++ tester2.cpp -o tester2.o -c $(OPTS)

settest:settest.o mytcplib.o mulio.o
	g++ settest.o mytcplib.o mulio.o -o settest -pthread -lrt
settest.o:settest.cpp
	g++ settest.cpp -o settest.o -c $(OPTS)

searchtest:searchtest.o mytcplib.o mulio.o
	g++ searchtest.o mytcplib.o mulio.o -o searchtest -pthread -lrt
searchtest.o:searchtest.cpp
	g++ searchtest.cpp -o searchtest.o -c $(OPTS)

skipgraph:skipgraph.o mytcplib.o mulio.o
	g++ skipgraph.o mytcplib.o mulio.o -o skipgraph -pthread -lrt -g

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
