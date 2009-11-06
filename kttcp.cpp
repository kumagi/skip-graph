c#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

#include <sys/time.h>//gettimeofday

#include <arpa/inet.h>

#include <assert.h>//assert
#include "mulio.h"
#include "mytcplib.h"

enum mode{
	M_SEND,
	M_RECV
};
struct settings{
	int targetip;
	unsigned int verbose;
	unsigned int threads;
	unsigned int testlength;
	unsigned short useport;
	enum mode mode;
}settings;

void print_usage(void);
void* send_method(void* dummy);
double gettimeofday_sec();
void settings_init();

int workthread(int socket){
	char buff[1025];
	int length;
	int totallength=0;
	do{
		length = read(socket,buff,1024);
		totallength+=length;
	}while(length!=0);
	printf("received:%dBytes\n",totallength);
	
	return 1;
}

int main(int argc,char** argv){
	int c;
	pthread_t* workers;
	
	//set default
	settings_init();
	
	while ((c = getopt(argc, argv, "s:t:rvp:l:h")) != -1) {
        switch (c) {
        case 's'://send mode
            settings.mode = M_SEND;
			struct in_addr tmp_inaddr;
			if(inet_aton(optarg,&tmp_inaddr)){
				settings.targetip = tmp_inaddr.s_addr;
			}else {
				printf("aton:address invalid\n");
			}
			break;
		case 't'://number of threads
			settings.threads = atoi(optarg);
			break;
		case 'r':
			settings.mode = M_RECV;
			break;
        case 'v':
            settings.verbose++;
            break;
		case 'p':
			settings.useport = atoi(optarg);
			break;
		case 'l':
			settings.testlength = atoi(optarg);
			break;
		case 'h':
			print_usage();
			exit(0);
			break;
		}
	}
	workers = (pthread_t*)malloc(sizeof(pthread_t) * settings.threads);
	
	if(settings.mode == M_SEND){
		struct in_addr tmp_inaddr;
		tmp_inaddr.s_addr=settings.targetip;
		printf("send to %s:%d\n%d bytes of data\n%d threads\n",inet_ntoa(tmp_inaddr),settings.useport,settings.testlength,settings.threads);
		
		double start = gettimeofday_sec();
		for(unsigned int i=0; i<settings.threads; i++){
			pthread_create(&workers[i],0,send_method,NULL);
		}
		for(unsigned int i=0; i<settings.threads; i++){
			pthread_join(workers[i],0);
		}
		int senddata = settings.testlength * settings.threads;
		double time = gettimeofday_sec() - start;
		double speed = senddata/time;
		printf("\nsend :%d Bytes\ntime :%lf secs\n",senddata,time);
		if(speed > 1000000000){
			printf("speed :%.3lf GB/sec\n",speed/1000000000);
		}else if(speed > 1000000){
			printf("speed :%.3lf MB/sec\n",speed/1000000);
		}else if(speed > 1000){
			printf("speed :%.3lf KB/sec\n",speed/1000);
		}else{
			printf("speed :%.3lf Byte/sec\n",speed);
		}
		unsigned int bits = (unsigned int)speed*8;
		if(bits > 1000*1000*1000 ){
			printf("%d,%03d,%03d,%03d bps\n",bits/1000000000,(bits/(1000000))%1000,(bits/(1000))%1000,bits%1000);
		}else if(bits > 1000*1000 ){
			printf("%d,%03d,%03d bps\n",bits/1000000,(bits/(1000))%1000,bits%1000);
		}else if(bits > 1000){
			printf("%d,%03d bps\n",bits/1000,bits%1000);
		}else {
			printf("%d\n bps",bits);
		}
	}else if(settings.mode == M_RECV){
		class mulio mulio;
		int listening = create_tcpsocket();
		set_reuse(listening);
		bind_inaddr_any(listening,settings.useport);
		listen(listening,2048);
	
		mulio.SetAcceptSocket(listening);
		mulio.SetCallback(workthread);
		mulio.run();
	
		printf("port:%d\n",settings.useport);
		pthread_attr_t thread_attr;
		pthread_attr_setdetachstate(&thread_attr, 1); 
		
		for(unsigned int i=0; i<settings.threads-1; i++){
			pthread_create(&workers[i],&thread_attr,send_method,&mulio);
		}
		mulio.eventloop();
	}
	return 0;
}

void print_usage(void){
	printf("Kttcp:Kuma Test TCP\n");
	printf("usage : -s xxx.xxx.xxx.xxx \n");
	printf("           Set the target IP address\n");
	printf("      : -r \n");
	printf("           Receive mode\n");
	printf("      : -t x\n");
	printf("           Set the number of threads\n");
	printf("      : -p x\n");
	printf("           Set the target/receive port\n");
	printf("      : -l x\n");
	printf("           Set the message's length\n");
	printf("      : -v\n");
	printf("           Set the verbose mode\n");
	printf("      : -h\n");
	printf("           Print this message\n");
}

void* send_method(void* dummy)
{
	char* buffer;
	buffer = (char*)malloc(1024);
	
	int conn = create_tcpsocket();
	set_linger(conn);
	socket_maximize_sndbuf(conn);
		
	connect_port_ip(conn,settings.targetip,settings.useport);
	
	for(unsigned int i=0; i<settings.testlength/1024; i++){
		write(conn,buffer,1024);
	}
	if(settings.testlength&1023){
		write(conn,buffer,settings.testlength&1023);
	}
	close(conn);
	return NULL;
}
void* recv_method(void* Hmulio)
{
	class mulio* mulio = (class mulio*)Hmulio;
	mulio->eventloop();
	return NULL;
}
double gettimeofday_sec()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + (double)tv.tv_usec*1e-6;
}
void settings_init(){
	settings.mode = M_RECV;
	settings.targetip = htonl(INADDR_ANY);
	settings.useport = 10004;
	settings.testlength = 2048;
	settings.threads = 1;
}
