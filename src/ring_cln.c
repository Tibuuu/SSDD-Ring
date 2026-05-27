// FUNCIONALIDAD DE LA PARTE CLIENTE
#include <sys/mman.h>
#include <stdio.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/uio.h>
#include <sys/stat.h>

#include "ring.h"
#include "common.h"

static int is_initialized(void);
static int initialize(void);

static unsigned int self_ip;
//static unsigned int out_ip;
//static unsigned short out_port;
static const char* f_dir; 
static unsigned short self_port;


// inicia el nodo añadiéndolo a la red P2P si ya está creada;
// los puertos e IPs deben estar en formato de red;
// debe devolver en el último parámetro el puerto reservado en formato red;
// retorna 0 si OK y -1 si error
int ring_init(const char *shrd_dir, unsigned int local_ip, unsigned int remote_ip, unsigned short remote_port, unsigned short *alloc_port) {
    if (initialize()) return -1; // ya está inicializada
    int srv_socket;
    
    self_ip=local_ip;
    f_dir=shrd_dir;
    set_sh_dir(f_dir);
        
    if(0>(srv_socket=create_socket_srv(&self_port))){ //compruebo que no es menor de 0 el retorno 
        return -1;
    }
    *alloc_port=self_port;
    create_thread(server_thread,(void*)(long) srv_socket); //casteo de in a long y luego a void para evitar el warning de diff size
    
    if(remote_ip!=0){
    int con;
    if((con=create_socket_cln(remote_ip, remote_port))<0){
        perror("error cln socket init"); close(con); return -1;
    } 
    
    char buf='N';
        //pid_t res;
    if(send(con,&buf,sizeof(char),MSG_MORE)<0){
        perror("error en send for new node"); close(con); return -1;    
    }
    if(send(con, &self_port, sizeof(unsigned short), 0)<0){
        perror("error en send for new node"); close(con); return -1; 
    }
    unsigned int aux_ip;
    if(recv(con, &aux_ip, sizeof( unsigned int ),MSG_WAITALL)<0){
        perror("error recv init ip"); close(con); return -1;
    }
    unsigned short aux_port;
    if(recv(con, &aux_port, sizeof( unsigned short), MSG_WAITALL)<0){
        perror("error recv init port"); close(con); return -1;
    }
    set_successor(aux_ip, aux_port);
    close(con);
    } else{
        set_successor(self_ip,self_port);
    }
    
    return 0;
}
// función local que devuelve la IP y el puerto del nodo;
// retorna 0 si OK y -1 si error
int ring_self(unsigned int *ip, unsigned short *port) {
    if (!is_initialized()) return -1; // no está inicializada
    *ip=self_ip;
    *port=self_port;
    //printf("self_port: %d\n", ntohs(self_port));
    return 0;
}
// devuelve el PID del nodo remoto especificado o -1 si error
int ring_remote_pid(unsigned int remote_ip, unsigned short remote_port) {
    if (!is_initialized()) return -1; // no está inicializada
    int con;
    if((con=create_socket_cln(remote_ip, remote_port))<0) return -1;
    char buf='P';
    pid_t res;
    if(send(con,&buf,sizeof(char),0)<0){
        perror("error en send"); close(con); return -1;    
    }

    if(recv(con, &res,sizeof(pid_t), MSG_WAITALL)<0){
        perror("error en recv"); close(con); return -1;
    }
    close(con);
    res= ntohl(res);
    return res;
}
// función local que devuelve la IP y el puerto del nodo sucesor;
// retorna 0 si OK y -1 si error
int ring_successor(unsigned int *ip, unsigned short *port) {
    if (!is_initialized()) return -1; // no está inicializada
    
    get_successor(ip, port);
    return 0;
}
// devuelve la IP y el puerto del nodo sucesor del especificado;
// retorna 0 si OK y -1 si error
int ring_remote_successor(unsigned int remote_ip, unsigned short remote_port, unsigned int *suc_ip, unsigned short *suc_port) {
    if (!is_initialized()) return -1; // no está inicializada
    int con;
    char buf= 'R';
    unsigned int buf2;
    unsigned short buf3;
    if((con=create_socket_cln(remote_ip, remote_port))<0){return -1;}
    if(send(con,&buf, sizeof(char),0)<0){
        perror("error send char R"); close(con); return -1;
    }
    if(recv(con, &buf2, sizeof(unsigned int),MSG_WAITALL)<0){
        perror("error recv R, ip"); close(con); return -1;
    }
    if(recv(con, &buf3, sizeof(unsigned short),MSG_WAITALL)<0){
        perror("error recv R, node"); close(con); return -1;
    }
    *suc_port=buf3;
    *suc_ip=buf2;
    close(con);
    return 0;
}
// devuelve la IP y el puerto del nodo sucesor del sucesor del especificado;
// retorna 0 si OK y -1 si error
int ring_remote_successor_successor(unsigned int remote_ip, unsigned short remote_port, unsigned int *suc_suc_ip, unsigned short *suc_suc_port) {
    if (!is_initialized()) return -1; // no está inicializada
    int con;
    char buf= 'U';
    unsigned int buf2;
    unsigned short buf3;
    if((con=create_socket_cln(remote_ip, remote_port))<0){return -1;}
    if(send(con,&buf, sizeof(char),0)<0){
        perror("error send char R"); close(con); return -1;
    }
    if(recv(con, &buf2, sizeof(unsigned int),MSG_WAITALL)<0){
        perror("error recv R, ip"); close(con); return -1;
    }
    if(recv(con, &buf3, sizeof(unsigned short),MSG_WAITALL)<0){
        perror("error recv R, node"); close(con); return -1;
    }
    *suc_suc_port=buf3;
    *suc_suc_ip=buf2;
    return 0;
}
// descarga el fichero del nodo especificado;
// retorna el tamaño del fichero si OK y -1 en caso de error
int ring_download(unsigned int remote_ip, unsigned short remote_port, const char *filename) {
    if (!is_initialized()) return -1; // no está inicializada
    int con, fd, f_tam;
    char dir[strlen(get_sh_dir())+strlen(filename)+2];
    char buf= 'D';
    int len_sh_dir= strlen(filename);
    const char* buf2= filename;
    char* map_aux;
    snprintf(dir, sizeof(dir),"%s/%s", get_sh_dir(), filename);

    if((con=create_socket_cln(remote_ip, remote_port))<0){
        perror("error socket download"); close(con); return -1;
    }
    if((send(con, &buf, sizeof(char), MSG_MORE))<0){
        perror("error send D"); close(con); return -1;
    }
    if((send(con, &len_sh_dir, sizeof(int), MSG_MORE))<0){
        perror("error send D"); close(con); return -1;
    }
    if((send(con, buf2, len_sh_dir, 0))<0){
        perror("error send D"); close(con); return -1;
    }
    if((recv(con, &f_tam, sizeof(int),MSG_WAITALL))<0 || f_tam== -1){
        perror("error en recv tam fichero"); close(con); return -1;
    } 
    
    if((fd=open(dir, O_CREAT|O_TRUNC|O_RDWR,0666))<0){
        perror("open dir"); close(con); return -1;
    }
    if(ftruncate(fd, f_tam)){
        perror("truncate"); close(fd); close(con); return -1;
    }
    if((map_aux=mmap(NULL,f_tam, PROT_WRITE,MAP_SHARED, fd,0))==MAP_FAILED){
        perror("mmap failed"); close(con); return -1;
    }
    close(fd); 
    if((recv(con, map_aux, f_tam, MSG_WAITALL))<0){
        perror("error recv map_aux"); close(con); return -1;
    }

    munmap(map_aux, f_tam);
    return 0;
}
// busca el fichero en el anillo dando un número máximo de saltos y devolviendo
// la IP y el puerto del nodo que lo contiene;
// retorna 0 si OK y -1 si error
int ring_lookup(const char *filename, int hops, unsigned int *ip, unsigned short *port) {
    if (!is_initialized()) return -1; // no está inicializada
    int fd, con;
    char buf='L';
    unsigned int res_ip;
    unsigned short res_port;
    int len_sh_dir= strlen(filename);
    char dir[strlen(get_sh_dir())+strlen(filename)+2];
    snprintf(dir, sizeof(dir),"%s/%s", get_sh_dir(), filename);
    if((fd=open(dir,O_RDONLY))>=0){
        close(fd);
        ring_self(ip, port);
        return 0;
    }
    if(hops==0)
        return -1;
    --hops;
    if((con=create_socket_cln(*ip, *port))<0){
        perror("error socket lookup"); close(con); return -1; 
    }
    if(send(con, &buf, sizeof(char),MSG_MORE)<0){
        perror("err send L lookup"); close(con); return -1;
    }
    if(send(con, &hops, sizeof(int),MSG_MORE)<0){
        perror("err send hops lookup"); close(con); return -1;
    }
    if(send(con, &len_sh_dir, sizeof(int),MSG_MORE)<0){
        perror("err send len_dir lookup"); close(con); return -1;
    }
    if(send(con, filename, len_sh_dir,0)<0){
        perror("err send filename lookup"); close(con); return -1;
    }
    if(recv(con, &res_ip, sizeof(unsigned int), MSG_WAITALL)<0){
        perror("err recv res_ip lookup"); close(con); return -1;
    }
    if(recv(con, &res_port, sizeof(unsigned short), MSG_WAITALL)<0){
        perror("err recv res_port lookup"); close(con); return -1;
    }
    if(res_ip==0)
        return -1;
    *ip=res_ip;
    *port=res_port;
    return 0;
}
// busca y descarga el fichero del nodo encontrado en el anillo que lo contiene;
// retorna el tamaño del fichero si OK y -1 en caso de error;
// ESTA FUNCIÓN YA ESTA COMPLETADA
int ring_get_file(const char *filename, int hops) {
    if (!is_initialized()) return -1; // no está inicializada
    unsigned int ip, ip_local;
    unsigned short port, port_local;
    int res = ring_lookup(filename, hops, &ip, &port);
    ring_self(&ip_local, &port_local);
    // realiza la descarga si encontrado en un nodo que no es el local
    if ((res!=-1) && ((ip!=ip_local) || (port!=port_local)))
        res = ring_download(ip, port, filename);
    return res;
}

// funciones auxiliares
static int initialized;

static int initialize(void) {
    return initialized?1:(initialized=1,0);
}
static int is_initialized(void) {
    return initialized;
}
