// FUNCIONALIDAD DE LA PARTE SERVIDORA
#include <stdio.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/uio.h>
#include <sys/stat.h>
#include <sys/sendfile.h>
#include <pthread.h>
#include <string.h>
#include "ring.h"
#include "common.h"

typedef struct{
    int soc;
    unsigned int ip;
} request_args;



void* request_handler( void* arg){
    request_args *aux_arg= (request_args*) (long) arg;
    int soc= aux_arg->soc;
    unsigned int ip=aux_arg->ip;
    free(arg);
    char aux;
    if(recv(soc, &aux,sizeof(char),MSG_WAITALL)<0){
        perror("error en recv en request handler"); close(soc); return NULL;
    }
    switch(aux){
        case 'P': 
            int res= htonl(getpid());
            if(send(soc, &res,sizeof(int),0)<0){
                perror("error en send case P request handler"); close(soc); return NULL;
            }
            break;
        case 'N': 
            unsigned short new_node;
            unsigned int old_ip;
            unsigned short old_port;
            if(recv(soc, &new_node, sizeof(unsigned short),MSG_WAITALL)<0){
                perror("ERROR en recv nodo case N"); close(soc); return NULL;
            }
            get_successor(&old_ip, &old_port);
            set_successor(ip, new_node);
            if(0>send(soc, &old_ip, sizeof(unsigned int), MSG_MORE)){
                perror("error en send soc case N request handler"); close(soc); return NULL;
            }
            if(0>send(soc, &old_port, sizeof(unsigned short),0)){
                perror("error en send ip case N request handlder"); close(soc); return NULL;
            }
            break;
        case 'R': {
            unsigned short res_port;
            unsigned int res_ip;
            get_successor(&res_ip, &res_port);
            if(send(soc, &res_ip, sizeof(unsigned int), MSG_MORE)<0){
                perror("error en send ip case R req handler"); close(soc); return NULL;
            }
            if(send(soc, &res_port, sizeof(unsigned short),0)<0){
                perror("error send port case R req handler"); close(soc); return NULL;
            }
            break;  }
        case 'U':{
            unsigned short first_port, res_port;
            unsigned int first_ip, res_ip;
            get_successor(&first_ip, &first_port);
            ring_remote_successor(first_ip, first_port, &res_ip, &res_port);
            if(send(soc, &res_ip, sizeof(unsigned int), MSG_MORE)<0){
                perror("error en send ip case U req handler"); close(soc); return NULL;
            }
            if(send(soc, &res_port, sizeof(unsigned short),0)<0){
                perror("error send port case U req handler"); close(soc); return NULL;
            }
            break;  }
        case 'D' : {
            int len_sh_dir, fd, f_tam, tam_err=-1;
            struct stat st;
            if(recv(soc, &len_sh_dir, sizeof(int), MSG_WAITALL)<0){
                perror("error en recv len_sh_dir case D req handler"); close(soc); return NULL;
            }
            char sh_dir[len_sh_dir+1];
            if(recv(soc, sh_dir, len_sh_dir, MSG_WAITALL)<0){
                perror("error en recv filename case D req handler"); close(soc); return NULL;
            }
            sh_dir[strlen(sh_dir)]='\0';

            char dir[2+strlen(get_sh_dir())+len_sh_dir];
            snprintf(dir,sizeof(dir),"%s/%s", get_sh_dir(), sh_dir);
            if((fd=open(dir,O_RDONLY))<0){
                perror("open case D request handler"); 
                if(send(soc,&tam_err,sizeof(int),0)<0){
                    perror("error send f_tam -1"); close(soc); return NULL;
                }
                break;
            }
            if(fstat(fd, &st)<0){
                perror("error fstat"); close(soc); return NULL;
            }
            f_tam=st.st_size;
            if(send(soc,&f_tam,sizeof(int),MSG_MORE)<0){
                    perror("error send f_tam "); close(soc); return NULL;
            }
            sendfile(soc, fd, NULL, f_tam);
            close(fd);
            break;
        }
        case 'L' : {
            unsigned int ip;
            unsigned short port;
            int hops,len_sh_dir,err;
            if(recv(soc,&hops,sizeof(unsigned int),MSG_WAITALL)<0){
                perror("error recv hops case L"); close(soc); return NULL;
            }
            if(recv(soc, &len_sh_dir, sizeof(int), MSG_WAITALL)<0){
                perror("error en recv len_sh_dir case D req handler"); close(soc); return NULL;
            }
            char sh_dir[len_sh_dir+1];
            if(recv(soc, sh_dir, len_sh_dir, MSG_WAITALL)<0){
                perror("error en recv filename case D req handler"); close(soc); return NULL;
            }
            sh_dir[len_sh_dir]='\0';

            get_successor(&ip, &port);
            err=ring_lookup(sh_dir,hops, &ip, &port);
            if(err==0){
            if(send(soc, &ip, sizeof(unsigned int), MSG_MORE)<0){
                perror("error en send ip case U req handler"); close(soc); return NULL;
            }
            if(send(soc, &port, sizeof(unsigned short),0)<0){
                perror("error send port case U req handler"); close(soc); return NULL;
            } 
            } else{
                ip=0;
                if(send(soc, &ip, sizeof(unsigned int),MSG_MORE)<0){
                perror("error send err ip case U req handler"); close(soc); return NULL;
                } 
                if(send(soc, &port, sizeof(unsigned short),0)<0){
                perror("error send port case U req handler"); close(soc); return NULL;
                } 
            }
            break;
        }
        
    }
    close(soc);
    return NULL;
}



// función para el thread que implementa la funcionalidad de servidor
// debe recibir como argumento el socket de servicio
void *server_thread(void *soc_in){
    int soc = (int)(long) soc_in;
    unsigned int addr_size;
    struct sockaddr_in clnt_addr;
    while(1){
        request_args *s_con=malloc(sizeof(request_args));
        addr_size=sizeof(clnt_addr);
        if((s_con->soc=accept(soc,(struct sockaddr *)&clnt_addr, &addr_size))<0){
            perror("Error en el accept"); 
            close(soc); 
            return NULL;
        }
        printf("Conectado el cliente con ip: %s y puerto %u\n", 
                inet_ntoa(clnt_addr.sin_addr), ntohs(clnt_addr.sin_port));
        s_con->ip=clnt_addr.sin_addr.s_addr;
        create_thread(request_handler,(void*) s_con);
    }
    close(soc);
    return NULL;
}





    
