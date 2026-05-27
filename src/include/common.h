// DECLARACIÓN DE LA FUNCIONALIDAD COMÚN PARA LA PARTE CLIENTE
// Y LA PARTE SERVIDORA
#ifndef _COMMON_H
#define _COMMON_H        1

// thread de servicio
void *server_thread(void *arg);

// crea un thread detached
int create_thread(void *(*func)(void *), void *arg);

// Inicializa el socket, le asigna un puerto seleccionado por el SO
// y lo prepara para aceptar conexiones. Recibe un parámetro de salida
// donde devuelve el puerto asignado.
int create_socket_srv(unsigned short *port);

// crea socket y se conecta al servidor a partir de IP y puerto en formato red
int create_socket_cln(unsigned int ip, unsigned short port);

//setteo un sucesor (inicio)
void set_successor(unsigned int ip, unsigned short port);
//devuelve el sucesor del nodo remoto
void get_successor(unsigned int *ip, unsigned short *port);

//pone la nueva direccion compartida del ficher
void set_sh_dir (const char* sh_dir);

//devuelve la direccion compartida del socket
const char*  get_sh_dir ();
#endif // _COMMON_H
