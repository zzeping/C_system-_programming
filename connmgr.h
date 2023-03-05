#ifndef _CONNMGR_H_
#define _CONNMGR_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include "lib/tcpsock.h"
#include "lib/dplist.h"
#include "config.h"
#ifndef TIMEOUT
#define TIMEOUT 5
#endif
#define max(x,y) ((x) > (y) ? (x) : (y))

struct tcp_connection{
    sensor_data_t sensor_data;
    tcpsock_t* socket_information;
    time_t last_update_ts;
};
typedef struct tcp_connection tcp_connection_t;

void connmgr_listen(int port);
void connmgr_free(void);
void * element_copy(void * element);
void element_free(void ** element);
int element_compare(void * x, void * y);
void read_data(tcp_connection_t * connection,int m);
void remove_timeout_connections (void);
void update_timeout_value (void);
void print_all(void);


#endif  //CONNMGR_H_