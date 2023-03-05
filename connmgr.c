#define _GNU_SOURCE
#include <sys/select.h>
#include <errno.h>
#include "connmgr.h"
#include "sbuffer.h"

//********Global variables********
tcp_connection_t *server=NULL;
int server_sd;
fd_set sd_set_origin, sd_set_motified;  /* origin: The socket descriptors to be watched to see if they are ready for reading
                                         * motified: The socket descriptors that are ready for reading*/
struct timeval timeout; 
int sd_amount; 
int max_sd; 
dplist_t* connection_list=NULL;
bool last_removed_flag;
FILE *file;
extern char* log_message;

extern sbuffer_t *sbuffer;
extern int connection_end;
extern pthread_cond_t cond1;
extern pthread_cond_t cond_db;
extern void fifo_log(char* log);
extern pthread_rwlock_t *flag_lock;

// *********Functions*******
void * element_copy_c(void * element)
{
    tcp_connection_t *tcp_node = NULL;
    tcp_node = malloc(sizeof(tcp_connection_t));
    *tcp_node = *(tcp_connection_t *)element;
    return (void *) tcp_node;
}

void element_free_c(void ** element)
{
    tcp_connection_t *tcp_node = (tcp_connection_t*) *element;
    tcp_close(&(tcp_node->socket_information));
    free(*element);
    *element = NULL;
}

int element_compare_c(void * x, void * y)
{
    return ((((tcp_connection_t*)x)->socket_information->sd < ((tcp_connection_t*)y)->socket_information->sd) ? -1 : (((tcp_connection_t*)x)->socket_information->sd == ((tcp_connection_t*)y)->socket_information->sd) ? 0 : 1);
}


void connmgr_listen(int port) {
    /*This project choode to use select() to monitor multiple socket descriptors (incoming data), 
     * waiting until one or more of the socket descriptors become "ready" for data inserting */

    printf("Server(port:%d) is started\n",port);

    file = fopen("sensor_data_recv","w");
    
    //*********Creates a server socket and opens it in 'passive listening mode'
    server = malloc(sizeof(tcp_connection_t));
    if (tcp_passive_open(&(server->socket_information), port) != TCP_NO_ERROR) exit(EXIT_FAILURE); 
    //Return the socket descriptor of sever
    if (tcp_get_sd(server->socket_information,&server_sd) != TCP_NO_ERROR) exit(EXIT_FAILURE);

    
    //*********Initialize connection_list and add sever in it*********
    connection_list = dpl_create(element_copy_c, element_free_c, element_compare_c);
    if(connection_list == NULL) printf("fail to create connection list\n");
    connection_list = dpl_insert_at_index(connection_list, server,0,false); 
    
    
    //*********Initialize select() parameters*********
    // Watch stdin (server socket ddescriptor) to see when it has input.
    FD_ZERO(&sd_set_origin);  //removes all socket descriptors from set
    FD_SET(server_sd,&sd_set_origin); // adds the server socket descriptor to set.
    max_sd = server_sd;  //  the highest-numbered socket descriptor in the set, currently is the server one
    timeout.tv_sec = TIMEOUT;  /*the interval that select() should block 
                                * waiting for a file descriptor to become ready (second)
                                * wait for the first connction to come for double TIMEOUT value (make it different from the last one is removed)*/
    timeout.tv_usec = 0;  
    last_removed_flag=1;

    // protect flag -- read
    pthread_rwlock_rdlock(flag_lock);
    int connection_end_flag=connection_end;
    pthread_rwlock_unlock(flag_lock);
    while(connection_end_flag==0) {

        sd_set_motified = sd_set_origin;   // the origin set is to save the connections that need to be watched
        sd_amount = select(max_sd+1, &sd_set_motified, NULL, NULL, &timeout); /*return the number of socket descriptors contained 
                                                                * in the motified descriptor set, meanwhile the motified set 
                                                                * has been changed!!   */                                         
        //printf("timeout on: %ld\n", time(NULL));
        //print_all();
        remove_timeout_connections(); // remove the sensor(s) that lost connection(timeout)
        
        // protect flag -- read
        pthread_rwlock_rdlock(flag_lock);
        connection_end_flag=connection_end;
        pthread_rwlock_unlock(flag_lock);

        if(sd_amount <0 )
        {
            perror("select()");
            exit(EXIT_FAILURE);
        }

        if(sd_amount >0 )  // connections come
        {
            // the server sd is in the set -> new connection has been made
            if(FD_ISSET(server_sd,&sd_set_motified)) 
            {
                // get the new connection
                tcp_connection_t *new_connection = malloc(sizeof(tcp_connection_t));
                new_connection->last_update_ts = time(NULL);
                if (tcp_wait_for_connection(server->socket_information, &new_connection->socket_information) != TCP_NO_ERROR) exit(EXIT_FAILURE);
                int new_sd;
                if (tcp_get_sd(new_connection->socket_information,&new_sd) != TCP_NO_ERROR) exit(EXIT_FAILURE);
                dpl_insert_at_index(connection_list, new_connection, dpl_size(connection_list), false); // insert the new connection into the list
                FD_SET(new_sd,&sd_set_origin); // add new sd to the origin sd set 
                max_sd = max(max_sd,new_sd);  // update max sd value
                read_data(new_connection,1);  // print new connection's first data

            }

            // loop all conncetions(except for server) to read new data from previously added sensors
            for(int i=1; i<dpl_size(connection_list); i++) {
                
                tcp_connection_t *dummy = dpl_get_element_at_index(connection_list, i);
                int dummy_sd;
                if (tcp_get_sd(dummy->socket_information,&dummy_sd) != TCP_NO_ERROR) exit(EXIT_FAILURE); 
                if (FD_ISSET(dummy_sd,&sd_set_motified)) // new data of the connection has come
                {
                    dummy->last_update_ts = time(NULL);
                    read_data(dummy,0);
                }
            }
            last_removed_flag = 0; 
        }
        update_timeout_value(); 


        if( last_removed_flag==1 )
        {
            printf("Server timeout\n");
            if (tcp_close(&server->socket_information) != TCP_NO_ERROR) exit(EXIT_FAILURE);
            printf("Server is shutting down\n");
            connmgr_free();
            break;
        }
        
        // when the last connection has been removed -> wait for another TIMEOUT before exit  
        if(sd_amount==0 && dpl_size(connection_list) == 1)  
        {
            asprintf(&log_message,"The last connection has been removed, wait for another TIMEOUT\n");
            fifo_log(log_message);
            last_removed_flag = 1;
            timeout.tv_sec = TIMEOUT;
        }


        //print_all();
    }

    fclose(file);

}

void read_data(tcp_connection_t * connection, int m) 
{
    int bytes, result;
    // read sensor ID
    bytes = sizeof(connection->sensor_data.id);
    result = tcp_receive(connection->socket_information, (void *) &(connection->sensor_data.id), &bytes);
    if(m) 
    {
        asprintf(&log_message,"A sebsor node with %d has opened a new connection.\n",connection->sensor_data.id);
        fifo_log(log_message);
    }
    // read temperature
    bytes = sizeof(connection->sensor_data.value);
    result = tcp_receive(connection->socket_information, (void *) &(connection->sensor_data.value), &bytes);
    // read timestamp
    bytes = sizeof(connection->sensor_data.ts);
    result = tcp_receive(connection->socket_information, (void *) &(connection->sensor_data.ts), &bytes);
    if ((result == TCP_NO_ERROR) && bytes) {
        printf("sensor id = %" PRIu16 " - temperature = %g - timestamp = %ld (connection size: %d)\n", 
            connection->sensor_data.id, connection->sensor_data.value, (long int) connection->sensor_data.ts, dpl_size(connection_list)-1);
        fprintf(file, "%d %f %ld\n", connection->sensor_data.id, connection->sensor_data.value, (long int) connection->sensor_data.ts);
        if (sbuffer_insert(sbuffer,&(connection->sensor_data)) != SBUFFER_SUCCESS) exit(EXIT_FAILURE);
        }
        if(datamgr_unread_amount(sbuffer)>0) pthread_cond_signal(&cond1);
        if(sensor_db_unread_amount(sbuffer)>0) pthread_cond_signal(&cond_db);


}

void remove_timeout_connections (void)
{
    // loop all connections to check if timeout
    for(int i=1; i < dpl_size(connection_list); i++)
    {
        tcp_connection_t *dummy = dpl_get_element_at_index(connection_list, i);
        //printf("Sensor(id:%d) last income time = %ld\n", dummy->sensor_data.id, dummy->last_update_ts);
        if( time(NULL)- dummy->sensor_data.ts >= TIMEOUT)
        {
            asprintf(&log_message,"The sensor node with %d has closed the connection.\n", dummy->sensor_data.id);
            fifo_log(log_message);
            int dummy_sd;
            if (tcp_get_sd(dummy->socket_information,&dummy_sd) != TCP_NO_ERROR) exit(EXIT_FAILURE); 
            FD_CLR(dummy_sd , &sd_set_origin);
            tcp_close(&(dummy->socket_information));
            dpl_remove_at_index(connection_list, i, true);
        }
    }
}

void update_timeout_value (void)
{
    // find the connection with smallest timeout value
    // select() will not block could happen because of one connection timeout or server timeout
    long earliest_timeout = time(NULL);
    for(int i=1; i < dpl_size(connection_list); i++)  
    {
        tcp_connection_t* dummy = dpl_get_element_at_index(connection_list, i); 
        if(dummy->last_update_ts <= earliest_timeout )
        {   
            earliest_timeout = dummy->last_update_ts;
        }
    }
    timeout.tv_sec = TIMEOUT - (time(NULL) - earliest_timeout);

    
}

void connmgr_free()
{
    dpl_free(&connection_list, true);
    free(connection_list);
}

void print_all(void)
{
    for(int i=0; i < dpl_size(connection_list); i++)
    {
        tcp_connection_t *dummy = dpl_get_element_at_index(connection_list, i);
        {
            printf("id: %"PRIu16", last update: %ld, value:%g, sd:%d, timeout: %ld, sd_amount: %d , on: %ld\n",dummy->sensor_data.id,dummy->last_update_ts,dummy->sensor_data.value, dummy->socket_information->sd, timeout.tv_sec, sd_amount, time(NULL));
        }
    }
}