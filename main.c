#define _GNU_SOURCE

/**
 * \author Zeping Zhang
 */
#include "config.h"
#include "connmgr.h"
#include <pthread.h>
#include "sbuffer.h"
#include "datamgr.h"
#include "sensor_db.h"
#define FIFO_NAME 	"FIFOlog" 
#define MAX     80
#include "errmacros.h"

//********Global variables********
int server_port;
pthread_t connmgr_thread, datamgr_thread, sensor_db_thread;
sbuffer_t *sbuffer;
int connection_end;
int datamgr_read_amount=0;
int db_read_amount=0;

pthread_mutex_t datamgr_lock = PTHREAD_MUTEX_INITIALIZER; /*mutex to use with condition 
                                                    variable to sleep datamgr thread */
pthread_mutex_t sensor_db_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond1 = PTHREAD_COND_INITIALIZER;
pthread_cond_t cond_db = PTHREAD_COND_INITIALIZER;
pthread_rwlock_t *flag_lock;
FILE *fifo_write;
FILE *fifo_read; 
FILE* write_file;
char recv_buf[MAX];
char *log_message;

//********Functions********
void* connmgr_main(void* port);
void* datamgr_main();
void* sensor_db_main();
void reconnect_to_db(DBCONN *conn);
int callback(void *NotUsed, int argc, char **argv, char **azColName);
void fifo_log(char* log);

//********Main process********
int main(int argc, char *argv[]) {
    
    if (argc != 2) {
        printf("Fail to set server_port!");
        exit(EXIT_SUCCESS);
    } else {
        // user input validation
        server_port = atoi(argv[1]);
    }

    pid_t pid = fork();

    if(pid < 0)
    { // error
       perror("Fork failed");
    }

    if (pid == 0)
    { // child process
    write_file = fopen("gateway.log","w");
    int result;
    char *str_result;

    result = mkfifo(FIFO_NAME, 0666);
    CHECK_MKFIFO(result); 
    fifo_read = fopen(FIFO_NAME, "r"); 
    do {
        str_result = fgets(recv_buf, MAX, fifo_read);
        if ( str_result != NULL )
        { 
        fprintf(write_file, "%s",recv_buf);
        }
    } while ( str_result != NULL ); 

    fclose(write_file);
    result = fclose(fifo_read);
    FILE_CLOSE_ERROR(result);

    }

    else
    {  // main process 
    sbuffer_init(&sbuffer);
    connection_end=0;

    int result = mkfifo(FIFO_NAME, 0666);
    CHECK_MKFIFO(result); 
    fifo_write = fopen(FIFO_NAME, "w"); 
    FILE_OPEN_ERROR(fifo_write); 

    flag_lock = malloc(sizeof(pthread_rwlock_t)); // initialize the rwlock
    pthread_rwlock_init(flag_lock,NULL);



    if(pthread_create(&connmgr_thread,NULL,connmgr_main,&server_port) != 0) exit(EXIT_FAILURE);
    if(pthread_create(&datamgr_thread,NULL,datamgr_main,NULL) != 0) exit(EXIT_FAILURE);
    if(pthread_create(&sensor_db_thread,NULL,sensor_db_main,NULL) != 0) exit(EXIT_FAILURE);

    pthread_join(connmgr_thread,NULL);
    pthread_join(datamgr_thread,NULL);
    pthread_join(sensor_db_thread,NULL);

    
    pthread_cond_destroy(&cond1);
    pthread_cond_destroy(&cond_db);
    pthread_mutex_destroy(&datamgr_lock);
    pthread_mutex_destroy(&sensor_db_lock);
    pthread_rwlock_destroy(flag_lock);
    free(flag_lock);

    result = fclose(fifo_write);
    FILE_CLOSE_ERROR(result);

    sbuffer_free(&sbuffer);

    }
    
    wait(NULL); //wait untill child completes

    return 0;
}


void* connmgr_main(void* port)
{
    int port_i = *(int*)port;
    connmgr_listen(port_i);
    printf("Connection manager ended\n");
    // protect flag -- write
    pthread_rwlock_wrlock(flag_lock);
    connection_end = 1;
    pthread_rwlock_unlock(flag_lock);

    pthread_cond_signal(&cond1);
    pthread_cond_signal(&cond_db);
    connmgr_free();
    return NULL;
}

void* datamgr_main()
{
    FILE* snsr_ptr = fopen("room_sensor.map", "r");
    parse_sensor_map(snsr_ptr);

    // protect flag -- read
    pthread_rwlock_rdlock(flag_lock);
    int connection_end_flag=connection_end;
    pthread_rwlock_unlock(flag_lock);

    while (connection_end_flag==0 || datamgr_unread_amount(sbuffer)>0)
    {
        pthread_mutex_lock(&datamgr_lock);
        if (datamgr_unread_amount(sbuffer)==0)
        {
            pthread_cond_wait(&cond1, &datamgr_lock);
        }
        pthread_mutex_unlock(&datamgr_lock);
        // protect flag -- read
        pthread_rwlock_rdlock(flag_lock);
        connection_end_flag=connection_end;
        pthread_rwlock_unlock(flag_lock);

        if(datamgr_unread_amount(sbuffer)>0) datamgr_parse_sensor_buffer();


        
    }
    printf("Data manager ended\n");
    datamgr_free();
    fclose(snsr_ptr);
    return NULL;
}

void* sensor_db_main()
{
    db_read_amount=0;
    DBCONN *conn = init_connection(1);
    if(conn==NULL) reconnect_to_db(conn);
    else{
        asprintf(&log_message,"Connection to SQL server established\n");
        fifo_log(log_message);
    }

    // protect flag -- read
    pthread_rwlock_rdlock(flag_lock);
    int connection_end_flag=connection_end;
    pthread_rwlock_unlock(flag_lock);

    while (connection_end_flag==0 || datamgr_unread_amount(sbuffer)>0)
    {
        pthread_mutex_lock(&sensor_db_lock);
        if (sensor_db_unread_amount(sbuffer)==0)
        {
            pthread_cond_wait(&cond_db, &sensor_db_lock);
        }
        pthread_mutex_unlock(&sensor_db_lock);

        // protect flag -- read
        pthread_rwlock_rdlock(flag_lock);
        connection_end_flag=connection_end;
        pthread_rwlock_unlock(flag_lock);

        if(conn==NULL) 
        {
            asprintf(&log_message,"Connection to SQL server lost.\n");
            fifo_log(log_message);
            reconnect_to_db(conn);
        }
        if(sensor_db_unread_amount(sbuffer)>0)
        {
            db_read_amount++;
            sensor_data_t *data = malloc(sizeof(sensor_data_t));
            if(sensor_db_first_to_read(sbuffer,data)!=1) printf("data manager read fail\n");
            insert_sensor(conn,data->id,data->value,data->ts);
            free(data);
        } 
    }

    printf("Database manager ended\n");
    find_sensor_all(conn,callback);
    disconnect(conn);
    return NULL;
}

void reconnect_to_db(DBCONN *conn)
{
    sleep(5);
    for(int i=0; i<2;i++)
    {
        conn =  init_connection(1);
        if(conn!=NULL) {
            asprintf(&log_message,"Connection to SQL server established\n");
            fifo_log(log_message);
            return;
        }
        sleep(5);
    }
    if(conn==NULL)
    {
        asprintf(&log_message,"Unable to connect to SQL server\n");
        fifo_log(log_message);
        connection_end = 1;
    }
}

int callback(void *NotUsed, int argc, char **argv, char **azColName) {
    NotUsed = 0;
    for (int i = 0; i < argc; i++) {
        printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
    }
    printf("\n");
    return 0;
}

void fifo_log(char* log)
{
	char *send_buf; 
    static int n = 0; 
	int order =  __sync_add_and_fetch(&n, 1);
	time_t ts=time(NULL);
	
	asprintf(&send_buf, "%d %ld %s", order, ts, log );
	if (fputs(send_buf, fifo_write) == EOF)
    {
      	fprintf( stderr, "Error writing data to fifo.\n");
     	exit( EXIT_FAILURE );
    } 
	FFLUSH_ERROR(fflush(fifo_write));
	free(send_buf); 
    free(log);
}