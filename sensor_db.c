#define _GNU_SOURCE

#include "sensor_db.h"
#include <string.h>
#include <stdio.h>
#include "sbuffer.h"
#include "config.h"

extern sbuffer_t *sbuffer;
extern int connection_end;
extern char* log_message;
extern void fifo_log(char* log);



DBCONN *init_connection(char clear_up_flag)
{
    sqlite3 *db;
    char *err_msg = 0; 
    int rc = sqlite3_open(TO_STRING(DB_NAME), &db);

    if (rc != SQLITE_OK) 
    {
        fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(db));
        sqlite3_close(db);
        return NULL;
    }
    if(clear_up_flag == 1)
    {
        char *sql =  "DROP TABLE IF EXISTS "TO_STRING(TABLE_NAME)";"
                     "CREATE TABLE "TO_STRING(TABLE_NAME)"(id INTEGER PRIMARY KEY AUTOINCREMENT, sensor_id INTEGER, sensor_value DECIMAL(4,2), timestamp TIMESTAMP);";
        rc = sqlite3_exec(db, sql, 0, 0, &err_msg);
        if (rc != SQLITE_OK ) 
        {
            asprintf(&log_message, "Unable to create "TO_STRING(DB_NAME)".\n");
            fifo_log(log_message);
            fprintf(stderr, "SQL error: %s\n", err_msg);
            sqlite3_free(err_msg);        
            sqlite3_close(db);
            return NULL;
        } 
        asprintf(&log_message, "New table "TO_STRING(TABLE_NAME)" created.\n");
        fifo_log(log_message);
    }
    
    return db;
}

void disconnect(DBCONN *conn)
{
    sqlite3_close(conn);
}

int insert_sensor(DBCONN *conn, sensor_id_t id, sensor_value_t value, sensor_ts_t ts)
{
    char *sql;
    char *err_msg = 0;
    asprintf(&sql, "INSERT INTO "TO_STRING(TABLE_NAME)"(sensor_id, sensor_value, timestamp) VALUES(%hu, %f, %ld);", id, value, ts);
    int rc = sqlite3_exec(conn, sql, 0, 0, &err_msg);
    if (rc != SQLITE_OK ) 
    {
        
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);        
        sqlite3_close(conn);
        return -1;
    }
    
    free(sql);
    return 0;
}

int insert_sensor_from_file(DBCONN *conn, FILE *sensor_data)
{
    
    sensor_data_t* data = malloc(sizeof(sensor_data_t));
    int success=0;
    while(!feof(sensor_data)){
        fread(&(data->id),sizeof(sensor_id_t),1,sensor_data);
        fread(&(data->value),sizeof(sensor_value_t),1,sensor_data);
        fread(&(data->ts),sizeof(sensor_ts_t),1,sensor_data);
        success = insert_sensor(conn,data->id,data->value,data->ts);
        if (success != 0)
        {
            free(data);
            return -1;
        }
    }
    free(data);
    return 0;
}

int find_sensor_all(DBCONN *conn, callback_t f)
{
    char *err_msg = 0;
    char *sql =  "SELECT * FROM "TO_STRING(TABLE_NAME)";";
    int rc = sqlite3_exec(conn, sql, f, 0, &err_msg);
    if (rc != SQLITE_OK ) 
    {
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);        
        sqlite3_close(conn);
        return -1;
    }    
    return 0;
}

int find_sensor_by_value(DBCONN *conn, sensor_value_t value, callback_t f)
{
    char *err_msg = 0;
    char *sql;
    asprintf(&sql, "SELECT * FROM "TO_STRING(TABLE_NAME)" WHERE sensor_value = %lf;", value);
    int rc = sqlite3_exec(conn, sql, f, 0, &err_msg);
    if (rc != SQLITE_OK ) 
    {
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);        
        sqlite3_close(conn);
        return -1;
    }  
    free(sql);  
    return 0;
}

int find_sensor_exceed_value(DBCONN *conn, sensor_value_t value, callback_t f)
{
    char *err_msg = 0;
    char *sql;
    asprintf(&sql, "SELECT * FROM "TO_STRING(TABLE_NAME)" WHERE sensor_value > %lf;", value);
    int rc = sqlite3_exec(conn, sql, f, 0, &err_msg);
    if (rc != SQLITE_OK ) 
    {
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);        
        sqlite3_close(conn);
        return -1;
    }  
    free(sql);
    return 0;
}


int find_sensor_by_timestamp(DBCONN *conn, sensor_ts_t ts, callback_t f)
{
    char *err_msg = 0;
    char *sql;
    asprintf(&sql, "SELECT * FROM "TO_STRING(TABLE_NAME)" WHERE timestamp = %ld;", ts);
    int rc = sqlite3_exec(conn, sql, f, 0, &err_msg);
    if (rc != SQLITE_OK ) 
    {
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);        
        sqlite3_close(conn);
        return -1;
    }
    free(sql);
    return 0;
}


int find_sensor_after_timestamp(DBCONN *conn, sensor_ts_t ts, callback_t f)
{
    char *err_msg = 0;
    char *sql;
    asprintf(&sql, "SELECT * FROM "TO_STRING(TABLE_NAME)" WHERE timestamp > %ld;", ts);
    int rc = sqlite3_exec(conn, sql, f, 0, &err_msg);
    if (rc != SQLITE_OK ) 
    {
        fprintf(stderr, "SQL error: %s\n", err_msg);
        sqlite3_free(err_msg);        
        sqlite3_close(conn);
        return -1;
    }    
    free(sql);
    return 0;
}


