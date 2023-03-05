#define _GNU_SOURCE
#include "datamgr.h"
#include "lib/dplist.h"
#include <string.h>
#include <pthread.h>
#include <inttypes.h>
#include "sbuffer.h"


extern int datamgr_read_amount;
extern sbuffer_t *sbuffer;
extern pthread_mutex_t datamgr_lock;
extern pthread_cond_t datamgr_cond;
extern char* log_message;
extern void fifo_log(char* log);


typedef struct {
    sensor_id_t sensor_id;
    room_id_t room_id;
    sensor_value_t sensor_data[RUN_AVG_LENGTH];
    sensor_ts_t last_modified;
    unsigned char num_data;
    sensor_value_t data_sum;
    sensor_value_t avg_data;
} element_t;

void * element_copy(void * element);
void element_free(void ** element);
int element_compare(void * x, void * y);

dplist_t *sensor_dplist = NULL;

void parse_sensor_map(FILE *fp_sensor_map)
{
    ERROR_HANDLER(fp_sensor_map == NULL, "Error openning streams - NULL\n");
    sensor_dplist = dpl_create(element_copy, element_free, element_compare);
    char l_length[10];
    unsigned char map_index = 0;
    while(fgets(l_length, sizeof(l_length), fp_sensor_map) != NULL)
    {
        element_t *map = NULL;
        map = calloc(1,sizeof(element_t));
        map->num_data = 0;
        map->data_sum = 0;
        sscanf(l_length, "%hu%hu", &(map->room_id), &(map->sensor_id));
        dpl_insert_at_index(sensor_dplist,map,map_index,false);
        map_index++;
    }
}

void datamgr_parse_sensor_buffer()
{
    datamgr_read_amount++;
    int found = 0;      // indicate if the id of data can be found in the sensor list
    sensor_data_t *data = malloc(sizeof(sensor_data_t));
    if(datamgr_first_to_read(sbuffer,data)!=1) printf("data manager read fail\n");
    element_t *sensor = NULL;
    for(int i=0; i<dpl_size(sensor_dplist); i++)
    {
        sensor = dpl_get_element_at_index(sensor_dplist, i);
        if(sensor->sensor_id == data->id)
        {
            found = 1;
            break;
        } 
    }
    if(found == 1)   // the sensor is inside the sensor list, read in the sensor data.
    {
       sensor->last_modified = data->ts;
        if(sensor->num_data<RUN_AVG_LENGTH-1)
        {
        sensor->avg_data = 0;
        sensor->sensor_data[sensor->num_data] = data->value;
        sensor->num_data++;
        sensor->data_sum = sensor->data_sum + data->value;
        } else {
        if(sensor->num_data==RUN_AVG_LENGTH-1)
        {
            sensor->num_data++;
            sensor->sensor_data[sensor->num_data] = data->value;
            sensor->data_sum = sensor->data_sum + data->value;
            sensor->avg_data = sensor->data_sum/RUN_AVG_LENGTH;
        } 
        else {
            sensor->data_sum = sensor->data_sum - sensor->sensor_data[0] + data->value;
            for(int i=0; i<RUN_AVG_LENGTH-1; i++)
            {
                sensor->sensor_data[i] = sensor->sensor_data[i+1];
            }
            sensor->sensor_data[RUN_AVG_LENGTH-1] = data->value;
            sensor->avg_data = sensor->data_sum/RUN_AVG_LENGTH;
        }

        if(sensor->avg_data<SET_MIN_TEMP)
        {
            asprintf(&log_message,"The sensor node with %hu reports it's too cold.(running avg temperature= %lf)\n",sensor->sensor_id,sensor->avg_data);
            fifo_log(log_message);
        }
        else if(sensor->avg_data>SET_MAX_TEMP)
        {
            asprintf(&log_message,"The sensor node with %hu reports it's too hot.(running avg temperature= %lf)\n",sensor->sensor_id,sensor->avg_data);  
            fifo_log(log_message);
        }
            
        }


    } else {
        asprintf(&log_message,"Received sensor data with invalid sensor node %d \n", data->id);
        fifo_log(log_message);
    }
    free(data);

}





void datamgr_parse_sensor_files(FILE *fp_sensor_map, FILE *fp_sensor_data)
{
    ERROR_HANDLER(fp_sensor_map == NULL, "Error openning streams - NULL\n");
    sensor_dplist = dpl_create(element_copy, element_free, element_compare);
    char l_length[10];
    unsigned char map_index = 0;
    while(fgets(l_length, sizeof(l_length), fp_sensor_map) != NULL)
    {
        element_t *map = NULL;
        map = calloc(1,sizeof(element_t));
        map->num_data = 0;
        map->data_sum = 0;
        sscanf(l_length, "%hu%hu", &(map->room_id), &(map->sensor_id));
        dpl_insert_at_index(sensor_dplist,map,map_index,false);
        map_index++;
    }
    while(!feof(fp_sensor_data)) 
    {
        sensor_data_t* data = NULL;
        data = calloc(1,sizeof(sensor_data_t));
        fread(&(data->id),sizeof(sensor_id_t),1,fp_sensor_data);
        fread(&(data->value),sizeof(sensor_value_t),1,fp_sensor_data);
        fread(&(data->ts),sizeof(sensor_ts_t),1,fp_sensor_data);
        element_t *sensor = NULL;
        for(int i=0; i<dpl_size(sensor_dplist); i++)
        {
            sensor = dpl_get_element_at_index(sensor_dplist, i);
            if(sensor->sensor_id == data->id)
            {
                break;
            }
        }
        sensor->last_modified = data->ts;
        if(sensor->num_data<RUN_AVG_LENGTH-1)
        {
            sensor->avg_data = 0;
            sensor->sensor_data[sensor->num_data] = data->value;
            sensor->num_data++;
            sensor->data_sum = sensor->data_sum + data->value;
        } 
        else {
            if(sensor->num_data==RUN_AVG_LENGTH-1)
            {
                sensor->num_data++;
                sensor->sensor_data[sensor->num_data] = data->value;
                sensor->data_sum = sensor->data_sum + data->value;
                sensor->avg_data = sensor->data_sum/RUN_AVG_LENGTH;
            } 
            else {
                sensor->data_sum = sensor->data_sum - sensor->sensor_data[0] + data->value;
                for(int i=0; i<RUN_AVG_LENGTH-1; i++)
                {
                    sensor->sensor_data[i] = sensor->sensor_data[i+1];
                }
                sensor->sensor_data[RUN_AVG_LENGTH-1] = data->value;
                sensor->avg_data = sensor->data_sum/RUN_AVG_LENGTH;
            }

            if(sensor->avg_data<SET_MIN_TEMP)
            {
                fprintf(stderr,"The temperature of sensor %hu in room %hu is too low. running average: %lf, lower than: %lf, timestamp: %ld\n",sensor->sensor_id,sensor->room_id,sensor->avg_data,(double)SET_MIN_TEMP,sensor->last_modified);
            }
            else if(sensor->avg_data>SET_MAX_TEMP)
            {
                fprintf(stderr,"The temperature of sensor %hu in room %hu is too high. running average: %lf, higher than: %lf, timestamp: %ld\n",sensor->sensor_id,sensor->room_id,sensor->avg_data,(double)SET_MAX_TEMP,sensor->last_modified);  
            }
            
        } 
        free(data);    
    }
    
}

void datamgr_free()
{
    dpl_free(&sensor_dplist, true);
}

uint16_t datamgr_get_room_id(sensor_id_t sensor_id)
{
    element_t *sensor, *temp=malloc(sizeof(element_t));
    temp->sensor_id = sensor_id;
    sensor = dpl_get_element_at_index(sensor_dplist,dpl_get_index_of_element(sensor_dplist, temp));
    free(temp);
    return (sensor != NULL) ? sensor->room_id : 0;
}

sensor_value_t datamgr_get_avg(sensor_id_t sensor_id)
{
    element_t *sensor, *temp=malloc(sizeof(element_t));
    temp->sensor_id = sensor_id;
    sensor = dpl_get_element_at_index(sensor_dplist,dpl_get_index_of_element(sensor_dplist, temp));
    free(temp);
    return (sensor != NULL) ? sensor->avg_data : 0.0;
}

time_t datamgr_get_last_modified(sensor_id_t sensor_id)
{
    element_t *sensor, *temp=malloc(sizeof(element_t));
    temp->sensor_id = sensor_id;
    sensor = dpl_get_element_at_index(sensor_dplist,dpl_get_index_of_element(sensor_dplist, temp));
    free(temp);
    return (sensor != NULL) ? sensor->last_modified : 0;
}

int datamgr_get_total_sensors()
{
    return (sensor_dplist != NULL) ? dpl_size(sensor_dplist) : -1;
}

void * element_copy(void * element)
{
    element_t *sensor = NULL;
    sensor = malloc(sizeof(element_t));
    *sensor = *(element_t *)element;
    return (void *) sensor;
}

void element_free(void ** element)
{
    element_t *sensor = *element;
    free(sensor);
    sensor = NULL;
}

int element_compare(void * x, void * y)
{
    return ((((element_t*)x)->sensor_id < ((element_t*)y)->sensor_id) ? -1 : (((element_t*)x)->sensor_id == ((element_t*)y)->sensor_id) ? 0 : 1);
}