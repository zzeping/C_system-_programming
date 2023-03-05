/**
 * \author Zeping Zhang
 */

#define _GNU_SOURCE
#include <stdlib.h>
#include <stdio.h>
#include "sbuffer.h"
#include <pthread.h>


/**
 * basic node for the buffer, these nodes are linked together to create the buffer
 */
typedef struct sbuffer_node {
    struct sbuffer_node *next;  /**< a pointer to the next node*/
    sensor_data_t data;         /**< a structure containing the data */
    int datamgr_read;
    int sensor_db_read;
} sbuffer_node_t;

/**
 * a structure to keep track of the buffer
 */
struct sbuffer {
    sbuffer_node_t *head;       /**< a pointer to the first node in the buffer */
    sbuffer_node_t *tail;       /**< a pointer to the last node in the buffer */
    pthread_rwlock_t *rwlock;
};

int sbuffer_init(sbuffer_t **buffer) {
    *buffer = malloc(sizeof(sbuffer_t));
    if (*buffer == NULL) return SBUFFER_FAILURE;
    (*buffer)->head = NULL;
    (*buffer)->tail = NULL;

    (*buffer)->rwlock = malloc(sizeof(pthread_rwlock_t)); // initialize the rwlock
    pthread_rwlock_init((*buffer)->rwlock,NULL);

    return SBUFFER_SUCCESS;
}

int sbuffer_free(sbuffer_t **buffer) {
    sbuffer_node_t *dummy;
    if ((buffer == NULL) || (*buffer == NULL)) {
        return SBUFFER_FAILURE;
    }
    pthread_rwlock_destroy((*buffer)->rwlock);
    free((*buffer)->rwlock);

    while ((*buffer)->head) {
        dummy = (*buffer)->head;
        (*buffer)->head = (*buffer)->head->next;
        free(dummy);
    }
    free(*buffer);
    *buffer = NULL;
    return SBUFFER_SUCCESS;
}

int sbuffer_remove(sbuffer_t *buffer, sensor_data_t *data) {
    sbuffer_node_t *dummy;
    if (buffer == NULL) return SBUFFER_FAILURE;
    if (buffer->head == NULL) return SBUFFER_NO_DATA;
    *data = buffer->head->data;
    dummy = buffer->head;
    if (buffer->head == buffer->tail) // buffer has only one node
    {
        buffer->head = buffer->tail = NULL;
    } else  // buffer has many nodes empty
    {
        buffer->head = buffer->head->next;
    }
    free(dummy);
    return SBUFFER_SUCCESS;
}

int sbuffer_insert(sbuffer_t *buffer, sensor_data_t *data) {
    sbuffer_node_t *dummy;
    if (buffer == NULL) return SBUFFER_FAILURE;
    dummy = malloc(sizeof(sbuffer_node_t));
    if (dummy == NULL) return SBUFFER_FAILURE;
    dummy->data = *data;
    dummy->next = NULL;
    dummy->datamgr_read=0;
    dummy->sensor_db_read=0;

    pthread_rwlock_wrlock(buffer->rwlock);

    if (buffer->tail == NULL) // buffer empty (buffer->head should also be NULL
    {
        buffer->head = buffer->tail = dummy;
    } else // buffer not empty
    {
        buffer->tail->next = dummy;
        buffer->tail = buffer->tail->next;
    }
    pthread_rwlock_unlock(buffer->rwlock);
    return SBUFFER_SUCCESS;
}

int sbuffer_size(sbuffer_t *buffer)
{
    int size = 0;
    if (buffer == NULL) return -1;
    pthread_rwlock_rdlock(buffer->rwlock);
    if (buffer->head != NULL)
    {
        sbuffer_node_t *dummy = buffer->head;
        while (dummy != NULL)
        {
            size++;
            dummy = dummy->next;
        }
    }
    pthread_rwlock_unlock(buffer->rwlock);
    return size;
}

int datamgr_first_to_read(sbuffer_t *buffer, sensor_data_t *data)
{
    if (buffer == NULL) return -1;
    if (buffer->head == NULL) return -1;
    pthread_rwlock_wrlock(buffer->rwlock);
    if(buffer->head->datamgr_read==1&&buffer->head->sensor_db_read==1)
    {
        sensor_data_t *remove=malloc(sizeof(sensor_data_t));
        sbuffer_remove(buffer,remove);
        free(remove);
    }
    
    sbuffer_node_t *dummy = buffer->head;
    while (dummy->datamgr_read ==1&&dummy->next!=NULL)
    {
        dummy = dummy->next;
    }
    dummy->datamgr_read = 1;
    pthread_rwlock_unlock(buffer->rwlock);
    data->id = dummy->data.id;
    data->ts = dummy->data.ts;
    data->value = dummy->data.value;
    
    return 1;
}

int sensor_db_first_to_read(sbuffer_t *buffer, sensor_data_t *data)
{
    if (buffer == NULL) return -1;
    if (buffer->head == NULL) return -1;
    pthread_rwlock_wrlock(buffer->rwlock);
    if(buffer->head->datamgr_read==1&&buffer->head->sensor_db_read==1)
    {
        sensor_data_t *remove=malloc(sizeof(sensor_data_t));
        sbuffer_remove(buffer,remove);
        free(remove);
    }
    sbuffer_node_t *dummy = buffer->head;
    while (dummy->sensor_db_read ==1&&dummy->next!=NULL)
    {
        dummy = dummy->next;
    }
    dummy->sensor_db_read = 1;
    pthread_rwlock_unlock(buffer->rwlock);
    data->id = dummy->data.id;
    data->ts = dummy->data.ts;
    data->value = dummy->data.value;
    return 1;
}

int datamgr_unread_amount(sbuffer_t *buffer)
{
    int amount = 0;
    if (buffer == NULL) return 0;
    pthread_rwlock_rdlock(buffer->rwlock);
    if (buffer->head != NULL)
    {
        sbuffer_node_t *dummy = buffer->head;
        while (dummy != NULL)
        {
            if(dummy->datamgr_read==0) amount++;
            dummy = dummy->next;
        }
    }
    pthread_rwlock_unlock(buffer->rwlock);
    return amount;
}

int sensor_db_unread_amount(sbuffer_t *buffer)
{
    int amount = 0;
    if (buffer == NULL) return 0;
    pthread_rwlock_rdlock(buffer->rwlock);
    if (buffer->head != NULL)
    {
        sbuffer_node_t *dummy = buffer->head;
        while (dummy != NULL)
        {
            if(dummy->sensor_db_read==0) amount++;
            dummy = dummy->next;
        }
    }
    pthread_rwlock_unlock(buffer->rwlock);
    return amount;
}