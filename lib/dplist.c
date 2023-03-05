/**
 * \author Zeping Zhang
 */
#define _GNU_SOURCE 
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include "dplist.h"


/*
 * definition of error codes
 * */
#define DPLIST_NO_ERROR 0
#define DPLIST_MEMORY_ERROR 1 // error due to mem alloc failure
#define DPLIST_INVALID_ERROR 2 //error due to a list operation applied on a NULL list 

#ifdef DEBUG
#define DEBUG_PRINTF(...) 									                                        \
        do {											                                            \
            fprintf(stderr,"\nIn %s - function %s at line %d: ", __FILE__, __func__, __LINE__);	    \
            fprintf(stderr,__VA_ARGS__);								                            \
            fflush(stderr);                                                                         \
                } while(0)
#else
#define DEBUG_PRINTF(...) (void)0
#endif


#define DPLIST_ERR_HANDLER(condition, err_code)                         \
    do {                                                                \
            if ((condition)) DEBUG_PRINTF(#condition " failed\n");      \
            assert(!(condition));                                       \
        } while(0)


/*
 * The real definition of struct list / struct node
 */

struct dplist_node {
    dplist_node_t *prev, *next;
    void *element;
};

struct dplist {
    dplist_node_t *head;

    void *(*element_copy)(void *src_element);

    void (*element_free)(void **element);

    int (*element_compare)(void *x, void *y);
};

dplist_t *dpl_create(// callback functions
        void *(*element_copy)(void *src_element),
        void (*element_free)(void **element),
        int (*element_compare)(void *x, void *y)
) {
    dplist_t *list;
    list = malloc(sizeof(struct dplist));
    DPLIST_ERR_HANDLER(list == NULL, DPLIST_MEMORY_ERROR);
    list->head = NULL;
    list->element_copy = element_copy;
    list->element_free = element_free;
    list->element_compare = element_compare;
    return list;
}

void dpl_free(dplist_t **list, bool free_element) {

    //TODO: add your code here
  dplist_node_t* temp;
  if(*list != NULL){
    dplist_t *t = *list;
    if(t->head != NULL){
      while(t->head->next != NULL) {
        temp = t->head;
        t->head = temp->next;
        if(free_element){
          t->element_free(&(temp->element));
        }
        free(temp);
      }
	if(free_element) {
	  t->element_free(&(t->head->element)); 
	} 
	free(t->head);
  t->head = NULL;
    }
    	free(*list);
	    *list = NULL;
  }
}

dplist_t *dpl_insert_at_index(dplist_t *list, void *element, int index, bool insert_copy) {

    //TODO: add your code here
  dplist_node_t *ref_at_index, *list_node;
  if (list == NULL) return NULL;
  list_node = malloc(sizeof(dplist_node_t));
  DPLIST_ERR_HANDLER(list_node == NULL, DPLIST_MEMORY_ERROR);
  if (list->head == NULL) { //  empty
    list_node->prev = NULL;
    list_node->next = NULL;
    list->head = list_node;
    if(insert_copy && (element != NULL)){
      list_node->element = list->element_copy(element);
    } else{
      list_node->element = element;
    }
  } else if (index <= 0) { //  beginning
    list_node->prev = NULL;
    list_node->next = list->head;
    list->head->prev = list_node;
    list->head = list_node;
    if(insert_copy && (element != NULL)){
      list_node->element = list->element_copy(element);
    } else{
      list_node->element = element;
    }
    
  } else {
    ref_at_index = dpl_get_reference_at_index(list, index);
    assert(ref_at_index != NULL);
    if (index < dpl_size(list)) { //  middle
      list_node->prev = ref_at_index->prev;
      list_node->next = ref_at_index;
      ref_at_index->prev->next = list_node;
      ref_at_index->prev = list_node;
    } else { // end
        assert(ref_at_index->next == NULL);
        list_node->next = NULL;
        list_node->prev = ref_at_index;
        ref_at_index->next = list_node;
      }
  if(insert_copy && (element != NULL)){
      list_node->element = list->element_copy(element);
    } else{
      list_node->element = element;
    }
  }
  return list;
}

dplist_t *dpl_remove_at_index(dplist_t *list, int index, bool free_element) {

    //TODO: add your code here
  dplist_node_t* temp;
  dplist_node_t *ref_at_index;
  
  if (list == NULL) return NULL;
  if (list->head == NULL) return list;

  if (index <= 0) {
    temp = list->head;
    if(list->head->next != NULL){
      list->head = temp->next;
      temp->next->prev = NULL;
      }else{
        list->head = NULL;
      }
      if(free_element) list->element_free(&(temp->element));
      free(temp);
  }else{
    ref_at_index = dpl_get_reference_at_index(list, index);
    assert(ref_at_index != NULL);
    temp = ref_at_index;

    if(index < (dpl_size(list)-1)){
      
        ref_at_index->prev->next = ref_at_index->next;
        temp->next->prev = temp->prev;
        if(free_element) list->element_free(&(temp->element));
        free(temp);
    	}else{
      	assert(ref_at_index->next == NULL);
      	if(ref_at_index->prev == NULL) list->head = NULL;
      	else {temp->prev->next = NULL;}
      	if(free_element) list->element_free(&(temp->element));
     	 free(temp);
    	}
	}
return list;
}

int dpl_size(dplist_t *list) {

    //TODO: add your code here
  dplist_node_t* temp;
  int number = 1;
  if (list == NULL) return -1;
  if(list->head == NULL) return 0;
  else{
    temp = list->head;
    while(temp->next != NULL){
      temp = temp->next;
      number++;
    }
  }
  return number;
}

void *dpl_get_element_at_index(dplist_t *list, int index) {

    //TODO: add your code here
  dplist_node_t *ref_at_index;
  void *t;
  
  if (list == NULL) return 0;
  if (list->head == NULL) return 0;
  else if(index <= 0){
    return list->head->element;
  }else if(index+1 > dpl_size(list)){
    ref_at_index = dpl_get_reference_at_index(list, dpl_size(list)-1);
    t = ref_at_index->element;
  }else{
    ref_at_index = dpl_get_reference_at_index(list, index);
    assert(ref_at_index != NULL);
    t = ref_at_index->element;
    }
  return t;
}

int dpl_get_index_of_element(dplist_t *list, void *element) {

    //TODO: add your code here
  dplist_node_t* temp;
  if (list == NULL) return -1;
  temp = list->head;
  if(element == NULL) return -1;
  int index = 0;
  if (list->head == NULL) return -1;  
  while(list->element_compare(element,temp->element)!=0){
    if(temp->next == NULL) return -1;
     temp = temp->next;
     index++; 
  }
  return index;
}

dplist_node_t *dpl_get_reference_at_index(dplist_t *list, int index) {

    //TODO: add your code here
  int count;
  dplist_node_t *dummy;
  //DPLIST_ERR_HANDLER(list == NULL, DPLIST_INVALID_ERROR);
  if (list == NULL) return NULL;
  if (list->head == NULL) return NULL;
  for (dummy = list->head, count = 0; dummy->next != NULL; dummy = dummy->next,count++) {
    if (count >= index) return dummy;
    }
  return dummy;
}

void *dpl_get_element_at_reference(dplist_t *list, dplist_node_t *reference) {

    //TODO: add your code here

    if (list == NULL) return NULL;
    if (list->head == NULL) return NULL;
    if (reference != NULL)
    {
      dplist_node_t *temp = list->head;
      while (temp != NULL)
      {
        if (temp == reference)  return reference->element;
        temp = temp->next;
      }
    }
    return NULL;

}

dplist_node_t *dpl_get_first_reference(dplist_t *list) {
  if (list == NULL) return NULL;
  if (list->head == NULL) return NULL;
  dplist_node_t *temp = list->head;
  return temp;

}

dplist_node_t *dpl_get_last_reference(dplist_t *list) {
  if (list == NULL) return NULL;
  if (list->head == NULL) return NULL;
  dplist_node_t *temp = list->head;
  while(temp->next != NULL){
    temp = temp->next;
  }
  return temp;
}


dplist_node_t *dpl_get_next_reference(dplist_t *list, dplist_node_t *reference) {
  if (list == NULL) return NULL;
  if (list->head == NULL) return NULL;
  if (reference == NULL) return NULL;
  dplist_node_t *temp = list->head;
  while(temp != NULL){
    if(temp == reference) return temp->next;
    temp = temp->next;
  }
  return NULL;

}

dplist_node_t *dpl_get_previous_reference(dplist_t *list, dplist_node_t *reference) {
  if (list == NULL) return NULL;
  if (list->head == NULL) return NULL;
  if (reference == NULL) return NULL;
  dplist_node_t *temp = list->head;
  while(temp != NULL){
    if(temp == reference) return temp->prev;
    temp = temp->next;
  }
  return NULL;

}

dplist_node_t *dpl_get_reference_of_element(dplist_t *list, void *element) {
  if (list == NULL) return NULL;
  if (element == NULL) return NULL;
  if (list->head == NULL) return NULL;
  dplist_node_t *temp = list->head;
  while(list->element_compare(element,temp->element)!=0){
    if(temp->next == NULL) return NULL;
    temp = temp->next;
  }
  return temp;

}

int dpl_get_index_of_reference(dplist_t *list, dplist_node_t *reference) {
  if (list == NULL) return -1;
  if (list->head == NULL) return -1;
  if (reference == NULL) return -1;
  dplist_node_t *temp = list->head;
  int index = 0;
  while(temp != reference){
    if(temp->next == NULL) return -1;
    temp = temp->next;
    index++;
  }
  return index;

}

dplist_t *dpl_insert_at_reference(dplist_t *list, void *element, dplist_node_t *reference, bool insert_copy) {
  if (list == NULL) return NULL;
  if (element == NULL) return NULL;
  if (reference == NULL) return NULL;
  int index = dpl_get_index_of_reference(list, reference);
  if(index != -1){
    return dpl_insert_at_index(list, element, index,insert_copy);
  }
  return list;

}

dplist_t *dpl_insert_sorted(dplist_t *list, void *element, bool insert_copy) {
  if(list == NULL) return NULL;
  
  if(element == NULL){
    return NULL;
  }
  if(list->head == NULL) return dpl_insert_at_index(list, element, 0, insert_copy);
  
  dplist_node_t *temp = list->head;
  int index=0;
  while(list->element_compare(element, temp->element)>0){
    temp = temp->next;
    index++;
    if(temp == NULL) break;
  }
  return dpl_insert_at_index(list, element, index, insert_copy);

} 


dplist_t *dpl_remove_at_reference(dplist_t *list, dplist_node_t *reference, bool free_element) {
  if (list == NULL) return NULL;
  if (reference == NULL) return NULL;
  int index = dpl_get_index_of_reference(list, reference);
  if(index != -1){
    return dpl_remove_at_index(list, index, free_element);
  }
  return list;

}

dplist_t *dpl_remove_element(dplist_t *list, void *element, bool free_element) {
  if(list == NULL) return NULL;
  int index = dpl_get_index_of_element(list, element);
  while(index != -1){
    return dpl_remove_at_index(list, index, free_element);
  }
  return list;

}

