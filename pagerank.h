#ifndef GM_GENERATED_CPP_PAGERANK_H
#define GM_GENERATED_CPP_PAGERANK_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <float.h>
#include <limits.h>
#include <cmath>
#include <algorithm>
#include <omp.h>
#include "gm.h"

#define MASTER 0
#define lock_win_for_node(_node)\
{\
    int _err_val;\
    int _rank;\
    _rank = G.get_rank_node(_node);\
    _err_val=MPI_Win_lock(MPI_LOCK_EXCLUSIVE,_rank, 0, win);\
    if(_err_val != MPI_SUCCESS)\
    fprintf(stderr,"error occured for lock with val%d",_err_val);\
}

#define unlock_win_for_node(_node)\
{\
    int _err_val;\
    int _rank;\
    _rank = G.get_rank_node(_node);\
    _err_val=MPI_Win_unlock(_rank, win);\
    if(_err_val != MPI_SUCCESS)\
    fprintf(stderr,"error occured for lock with val%d",_err_val);\
}

#define lock_and_get_node_property_int(_result, _ba_index, _rank, _prop_index)\
{\
    int _err_val;\
    _err_val=MPI_Win_lock(MPI_LOCK_EXCLUSIVE,_rank, 0, win);\
    if(_err_val != MPI_SUCCESS)\
    fprintf(stderr,"error occured for lock with val%d",_err_val);\
    MPI_Get(&_result, 1, MPI_INT, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_prop_index*sizeof(int), 1, MPI_INT, win);\
    MPI_Win_unlock(_rank, win);\
}

#define lock_and_get_edge_property_int(_result, _ba_index, _rank, _prop_index)\
{\
    int _err_val;\
    _err_val=MPI_Win_lock(MPI_LOCK_EXCLUSIVE,_rank, 0, win);\
    if(_err_val != MPI_SUCCESS)\
    fprintf(stderr,"error occured for lock with val%d",_err_val);\
    MPI_Get(&_result, 1, MPI_INT, _rank, base_address[G.get_num_processes()*_ba_index+_rank] + _prop_index*sizeof(int), 1, MPI_INT, win);\
    MPI_Win_unlock(_rank, win);\
}

#define lock_and_put_node_property_int(_result, _ba_index, _rank, _prop_index)\
{\
    int _err_val;\
    _err_val=MPI_Win_lock(MPI_LOCK_EXCLUSIVE,_rank, 0, win);\
    if(_err_val != MPI_SUCCESS)\
    fprintf(stderr,"error occured for lock with val%d",_err_val);\
    MPI_Put(&_result, 1, MPI_INT, _rank, base_address[G.get_num_processes()*_ba_index +_rank] + _prop_index*sizeof(int), 1, MPI_INT, win);\
    MPI_Win_unlock(_rank, win);\
}

#define lock_and_put_edge_property_int(_result, _ba_index, _rank, _prop_index)\
{\
    int _err_val;\
    _err_val=MPI_Win_lock(MPI_LOCK_EXCLUSIVE,_rank, 0, win);\
    if(_err_val != MPI_SUCCESS)\
    fprintf(stderr,"error occured for lock with val%d",_err_val);\
    MPI_Put(&_result, 1, MPI_INT, _rank, base_address[G.get_num_processes()*_ba_index + _rank]+_prop_index*sizeof(int), 1, MPI_INT, win);\
    MPI_Win_unlock(_rank, win);\
}

#define get_node_property_int(_result, _ba_index, _rank, _prop_index)\
{\
    MPI_Get(&_result, 1, MPI_INT, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_prop_index*sizeof(int), 1, MPI_INT, win);\
}

#define get_edge_property_int(_result, _ba_index, _rank, _prop_index)\
{\
    MPI_Get(&_result, 1, MPI_INT, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_prop_index*sizeof(int), 1, MPI_INT, win);\
}

#define put_node_property_int(_result, _ba_index, _rank, _prop_index)\
{\
    MPI_Put(&_result, 1, MPI_INT, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_prop_index*sizeof(int), 1, MPI_INT, win);\
}

#define put_edge_property_int(_result, _ba_index, _rank, _prop_index)\
{\
    MPI_Put(&_result, 1, MPI_INT, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_prop_index*sizeof(int), 1, MPI_INT, win);\
}

#define lock_and_get_node_property_bool(_result, _ba_index, _rank, _prop_index)\
{\
    int _err_val;\
    _err_val=MPI_Win_lock(MPI_LOCK_EXCLUSIVE,_rank, 0, win);\
    if(_err_val != MPI_SUCCESS)\
    fprintf(stderr,"error occured for lock with val%d",_err_val);\
    MPI_Get(&_result, 1, MPI_C_BOOL, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_prop_index*sizeof(int), 1, MPI_C_BOOL, win);\
    MPI_Win_unlock(_rank, win);\
}

#define lock_and_put_node_property_bool(_result, _ba_index, _rank, _prop_index)\
{\
    int _err_val;\
    _err_val=MPI_Win_lock(MPI_LOCK_EXCLUSIVE,_rank, 0, win);\
    if(_err_val != MPI_SUCCESS)\
    fprintf(stderr,"error occured for lock with val%d",_err_val);\
    MPI_Put(&_result, 1, MPI_C_BOOL, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_prop_index*sizeof(int), 1, MPI_C_BOOL, win);\
    MPI_Win_unlock(_rank, win);\
}

#define get_node_property_bool(_result, _ba_index, _rank, _prop_index)\
{\
    MPI_Get(&_result, 1, MPI_C_BOOL, _rank, base_address[G.get_num_processes()*_ba_index+_rankj + _prop_index*sizeof(int), 1, MPI_C_BOOL, win);\
}

#define put_node_property_bool(_result, _ba_index, _rank, _prop_index)\
{\
    MPI_Put(&_result, 1, MPI_C_BOOL, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_prop_index*sizeof(int), 1, MPI_C_BOOL, win);\
}

#define lock_and_put_node_property_float(_result, _ba_index, _rank, _prop_index)\
{\
    int _err_val;\
    _err_val=MPI_Win_lock(MPI_LOCK_EXCLUSIVE,_rank, 0, win);\
    if(_err_val != MPI_SUCCESS)\
    fprintf(stderr,"error occured for lock with val%d",_err_val);\
    MPI_Put(&_result, 1, MPI_FLOAT, _rank, base_address[G.get_num_processes()*_ba_index +_rank] + _prop_index*sizeof(float), 1, MPI_FLOAT, win);\
    MPI_Win_unlock(_rank, win);\
}

#define lock_and_get_node_property_float(_result, _ba_index, _rank, _prop_index)\
{\
    int _err_val;\
    _err_val=MPI_Win_lock(MPI_LOCK_EXCLUSIVE,_rank, 0, win);\
    if(_err_val != MPI_SUCCESS)\
    fprintf(stderr,"error occured for lock with val%d",_err_val);\
    MPI_Get(&_result, 1, MPI_FLOAT, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_prop_index*sizeof(float), 1, MPI_FLOAT, win);\
    MPI_Win_unlock(_rank, win);\
}

#define lock_and_get_node_property_double(_result, _ba_index, _rank, _prop_index)\
{\
    int _err_val;\
    _err_val=MPI_Win_lock(MPI_LOCK_EXCLUSIVE,_rank, 0, win);\
    if(_err_val != MPI_SUCCESS)\
    fprintf(stderr,"error occured for lock with val%d",_err_val);\
    MPI_Get(&_result, 1, MPI_DOUBLE, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_prop_index*sizeof(double), 1, MPI_DOUBLE, win);\
    MPI_Win_unlock(_rank, win);\
}

#define lock_and_get_edge_property_double(_result, _ba_index, _rank, _prop_index)\
{\
    int _err_val;\
    _err_val=MPI_Win_lock(MPI_LOCK_EXCLUSIVE,_rank, 0, win);\
    if(_err_val != MPI_SUCCESS)\
    fprintf(stderr,"error occured for lock with val%d",_err_val);\
    MPI_Get(&_result, 1, MPI_DOUBLE, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_prop_index*sizeof(double), 1, MPI_DOUBLE, win);\
    MPI_Win_unlock(_rank, win);\
}

#define lock_and_put_node_property_double(_result, _ba_index, _rank, _prop_index)\
{\
    int _err_val;\
    _err_val=MPI_Win_lock(MPI_LOCK_EXCLUSIVE,_rank, 0, win);\
    if(_err_val != MPI_SUCCESS)\
    fprintf(stderr,"error occured for lock with val%d",_err_val);\
    MPI_Put(&_result, 1, MPI_DOUBLE, _rank, base_address[G.get_num_processes()*_ba_index +_rank] + _prop_index*sizeof(double), 1, MPI_DOUBLE, win);\
    MPI_Win_unlock(_rank, win);\
}

#define get_edge_property_double(_result, _ba_index, _rank, _prop_index)\
{\
    MPI_Get(&_result, 1, MPI_DOUBLE, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_prop_index*sizeof(double), 1, MPI_DOUBLE, win);\
}

#define get_node_property_double(_result, _ba_index, _rank, _prop_index)\
{\
    MPI_Get(&_result, 1, MPI_DOUBLE, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_prop_index*sizeof(double), 1, MPI_DOUBLE, win);\
}

#define put_node_property_double(_result, _ba_index, _rank, _prop_index)\
{\
    MPI_Put(&_result, 1, MPI_DOUBLE, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_prop_index*sizeof(double), 1, MPI_DOUBLE, win);\
}

#define put_edge_property_double(_result, _ba_index, _rank, _prop_index)\
{\
    MPI_Put(&_result, 1, MPI_DOUBLE, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_prop_index*sizeof(double), 1, MPI_DOUBLE, win);\
}

void pagerank(gm_graph& G, double e, 
    double d, int32_t max, 
    double*G_pg_rank);
extern MPI_Win win;
extern MPI_Aint *base_address;
extern int base_address_index;

#endif
