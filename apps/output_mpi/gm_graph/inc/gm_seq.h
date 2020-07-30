#ifndef GM_SEQ_H
#define GM_SEQ_H

#include <stdio.h>
#include <list>
#include "gm_internal.h"
#include "gm_runtime.h"
#include "gm_graph.h"

//mpi
#define MASTER 0
//mpi

class Seq_Iterator
{
private:
    int iter;
    int end;
    node_t *Q;

public:
    Seq_Iterator(node_t *_Q, int iterator_begin, int iterator_end) :
            Q(_Q), iter(iterator_begin), end(iterator_end) {
    }

    inline bool has_next() {
        return iter != end;
    }

    inline node_t get_next() {
        node_t value = Q[iter];
        iter++;
        return value;
    }
};

class Rev_Iterator
{
private:
    int iter;
    int end;
    node_t *Q;

public:
    Rev_Iterator(node_t *_Q, int iterator_begin, int iterator_end) :
            Q(_Q), iter(iterator_begin), end(iterator_end) {
    }

    inline bool has_next() {
        return iter != end;
    }

    inline node_t get_next() {
        node_t value = Q[iter];
        iter--;
        return value;
    }
};

/* MPI Implementation of node sequence starts here.
 */
class gm_node_seq
{
public:
    gm_node_seq(gm_graph& _G) : G(_G)
    {
      init();
    }

    void init() {
      //G = _G;
      my_rank = G.get_rank();
      // Allocate memory for node sequence
      if (G.get_rank() == MASTER) {
        // FIXME: The size is a guess. It can be more than this.
        Q = new node_t[2 * G.num_nodes()];
        // Initialize both front and back to num_nodes.
        Q_front = G.num_nodes();
        Q_back = G.num_nodes();
      }
      // Attach it to window.
      G.win_attach_master(Q, 2 * G.num_nodes());
      G.win_attach_master(&Q_front, 1);
      G.win_attach_master(&Q_back, 1);
    }

    virtual ~gm_node_seq() {
      if(my_rank == MASTER){
        delete[] Q;
      }
    }

    //------------------------------------------------------------
    // API
    //   push_back/front, pop_back/front, clear, get_size
    //   push has separate parallel interface
    //back<--|__|__|__|__|-->front
    //back and 
    //------------------------------------------------------------
    void push_back(node_t e) {
      if(G.get_rank() == MASTER)
      {
        if(Q_front == Q_back){ 
          Q_front++;
        }
        Q[Q_back--] = e;
      }
      MPI_Barrier(MPI_COMM_WORLD);
    }

    void push_front(node_t e) {
      if(G.get_rank() == MASTER)
      {
        if(Q_front == Q_back){ 
          Q_back--;
        }
        Q[Q_front++] = e; 
      }
      MPI_Barrier(MPI_COMM_WORLD);
    }

    node_t pop_back() {
        node_t e;
        if(G.get_rank() == MASTER)
        {
          e = Q[++Q_back];
          if(Q_back == Q_front-1)
          {
            Q_back = G.num_nodes();
            Q_front = Q_back;
          }
        }
        MPI_Bcast(&e, 1, MPI_INT, MASTER, MPI_COMM_WORLD);
        return e;
    }

    node_t pop_front() {
        node_t e;
        if(G.get_rank() == MASTER)
        {
          e = Q[--Q_front];
          if(Q_back == Q_front-1)
          {
            Q_back = G.num_nodes();
            Q_front = Q_back;
          }
        }
        MPI_Bcast(&e, 1, MPI_INT, MASTER, MPI_COMM_WORLD);
        return e;
    }

    void clear() {
      if(G.get_rank() == MASTER)
      {
        Q_back = G.num_nodes();
        Q_front = Q_back;
      }
      MPI_Barrier(MPI_COMM_WORLD);
    }

    int get_size() {
        int size;
        if(G.get_rank() == MASTER)
        {
          if(Q_front == Q_back)
            size = 0;
          else
            size = (Q_front - Q_back) - 1;
        }
        MPI_Bcast(&size, 1, MPI_INT, MASTER, MPI_COMM_WORLD);
        return size;
    }

    // for parallel execution
    //TODO: par routines for MPI need to be implemented.
    //void push_back_par(T e, int tid) {
    //    local_Q_back[tid].push_back(e);
    //}
    //void push_front_par(T e, int tid) {
    //    local_Q_front[tid].push_front(e);
    //}

    // parallel pop is prohibited

    //-------------------------------------------
    // called when parallel addition is finished
    //-------------------------------------------
    //void merge() {
    //    for (int i = 0; i < max_thread; i++) {
    //        if (local_Q_front[i].size() > 0) Q.splice(Q.begin(), local_Q_front[i]);

    //        if (local_Q_back[i].size() > 0) Q.splice(Q.end(), local_Q_back[i]);
    //    }
    //}


    typedef Seq_Iterator seq_iter;
    typedef Rev_Iterator rev_iter;
    //typedef seq_iter par_iter; // type-alias

    seq_iter prepare_seq_iteration() {
        seq_iter I(Q, Q_back+1, Q_front);
        return I; // copy return
    }
    rev_iter prepare_rev_iteration() {
        rev_iter I(Q, Q_front-1, Q_back);
        return I; // copy return
    }

    // [xxx] to be implemented
    //par_iter prepare_par_iteration(int thread_id, int max_threads) {
    //    assert(false);
    //    return NULL;
    //}

private:

    gm_graph G;
    int my_rank;

    node_t *Q;
    int Q_front;
    int Q_back;
    //typename std::list<T> Q;
    //typename std::list<T>* local_Q_front;
    //typename std::list<T>* local_Q_back;

    //int max_thread;
    //static const int THRESHOLD = 1024;

};


#endif
