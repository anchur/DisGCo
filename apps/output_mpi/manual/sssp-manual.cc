#include "sssp.h"

extern MPI_Win win;
extern MPI_Aint *base_address;
extern int base_address_index;

void sssp(gm_graph& G, int32_t* G_dist, 
    int32_t* G_len, node_t& root)

//G_dist is a node property while G_len is an edge property
//should be taken care while passing it to the function

{
    gm_rt_initialize();
    G.freeze();

    // will check atlast

    bool fin = false ;

    //bool* G_updated; = gm_rt_allocate_bool(G.num_nodes(),gm_rt_thread_id());
    int G_updated_index = base_address_index;
    bool* G_updated = new bool[G.get_num_of_local_nodes()];
    G.win_attach_bool(G_updated, G.get_num_of_local_nodes());

    //bool* G_updated_nxt; = gm_rt_allocate_bool(G.num_nodes(),gm_rt_thread_id());
    int G_updated_nxt = base_address_index;
    bool* G_updated_nxt = new bool[G.get_num_of_local_nodes()];
    G.win_attach_bool(G_updated_nxt, G.get_num_of_local_nodes());


    //int32_t* G_dist_nxt; = gm_rt_allocate_int(G.num_nodes(),gm_rt_thread_id());
    int G_dist_nxt_index = base_address_index;
    int32_t* G_dist_nxt = new int32_t[G.get_num_of_local_nodes()];
    G.win_attach_int(G_dist_nxt, G.get_num_of_local_nodes());


    fin = false ;

    //mpi for
    for (node_t t0 = 0; t0 < G.get_num_of_local_nodes(); t0 ++) 
    {
        G_dist[t0] = (t0 == root)?0:INT_MAX ;
        G_updated[t0] = (t0 == root)?true:false ;
        G_dist_nxt[t0] = G_dist[t0] ;
        G_updated_nxt[t0] = G_updated[t0] ;
    }
    while ( !fin)
    {
        bool __E8 = false ;

        fin = true ;
        __E8 = false ;

        //mpi for
        for (node_t n = 0; n < G.get_num_of_local_nodes(); n ++) 
        {
            if (G_updated[n])
            {
                for (edge_t s_idx = G.local_begin[n];s_idx < G.local_begin[n+1] ; s_idx ++)
                {
                    node_t s = G.local_node_idx [s_idx];
                    edge_t e;

                    e = s_idx ;
                    { // argmin(argmax) - test and test-and-set
                        int32_t G_dist_nxt_new = G_dist[n] + G_len[e];
                        if (G_dist_nxt[s]>G_dist_nxt_new) {
                            bool G_updated_nxt_arg = true;
                            gm_spinlock_acquire_for_node(s);
                            if (G_dist_nxt[s]>G_dist_nxt_new) {
                                G_dist_nxt[s] = G_dist_nxt_new;
                                G_updated_nxt[s] = G_updated_nxt_arg;
                            }
                            gm_spinlock_release_for_node(s);
                        }
                    }
                }
            }
        }
        #pragma omp parallel
        {
            bool __E8_prv = false ;

            __E8_prv = false ;

            //mpi for
            for (node_t t4 = 0; t4 < G.get_num_of_local_nodes(); t4 ++) 
            {
                G_dist[t4] = G_dist_nxt[t4] ;
                G_updated[t4] = G_updated_nxt[t4] ;
                G_updated_nxt[t4] = false ;
                __E8_prv = __E8_prv || G_updated[t4] ;
            }
            ATOMIC_OR(&__E8, __E8_prv);
        }
        fin =  !__E8 ;
    }

    gm_rt_cleanup();
}

