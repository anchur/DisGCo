#include "pagerank.h"

void pagerank(gm_graph& G, double e, 
    double d, int32_t max, 
    double*G_pg_rank)
{
    //Initializations
    gm_rt_initialize();
    G.freeze();
    G.make_reverse_edges();


    int G_pg_rank_index = base_address_index;
    G.win_attach_double(G_pg_rank, G.get_num_of_local_nodes());
    double diff = 0.0 ;
    int32_t cnt = 0 ;
    double N = 0.0 ;
    double* G_pg_rank_nxt = new double[G.get_num_of_local_nodes()];
    int G_pg_rank_nxt_index = base_address_index;
    G.win_attach_double(G_pg_rank_nxt, G.get_num_of_local_nodes());

    cnt = 0 ;
    N = (double)(G.num_nodes()) ;

    //mpi for
    for (node_t t0 = 0; t0 < G.get_num_of_local_nodes(); t0 ++) 
        G_pg_rank[t0] = 1 / N ;

    MPI_Barrier(MPI_COMM_WORLD);
    do
    {
        diff = ((float)(0.000000)) ;
        {
            double diff_prv = 0.0 ;

            diff_prv = ((float)(0.000000)) ;

            //mpi for
            for (node_t t = 0; t < G.get_num_of_local_nodes(); t ++) 
            {
                double val = 0.0 ;
                double __S1 = 0.0 ;

                __S1 = ((float)(0.000000)) ;
                for (edge_t w_idx = G.local_r_begin[t];w_idx < G.local_r_begin[t+1] ; w_idx ++) 
                {
                    node_t w = G.local_r_node_idx [w_idx];
                    double G_pg_rank_tmp1;
                    int r_1;
                    int ln_1;
                    r_1= G.get_rank_node(w);
                    ln_1= G.get_local_node_num(w);
                    lock_and_get_node_property_double(G_pg_rank_tmp1,G_pg_rank_index,r_1,ln_1);
                    __S1 = __S1 + G_pg_rank_tmp1 / ((double)(/*1*/(G.local_begin[w+1] - G.local_begin[w]))) ;
                }
                val = (1 - d) / N + d * __S1 ;
                diff_prv = diff_prv +  std::abs((val - G_pg_rank[t]))  ;
                G_pg_rank_nxt[t] = val ;
            }
            MPI_Barrier(MPI_COMM_WORLD);
            MPI_ADD<double>(&diff, diff_prv);
        }

        //mpi for
        for (node_t i3 = 0; i3 < G.get_num_of_local_nodes(); i3 ++) 
            G_pg_rank[i3] = G_pg_rank_nxt[i3] ;

        MPI_Barrier(MPI_COMM_WORLD);
        cnt = cnt + 1 ;
    }
    while ((diff > e) && (cnt < max));


    gm_rt_cleanup();
}

