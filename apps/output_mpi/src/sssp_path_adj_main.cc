#include "common_main.h"
#include "sssp_path_adj.h"
#include "gm_rand.h"
#include <stdio.h>
#include <sys/types.h>
#include <unistd.h>

class my_main: public main_t
{
private:
    double* dist;
    double* len;
    node_t* prev;
    gm_node_seq *SEQ;
    node_t begin;
    node_t end;
    node_t* prev_nodes;
    edge_t* prev_edges;
public:
    virtual ~my_main() {
        delete[] dist;
        delete[] len;
        delete[] prev;
    }

    virtual bool prepare() {
      SEQ = new gm_node_seq(G);
        
        dist = new double[G.get_num_of_local_nodes()];
        len = new double[G.get_num_of_local_forward_edges()];
        prev = new node_t[G.get_num_of_local_nodes()];
        prev_nodes = new node_t[G.get_num_of_local_nodes()];
        prev_edges = new edge_t[G.get_num_of_local_forward_edges()];


	    gm_rand32 xorshift_rng;
        for (edge_t i = 0; i < G.get_num_of_local_forward_edges(); i++)
            len[i] = (double)((xorshift_rng.rand() % 10000) + 1)/100;  // length: 1 ~ 100
        return true;
    }

    virtual bool run() {
        node_t root = rand() % G.num_nodes();
        begin = root;
        end = rand() % G.num_nodes();
        sssp_path(G, dist, len, begin , end, prev_nodes, prev_edges);
        MPI_Barrier(MPI_COMM_WORLD);
        // get specific instance from root to end
        double total_cost = get_path(G, begin, end, prev_nodes, prev_edges, len, *SEQ);
        MPI_Barrier(MPI_COMM_WORLD);
        return true;
    }

    virtual bool post_process() {
        printf("shortest path from %d to %d\n", begin, end);
        if(G.get_rank() == MASTER)
        {
          gm_node_seq::seq_iter n_I = SEQ->prepare_seq_iteration();
          while (n_I.has_next())
          {
            node_t n = n_I.get_next();
            printf("%d", n);
            if(n_I.has_next())
              printf(" -> ");
            else
              printf("\n");
          }
        }
        return true;
    }
};
static void wait_for_debugger ()
{
	volatile  int i=0;
	fprintf(stderr , "pid %ld  waiting  for  debugger\n"
			, (long)getpid ());
	while(i==0) { /*  change  'i' in the  debugger  */ }
	MPI_Barrier(MPI_COMM_WORLD);
}



int main(int argc, char** argv) {
		int provided;
		MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  	//wait_for_debugger();
    my_main M;
    M.main(argc, argv);
    MPI_Barrier(MPI_COMM_WORLD);
		MPI_Finalize();
}
