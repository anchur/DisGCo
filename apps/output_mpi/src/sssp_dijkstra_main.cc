
#include "common_main.h"
#include "sssp_dijkstra.h"
#include "gm_rand.h"
#include <sys/types.h>
#include <unistd.h>

class my_main: public main_t
{
  public:
    node_t* prev_nodes;
    edge_t* prev_edges;
    int32_t* edge_costs;

    char *edge_weight_file_name;

    virtual ~my_main() {
      delete[] edge_costs;
      delete[] prev_nodes;
      delete[] prev_edges;

    }

    virtual bool prepare() {
      prev_nodes = new node_t[G.num_nodes()];
      prev_edges = new edge_t[G.num_nodes()];
      int *temp_edge_costs = new int[G.num_edges()];
      edge_costs= new int[G.get_num_of_local_forward_edges()];
      int G_edge_costs_index = base_address_index;
      G.win_attach_int(edge_costs, G.get_num_of_local_forward_edges());

      if (G.get_rank() == MASTER) {
        fprintf(stderr, "Weights file = %s\n", edge_weight_file_name);
        FILE *fp;
        if ((fp = fopen(edge_weight_file_name, "r")) == NULL) {
          fprintf(stderr, "Error in opening file %s\n", edge_weight_file_name);
          exit(0);
        }

        int proc_id = 0;
        int i;
        for (proc_id = 0; proc_id < G.get_num_processes(); proc_id++) {
          int edge_array_size;
          if (proc_id < (G.get_num_processes() - 1)) {
            edge_array_size = G.global_forward_edge_start_for_all[proc_id + 1]
              - G.global_forward_edge_start_for_all[proc_id];
          } else {
            edge_array_size = G.num_edges() - G.global_forward_edge_start_for_all[proc_id];
          }

          int *tmp_edge_array = new int[edge_array_size];
          int j;
          for (j = 0; j < edge_array_size; j++) {
            if(feof(fp)) {
              fprintf(stderr, "Error within the weights file\n");
              exit(0);
            }
            fscanf(fp, "%d", &tmp_edge_array[j]);
          }
          lock_and_put_array(tmp_edge_array, proc_id, edge_array_size, G_edge_costs_index); 
          delete tmp_edge_array;
        }
        fclose(fp);
      }
      MPI_Barrier(MPI_COMM_WORLD);
      MPI_Win_detach(win, edge_costs);
      base_address_index--;
      return true;
    }

    virtual bool run() {
      node_t src_node_id = 1;
      node_t dst_node_id = 2;
      dijkstra(G, edge_costs, src_node_id, dst_node_id, prev_nodes, prev_edges);
      MPI_Barrier(MPI_COMM_WORLD);
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
	  if (argc != 3) {
			fprintf(stderr, "Usage: mpirun -n <num_processes> ./sssp <graph_name> <edge_weight_file_name>\n");
			exit(0);
		}
		int provided;
		MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  	//wait_for_debugger();
		my_main M;

		M.edge_weight_file_name = argv[argc - 1];
		// Originally M.main was expecting argc to be 2.
		argc--;

    M.main(argc, argv);
		MPI_Finalize();
}
