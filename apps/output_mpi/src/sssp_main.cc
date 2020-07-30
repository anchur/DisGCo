#include "common_main.h"
#include "sssp.h"
#include "gm_rand.h"
#include <sys/types.h>
#include <unistd.h>

class my_main: public main_t
{
public:
    int32_t* len; // length of each edge
    int32_t* dist;  // distance of each node
    node_t root;

		char *edge_weight_file_name;

    virtual ~my_main() {
        delete[] len;
        delete[] dist;
    }

    //--------------------------------------------------------
    // create 4 groups randomly
    //--------------------------------------------------------
/*    virtual bool prepare() {
	    gm_rand32 xorshift_rng;
        root = 0;
        dist = new int[G.get_num_of_local_nodes()];
        len = new int[G.get_num_of_local_forward_edges()];

        // for NUMA, let each thread touch it first
        //#pragma omp parallel for
        for (node_t i = 0; i < G.get_num_of_local_nodes(); i++)
            for (edge_t j = G.local_begin[i]; j < G.local_begin[i+1]; j++)
                len[j] = 0;
				srandom(G.get_rank());
        for (edge_t i = 0; i < G.get_num_of_local_forward_edges(); i++)
				{
            //len[i] = (xorshift_rng.rand() % 100) + 1;  // length: 1 ~ 100
            len[i] = (random() % 100) + 1;  // length: 1 ~ 100
						printf("rank: %d len: %d dest: %d\n",G.get_rank(), len[i], G.local_node_idx[i]);
				}
			MPI_Barrier(MPI_COMM_WORLD);
        return true;
    }
*/

		virtual bool prepare() {
			root = 0;
			dist = new int[G.get_num_of_local_nodes()];
			int *temp_len = new int[G.num_edges()];
			len = new int[G.get_num_of_local_forward_edges()];
			int G_len_index = base_address_index;
			G.win_attach_int(len, G.get_num_of_local_forward_edges());

			if (G.get_rank() == MASTER) {
				//fprintf(stderr, "Weights file = %s\n", edge_weight_file_name);
				FILE *fp;
				if ((fp = fopen(edge_weight_file_name, "r")) == NULL) {
					fprintf(stderr, "Error in opening file %s\n", edge_weight_file_name);
					exit(0);
				}
        {
          int i;
          for(i=0; i<G.num_edges(); i++) {
            fscanf(fp,"%d",&temp_len[G.emap[i]]);
            printf("%d\n", G.emap[i]);
          }
          for(i=0; i<G.num_edges(); i++) {
            printf("len[%d] = %d\n",i,temp_len[i]);
          }
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
            tmp_edge_array[j] = temp_len[G.global_forward_edge_start_for_all[proc_id]+j];
					}
					lock_and_put_array(tmp_edge_array, proc_id, edge_array_size, G_len_index); 
					delete tmp_edge_array;
				}
				fclose(fp);
			}
			MPI_Barrier(MPI_COMM_WORLD);
      //G.generate_input_for_dhfalcon(edge_weight_file_name);
			//for (edge_t i = 0; i < G.get_num_of_local_forward_edges(); i++)
			//{
			//	//len[i] = (xorshift_rng.rand() % 100) + 1;  // length: 1 ~ 100
			//	//len[i] = (random() % 100) + 1;  // length: 1 ~ 100
			//	printf("rank: %d len: %d dest: %d\n",G.get_rank(), len[i], G.local_node_idx[i]);
			//}
			MPI_Win_detach(win, len);
			base_address_index--;
			return true;
		}

    virtual bool run() {
      sssp(G, dist, len, root);
			MPI_Barrier(MPI_COMM_WORLD);
        return true;
    }

    virtual bool post_process() {
        //---------------------------------
        // dist values of 10 nodes
        //---------------------------------
     //printf("lnn: %d rank: %d", G.get_num_of_local_nodes(), G.get_rank()); 
      int i;
      for(i=0; i<G.get_num_of_local_nodes(); i++) {
       printf("dist[%d] = %d\n",G.get_global_node_num(i), dist[i]);
      }
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
    //MPI_Init(&argc, &argv);
		MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  	//wait_for_debugger();
		my_main M;

		M.edge_weight_file_name = argv[argc - 1];
		// Originally M.main was expecting argc to be 2.
		argc--;

    M.main(argc, argv);
		MPI_Finalize();
}
