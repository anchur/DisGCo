#include "common_main.h"
#include "bfs.h"
#include "gm_rand.h"
#include <sys/types.h>
#include <unistd.h>

class my_main: public main_t
{
public:
    int32_t* dist;  // distance of each node
    node_t root;

		char *edge_weight_file_name;

    virtual ~my_main() {
        delete[] dist;
    }

		virtual bool prepare() {
			root = 0;
			dist = new int[G.get_num_of_local_nodes()];
			return true;
		}

    virtual bool run() {
      bfs(G, dist, root);
			MPI_Barrier(MPI_COMM_WORLD);
        return true;
    }

    virtual bool post_process() {
        //---------------------------------
        // dist values of 10 nodes
        //---------------------------------
     //printf("lnn: %d rank: %d", G.get_num_of_local_nodes(), G.get_rank()); 
        for (int i = 0; i < 10; i++) {
          printf("rank: %d dist[%d] = %d\n", G.get_rank(), G.get_rank()*G.get_block_size()+i, dist[i]);
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
		int provided;
		MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  	//wait_for_debugger();
		my_main M;


    M.main(argc, argv);
		MPI_Finalize();
}
