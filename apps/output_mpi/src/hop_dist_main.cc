#include "common_main.h"
#include "hop_dist.h"
#include "gm_rand.h"
#include <sys/types.h>
#include <unistd.h>

class my_main: public main_t
{
public:
    int32_t* dist;  // distance of each node
    node_t root;

    virtual ~my_main() {
        delete[] dist;
    }

    //--------------------------------------------------------
    // create 4 groups randomly
    //--------------------------------------------------------
    virtual bool prepare() {
	    gm_rand32 xorshift_rng;
        root = 0;
        dist = new int[G.get_num_of_local_nodes()];

        return true;
    }

    virtual bool run() {
        hop_dist(G, dist, root);
        MPI_Barrier(MPI_COMM_WORLD);
        return true;
    }

    virtual bool post_process() {
        //---------------------------------
        // dist values of 10 nodes
        //---------------------------------
        for (int i = 0; i < 10; i++) {
            printf("dist[%d] = %d\n", i, dist[i]);
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
		MPI_Finalize();
}
