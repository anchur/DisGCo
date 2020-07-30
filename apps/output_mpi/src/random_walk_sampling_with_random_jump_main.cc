#include "common_main.h"
#include "random_walk_sampling_with_random_jump.h"
#include <sys/types.h>
#include <unistd.h>

class my_main: public main_t
{
public:

    virtual bool prepare() {
        return true;
    }

    virtual bool run() {
        int start = rand() % G.num_nodes();
        gm_node_set set(G.get_num_of_local_nodes(), G);
        random_walk_sampling_with_random_jump(G, start, 0.15, set);
        return true;
    }

    virtual bool post_process() {
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
