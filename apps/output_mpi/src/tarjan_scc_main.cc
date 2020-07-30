#include "common_main.h"
#include "tarjan_scc.h"
#include "gm_rand.h"
#include <sys/types.h>
#include <unistd.h>

class my_main: public main_t
{
public:
    int32_t* scc;

    virtual ~my_main() {
        delete[] scc;
    }

    //--------------------------------------------------------
    // create 4 groups randomly
    //--------------------------------------------------------
    virtual bool prepare() {
	    gm_rand32 xorshift_rng;
        scc = new int[G.get_num_of_local_nodes()];

				MPI_Barrier(MPI_COMM_WORLD);
        return true;
    }

    virtual bool run() {
        Tarjan(G, scc);
				MPI_Barrier(MPI_COMM_WORLD);
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
  	wait_for_debugger();
		my_main M;
    M.main(argc, argv);
		MPI_Finalize();
}
