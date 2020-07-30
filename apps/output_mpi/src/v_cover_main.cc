
#include "common_main.h"
#include "v_cover.h"
#include <sys/types.h>
#include <unistd.h>

class my_main: public main_t
{
public:
    bool* selected;
    int covered;

    ~my_main() {
        delete[] selected;
    }

    //--------------------------------------------------------
    // create 4 groups randomly
    //--------------------------------------------------------
    virtual bool prepare() {
        selected = new bool[G.get_num_of_local_forward_edges()];
        MPI_Barrier(MPI_COMM_WORLD);
        return true;
    }

    virtual bool run() {
        covered = v_cover(G, selected);
        MPI_Barrier(MPI_COMM_WORLD);
        return true;
    }

    virtual bool post_process() {
        //---------------------------------
        // values
        //---------------------------------
        printf("covered (may be non-deterministic) = %d\n", covered);
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
