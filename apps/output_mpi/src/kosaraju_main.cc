#include "common_main.h"
#include "kosaraju.h"  // defined in generated
#include <sys/types.h>
#include <unistd.h>

class my_main: public main_t
{
public:
    int* membership;
    int  num_membership;

    virtual ~my_main() {
        delete[] membership;
    }

    my_main() : membership(NULL), num_membership(0) {
    }

    virtual bool prepare() {
        membership = new int[G.get_num_of_local_nodes()];
        num_membership = 0;
        return true;
    }

    virtual bool run() 
    {
        num_membership = kosaraju(G, membership);
        MPI_Barrier(MPI_COMM_WORLD);
        return true;
    }

    virtual bool post_process() {
        printf("num_membership = %d\n", num_membership);
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
