#include "common_main.h"
#include "bfs_new.h"  // defined in generated
#include "gm_rand.h"
#include <sys/types.h>
#include <unistd.h>
class my_main: public main_t
{
public:
   int p;

    virtual ~my_main() {
    }

    my_main() {
    }

    virtual bool prepare() {
        return true;
    }

    virtual bool run() {
      char a[100] = "anchu";
      G.create_cyclic_distribution(p, a);
			//node_t source = 0;
      //bfs(G,source);  
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
  	//wait_for_debugger();
		my_main M;
    M.p = atoi(argv[argc-1]);
    argc--;

    M.main(argc, argv);

		//MPI_Finalize();
}