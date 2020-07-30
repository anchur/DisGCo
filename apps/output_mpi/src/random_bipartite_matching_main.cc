#include "common_main.h"
#include <mpi.h>
#include "random_bipartite_matching.h"
#include <sys/types.h>
#include <unistd.h>

class my_main: public main_t
{
  bool *isLeft;
  node_t * Match;
public:

    virtual bool prepare() {
      int i;
      int num_nghbrs;
      isLeft = new bool[G.get_num_of_local_nodes()];
      Match = new node_t[G.get_num_of_local_nodes()];
      for(i=0; i<G.get_num_of_local_nodes(); i++)
      {
        isLeft[i] = 0;
        num_nghbrs = G.local_begin[i+1]-G.local_begin[i];

        if(num_nghbrs > 0)
          isLeft[i] = 1;
      }
      MPI_Barrier(MPI_COMM_WORLD);

        return true;
    }

    virtual bool run() {
      random_bipartite_matching(G, isLeft, Match);

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
