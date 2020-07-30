#include <mpi.h>
#include "common_main.h"
#include "b2.h"
#include <unistd.h>

void P(gm_graph& G, int32_t* G_A, int32_t* G_B, node_t& k);

class b2_main: public main_t
{
public:
    int32_t* A;
    int32_t* B;
    node_t c;

    virtual ~b2_main() {
        delete[] A;
        delete[] B;
    }

    virtual bool prepare() {
        A = new int32_t[G.num_nodes()];
        B = new int32_t[G.num_nodes()];
        c = 2;
        return true;
    }

    virtual bool run() {
      P(G, A, B, c);
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
    b2_main M;
    M.main(argc, argv);
		MPI_Finalize();
}

