#include "common_main.h"
#include "conduct.h"
#include "gm_rand.h"
#include <sys/types.h>
#include <unistd.h>

class my_main: public main_t
{
public:
    int32_t* membership;
    double C;

    ~my_main() {
        delete[] membership;
    }

    //--------------------------------------------------------
    // create 4 groups randomly
    //--------------------------------------------------------
    virtual bool prepare() {
        membership = new int32_t[G.get_num_of_local_nodes()];
	gm_rand32 xorshift_rng;
        
        // For NUMA architectures, let each thread touch it first.
        #pragma parallel for 
        for (int i = 0; i < G.get_num_of_local_nodes(); i++) 
            membership[i] = 0;

        for (int i = 0; i < G.get_num_of_local_nodes(); i++) {
	    int32_t r = xorshift_rng.rand() % 100;
            if (r < 10)
                membership[i] = 0;  // 10%
            else if (r < (10 + 20))
                membership[i] = 1;  // 20%
            else if (r < (10 + 20 + 30))
                membership[i] = 2;  // 30%
            else
                membership[i] = 3;  // 40%
        }
        return true;
    }

    virtual bool run() {
        C = 0;
        for (int i = 0; i < 4; i++)
            C += conduct(get_graph(), membership, i);

        return true;
    }

    virtual bool post_process() {
        //---------------------------------
        // values
        //---------------------------------
        printf("sum C = %lf\n", C);
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
