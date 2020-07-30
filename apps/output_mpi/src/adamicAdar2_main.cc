#include "common_main.h"
#include "adamicAdar2.h"  // defined in generated
#include <unistd.h>
class aa_main: public main_t
{
public:
    double* aa; // edge property
    int method;

    ~aa_main() {
        delete[] aa;
    }

    aa_main() {
        method = 0;
        aa = NULL;
    }

    virtual bool prepare() {
        aa = new double[G.num_edges()];
        return true;
    }

    virtual bool run() {
        //if (method == 0)
            adamicAdar2(G, aa);
        //else
        //    adamicAdar2(G, aa);
        return true;
    }

    virtual void print_arg_info() {
        //printf("[usemethod=0/1]");
    }

    virtual bool check_args(int argc, char** argv) {
        if (argc > 0) method = atoi(argv[0]);
        return true;
    }

    virtual bool post_process() {
        int max_cnt = 0;
        for (int i = 0; i < G.num_edges(); i++) {
            //if (aa[i] != 0) {
                printf("%d-> %f\n", i, aa[i]);
                //if (max_cnt++ == 100) break;
            //}
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
    aa_main M;
    M.main(argc, argv);
		MPI_Finalize();
}

