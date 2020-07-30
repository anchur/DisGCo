#include "common_main.h"
#include "bc.h"  // defined in generated
#include "gm_rand.h"
#include <sys/types.h>
#include <unistd.h>
class my_main: public main_t
{
public:
    gm_node_seq* Seeds;
    float* BC;

    virtual ~my_main() {
        delete[] BC;
        delete Seeds;
    }

    my_main() {
        Seeds = NULL;
        BC = NULL;
    }

    virtual bool prepare() {
        Seeds = new gm_node_seq(G);
        //Seeds->init(G);
        BC = new float[G.num_nodes()];
        return true;
    }

    virtual bool run() {
#ifdef NODE64
	gm_rand64 xorshift_rng;
#else
	gm_rand32 xorshift_rng;
#endif
        assert(Seeds != NULL);
        // pick 5 random starting points;
        for (int i = 0; i < 1; i++) {
            node_t t;
            do {
                t = xorshift_rng.rand() % G.num_nodes();
            } while (t >= G.num_nodes());

						fprintf(stderr, "seed = %d\n", t);
            Seeds->push_back(t);
        }

        comp_BC(G, BC, *Seeds);
        return true;
    }

    virtual bool post_process() {
			int i;
			for (i = 0; i < G.num_nodes(); i++) {
        printf("BC[%d] = %0.9lf\n", i, BC[i]);
			}
//        printf("BC[1] = %0.9lf\n", BC[1]);
//        printf("BC[2] = %0.9lf\n", BC[2]);
//        printf("BC[3] = %0.9lf\n", BC[3]);
//        printf("BC[4] = %0.9lf\n", BC[4]);
//        printf("BC[5] = %0.9lf\n", BC[5]);
//        printf("BC[6] = %0.9lf\n", BC[6]);
//        printf("BC[100] = %0.9lf\n", BC[100]);
//        printf("BC[101] = %0.9lf\n", BC[101]);
//        printf("BC[102] = %0.9lf\n", BC[102]);
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
