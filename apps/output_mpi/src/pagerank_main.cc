#include "common_main.h"
#include "pagerank.h"  // defined in generated
#include <sys/types.h>
#include <unistd.h>
class my_main: public main_t
{
public:
    double* rank;
    int max_iter;
    double e;
    double d;

    my_main() {
        e = 0.001;
        d = 0.85;
        max_iter = 100;
        rank = NULL;
    }

    virtual bool prepare() {
        rank = new double[G.get_num_of_local_nodes()];
        return true;
    }

    virtual bool run() {
        pagerank(G, e, d, max_iter, rank);
        return true;
    }

    virtual bool post_process() {
        //---------------------------------
        // values
        //---------------------------------
        for (int i = 0; i < G.get_num_of_local_nodes(); i++) {
            printf("rank[%d] = %0.9lf\n", G.get_global_node_num(i), rank[i]);
        }
        delete[] rank;
        return true;
    }

    virtual void print_arg_info() {
        printf("[max_iteration=100] [eplision=0.001] [delta=0.85]");
    }

    virtual bool check_args(int argc, char** argv) {
        if (argc > 0) {
            max_iter = atoi(argv[0]);
            if (max_iter <= 0) return false;
        }
        if (argc > 1) {
            e = atof(argv[1]);
            if (e <= 0) return false;
        }
        if (argc > 2) {
            d = atof(argv[2]);
            if (d <= 0) return false;
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
    wait_for_debugger();
    my_main M;
    M.main(argc, argv);
		MPI_Finalize();
}

