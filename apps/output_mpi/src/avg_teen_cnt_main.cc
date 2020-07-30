#include "common_main.h"
#include "avg_teen_cnt.h"  // defined in generated
#include <sys/types.h>
#include <unistd.h>
class my_main: public main_t
{
public:
    int* age;
    int* teen_cnt;
    int K;
    float avg;
    my_main() {
        K = 5;
    }

    virtual bool prepare() {
        age = new int[G.get_num_of_local_nodes()];
        for (int i = 0; i < G.get_num_of_local_nodes(); i++) {
        	age[i] = 10;
        }
        teen_cnt = new int[G.get_num_of_local_nodes()];
        MPI_Barrier(MPI_COMM_WORLD);
        return true;
    }

    virtual bool run() {
        avg = avg_teen_cnt(G, age, teen_cnt, K);
        MPI_Barrier(MPI_COMM_WORLD);
        return true;
    }

    virtual bool post_process() {
        //---------------------------------
        // values
        //---------------------------------
        printf("avg = %0.9lf\n", avg);
        delete[] age;
        delete[] teen_cnt;
        return true;
    }

    virtual void print_arg_info() {
        printf("[K=5]");
    }

    virtual bool check_args(int argc, char** argv) {
        if (argc > 0) {
            K = atoi(argv[0]);
            if (K <= 0) return false;
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
		my_main M;
    M.main(argc, argv);
		MPI_Finalize();
}
