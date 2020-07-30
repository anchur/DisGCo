#include "common_main.h"
#include "triangle_counting.h"
#include <sys/types.h>
#include <unistd.h>

class my_main: public main_t
{
public:
    int tCount;

    virtual bool prepare() {
        return true;
    }

    virtual bool run() {
        tCount = triangle_counting(G);
        return true;
    }

    virtual bool post_process() {
        printf("number of triangles: %d\n", tCount);
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
