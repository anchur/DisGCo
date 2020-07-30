#include "common_main.h"
#include "communities.h"
#include <sys/types.h>
#include <unistd.h>

class my_main: public main_t
{
public:
    node_t* comm;

    virtual ~my_main() {
        delete[] comm;
    }

    virtual bool prepare() {
        comm = new node_t[G.get_num_of_local_nodes()];
        return true;
    }

    virtual bool run() {
        communities(G, comm);
        return true;
    }

    virtual bool post_process() {
        int* commCount = new int[G.num_nodes()];
        for (int i = 0; i < G.get_num_of_local_nodes(); i++)
            commCount[i] = 0;
        for (int i = 0; i < G.get_num_of_local_nodes(); i++)
            commCount[comm[i]]++;
        printf("Community\t#Nodes\t\t(showing max 10 entries)\n");
        for (int i = 0, x = 0; i < G.get_num_of_local_nodes(); i++)
            if (commCount[i] > 0) {
                printf("%d\t\t%d\n", i, commCount[i]);
                x++;
            }
        delete[] commCount;
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

