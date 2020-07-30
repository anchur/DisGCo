#ifndef COMMON_MAIN_H
#define COMMON_MAIN_H

#include <omp.h>
#include <mpi.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include "gm.h"

#define MASTER 0

class bgl_input_gen_t
{
protected:
#ifdef __HDFS__
    gm_graph_hdfs G;
    gm_graph_hdfs& get_graph() {
        return G;
    }
#else
    gm_graph G;
    gm_graph& get_graph() {
        return G;
    }
#endif

public:
    virtual ~bgl_input_gen_t() {
    }

    bgl_input_gen_t() {
    }

    virtual void main(int argc, char** argv) {
        bool b;

        // check if node/edge size matches with the library (runtime-check)
        gm_graph_check_node_edge_size_at_link_time();
        
				//mpi
				struct timeval T1, T2;
				int rank = G.get_rank();
				if (rank == MASTER) {
				//mpi
					if (argc < 2) {

						printf("%s <graph_name> ", argv[0]);
						//print_arg_info();
						printf("\n");

						exit (EXIT_FAILURE);
					}

					int new_argc = argc - 2;
					char** new_argv = &(argv[2]);
					//b = check_args(new_argc, new_argv);
					//if (!b) {
					//	printf("error procesing argument\n");
					//	printf("%s <graph_name> ", argv[0]);
					//	print_arg_info();
					//	printf("\n");
					//	exit (EXIT_FAILURE);
					//}

					//int num = atoi(argv[2]);
					//printf("running with %d threads\n", num);
					//gm_rt_set_num_threads(num); // gm_runtime.h

					//--------------------------------------------
					// Load graph anc creating reverse edges
					//--------------------------------------------
					char *fname = argv[1];
					gettimeofday(&T1, NULL);
					b = G.load_binary(fname);
					if (!b) {
						printf("error reading graph\n");
						exit (EXIT_FAILURE);
					}
					gettimeofday(&T2, NULL);
					printf("graph loading time=%lf\n", (T2.tv_sec - T1.tv_sec) * 1000 + (T2.tv_usec - T1.tv_usec) * 0.001);

					gettimeofday(&T1, NULL);
					G.make_reverse_edges();
					gettimeofday(&T2, NULL);
					printf("reverse edge creation time=%lf\n", (T2.tv_sec - T1.tv_sec) * 1000 + (T2.tv_usec - T1.tv_usec) * 0.001);
				//mpi
				}
				//mpi
				G.generate_input_for_bgl();
    }
};

#endif

int main(int argc, char** argv) {
	  if (argc != 2) {
			fprintf(stderr, "Usage: mpirun -n <num_processes> ./bglinputgen <graph_name>\n");
			exit(0);
		}
		int provided;
		MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  	//wait_for_debugger();
		bgl_input_gen_t M;

    M.main(argc, argv);
		MPI_Finalize();
}

