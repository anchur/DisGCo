if [ $# -ne 1 ]
then
	echo "usage: ./bglingen.sh <FILE_NAME>"
	exit
fi

FILE_NAME=$1

# Build the input generator
mpic++ -O0 -g -I../output_mpi/generated -I../output_mpi/gm_graph/inc -I. -fopenmp -DDEFAULT_GM_TOP="\"/home/anchu/research/greenmarl-to-mpi/Green-Marl-MPI/Green-Marl\"" -std=gnu++0x -DAVRO bgl_input_gen.cc ../output_mpi/gm_graph/lib/libgmgraph.a -L../output_mpi/gm_graph/lib -lgmgraph -o bglin

# Generate the bgl input graph
mpirun -n 1 ./bglin $FILE_NAME
