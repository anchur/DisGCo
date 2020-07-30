/*
 * =====================================================================================
 *
 *       Filename:  partition_graph.cc
 *
 *    Description:  Utilities to transform a given input graph based on a 
 *    user given partition.   
 *
 *        Created:  10/22/2019 11:26:00 IST
 *       Revision:  none
 *       Compiler:  g++
 *
 *         Author:  V. Krishna Nandivada (Krishna), nvk@cse.iitm.ac.in
 *        Company:  IIT Madras.
 *
 * =====================================================================================
 */

#include <stdio.h>
#include <stdlib.h>
#include "partition_graph.h"

/* Format of the partition file: starts with 
 * (1) Number of processors.
 * (2) List of number of vertices and their number of edges for each processor
 * (3) List of processor number for each vertex - one processor per
 * line.
 * Example:
 * Say: begin_array: 0, 1, 3, 4, 6
 *      node_idx: 2, 0, 3, 1, 0 ,2
 *
 * Say: partition file
 * 2    // two processors
 * 3 4  // indicating first processor has three nodes and four edges
 * 1 2  // indicating the that second processor gets one node and two edges.
 * 0
 * 1
 * 0
 * 0
 *
 * Effective CSR file:
 *    : begin_array: 0, 2, 3, 5, 6
 *    : begin_array: 0, 4, 3, 1, 6 -- wrong
 *      node_idx: 2, 0, 2, 1, 0 ,3 -- wrong
 *      node_idx: 2, 0, 2, 3, 0, 1
 *
 */

//anchu commented the code below.
//int *block_size; // blocks per process can vary.

/* Make at more elaborate if you want later. */
static int errorReading(int r, char *s){
	fprintf(stderr, "Error in reading partition file %s, code = %d \n", s, r);
	return r;
}

/* Reads the block sizes, and starting indexes for begin and node_index
 * arrays of vertices in each 
 * Returns: 0 if successful
 * 	    non-zero if unsuccessful 
 */

static FILE *fp;
int read_partition_summary(
    int **block_size,
    int **node_block_index, int **edge_block_index, int *numPartitions)
{
	static int start_point; // starting point where the processor indices start.
	int numP;
	if (start_point != 0){
		fseek (fp, SEEK_SET, start_point);
		return 0;
	}
		
	char *s = getenv("PARTITION_FILE");
	if (s == NULL){// no partition specified.
		return -1; 
	}
	fp = fopen (s,"r");
	if (fp == NULL) {
		fprintf(stderr, "Error in opening partition file %s \n", s);
		return 1;
	}
  printf("partition file opened for reading\n");
    	//if (fscanf(fp, "%d",&numV) <=0) return errorReading(1);

    	if (fscanf(fp, "%d",&numP) <=0) return errorReading(2, s);
      *numPartitions = numP;

	int *nbi = *node_block_index = (int *)calloc (numP,sizeof(int));
	int *ebi = *edge_block_index = (int *)calloc (numP,sizeof(int));
	int *bs  = *block_size = (int *)malloc (numP*sizeof(int));
	int *ebs  =  (int *)malloc (numP*sizeof(int));

	for (int i=0;i<numP;++i){
		if (fscanf(fp,"%d",&bs[i]) <= 0) errorReading(3, s); //3, 1 
		if (fscanf(fp,"%d",&ebs[i]) <= 0) errorReading(3, s); // 4, 2
		if (i!= 0) {
			nbi[i]  = bs [i-1] + nbi [i-1];
			ebi[i]  = ebs [i-1] + ebi [i-1];
		}
  }
		//if (i!= 0)
		//	nbi[i]  = bs [i] + nbi [i-1];
		//	ebi[i] += ebi [i-1];
  //anchu commented the below line
	start_point = ftell (fp);
	return 0;
}
// Read and return the processor index of the next vertex.
int nextProcIndex(){
	int p;
 	fscanf(fp, "%d",&p);
	return p;
}
