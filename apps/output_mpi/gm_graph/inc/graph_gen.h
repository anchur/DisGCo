#ifndef GRAPH_GEN_H_
#define GRAPH_GEN_H_

#include "gm_graph.h"

gm_graph* create_uniform_random_graph(node_t N, edge_t M, long seed, bool use_xorshift_rng);
gm_graph* create_uniform_random_graph2(node_t N, edge_t M, long seed);
gm_graph* create_uniform_random_nonmulti_graph(node_t N, edge_t M, long seed);
gm_graph* create_graph_from_gr(char* in_graph_filename, char* out_weights_filename); 
gm_graph* create_graph_for_twt(char* in_graph_filename, char* out_weights_filename, int N, char* out_dh_falcon_file);

#endif /* GRAPH_GEN_H_ */
