ifndef PROGS
 ifeq ($(TARGET), gps)
   PROGS= pagerank avg_teen_cnt conduct hop_dist sssp random_bipartite_matching bc_random
 else
  ifeq ($(TARGET), giraph)
    PROGS= pagerank avg_teen_cnt conduct hop_dist sssp random_bipartite_matching bc_random triangle_counting_directed
  else
   ifeq ($(TARGET), cpp_mpi)
#   PROGS=sssp_dijkstra
PROGS= sssp bfs cc adamicAdar bfs_new bc bc_adj hop_dist adamicAdar2 triangle_counting triangle_counting_directed conduct pagerank avg_teen_cnt sssp_path random_node_sampling random_degree_node_sampling random_bipartite_matching random_walk_sampling_with_random_jump v_cover communities sssp_dijkstra bidir_dijkstra b2 tarjan_scc kosaraju
   else
     PROGS= sssp_path hop_dist communities bc_random triangle_counting potential_friends pagerank avg_teen_cnt conduct bc kosaraju adamicAdar v_cover sssp random_walk_sampling_with_random_jump random_degree_node_sampling random_node_sampling sssp_path_adj bc_adj sssp_dijkstra bidir_dijkstra
   endif
  endif
 endif
endif
export PROGS
