#include "gm_common_neighbor_iter.h"

//mpi
extern MPI_Aint *base_address;
extern MPI_Win win;
//mpi


gm_common_neighbor_iter::gm_common_neighbor_iter(gm_graph& _G, node_t s, node_t d) :
        G(_G), src(s), dest(d) {
    // Get the begin array values of src and src + 1.
		get_begin_start_end(src, src_begin, src_end, G);
    // Get the begin array values of dest and dest + 1.
		get_begin_start_end(dest, dest_begin, dest_end, G);

		num_src_nghbrs = src_end - src_begin;
		num_dest_nghbrs = dest_end - dest_begin;
		// Get the node_idx values of src (neighbors of src)
   	src_nghbrs = (node_t *)calloc(num_src_nghbrs, sizeof(node_t));
		lock_and_get_node_idx(src_nghbrs[0], src, src_begin, num_src_nghbrs, G);
		// Get the node_idx values of src (neighbors of src)
   	dest_nghbrs = (node_t *)calloc(num_dest_nghbrs, sizeof(node_t));
		lock_and_get_node_idx(dest_nghbrs[0], dest, dest_begin, num_dest_nghbrs, G);

		// Find the common neighbors.
		find_common_nghbrs();
		// TODO: Check if reset is needed or not?
		reset();
}

void gm_common_neighbor_iter::find_common_nghbrs() {
	int i, j;

	// TODO: Implement second loop as binary search because
	// the arrays are sorted.
	for(i = 0; i < num_src_nghbrs; i++) {
		for (j = 0; j < num_dest_nghbrs; j++) {
			if (src_nghbrs[i] == dest_nghbrs[j])
				common_nghbrs.push_back(src_nghbrs[i]);
		}
	}
}

void gm_common_neighbor_iter::reset() {
	common_nghbrs_itr = common_nghbrs.begin();
}

node_t gm_common_neighbor_iter::get_next() {
    if (common_nghbrs_itr == common_nghbrs.end())
			return gm_graph::NIL_NODE;
		node_t t = (node_t)(*common_nghbrs_itr);
		common_nghbrs_itr++;
		return t;
}

bool gm_common_neighbor_iter::check_common(node_t t) {
    while (true) {
        node_t r = G.node_idx[dest_idx];
        if (r == t) return true;
        if (r > t) return false;
        dest_idx++;
        if (dest_idx == dest_end) {
            finished = true;
            return false;
        }
    }
    return false;
}

