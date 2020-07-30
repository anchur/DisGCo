#ifndef GM_CPPLIB_WORDS
#define GM_CPPLIB_WORDS
static const char* NODE_IDX = "local_node_idx";
static const char* EDGE_IDX = "edge_idx";
static const char* R_NODE_IDX = "local_r_node_idx";
static const char* R_EDGE_IDX = "r_edge_idx";
static const char* BEGIN = "local_begin";
static const char* R_BEGIN = "local_r_begin";
static const char* NUM_NODES = "num_nodes";
static const char* NUM_EDGES = "num_edges";
//mpi
static const char* NUM_LOCAL_NODES = "get_num_of_local_nodes";
static const char* NUM_LOCAL_EDGES = "get_num_of_local_forward_edges";
//mpi
static const char* RANDOM_NODE = "pick_random_node";

static const char* NODE_T = "node_t";
static const char* EDGE_T = "edge_t";
static const char* NODEITER_T = "node_t"; // [todo] clarify later --> to be removed
static const char* EDGEITER_T = "edge_t";
static const char* GRAPH_T = "gm_graph";
static const char* SET_T = "gm_node_set";
static const char* ORDER_T = "gm_node_order";
static const char* SEQ_T = "gm_node_seq";
static const char* QUEUE_T = "gm_collection";
static const char* MAP_T = "gm_map";
static const char* PROP_OF_COL = "gm_property_of_collection";
static const char* IS_IN = "is_in";
static const char* MAX_SET_CNT = "max_size";
static const char* GET_LIST = "get_list";
static const char* FROM_IDX = "local_node_idx_src";
static const char* FW_EDGE_IDX = "local_e_rev2idx";
static const char* ORG_EDGE_IDX = "local_e_idx2idx";
static const char* GET_ORG_IDX = "get_org_edge_idx";

#endif
