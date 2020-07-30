#ifndef GM_GRAPH_H_
#define GM_GRAPH_H_
#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <map>
#include <vector>
#include <stdlib.h>
#include <mpi.h>
#include <omp.h>
#include <string>
#include <unordered_map>

#include "gm_internal.h"

typedef node_t node_id;
typedef edge_t edge_id;

#define lock_and_put_array(_array_name, _rank, _count, _ba_index)\
{\
	int _err_val;\
	_err_val=MPI_Win_lock(MPI_LOCK_EXCLUSIVE,_rank, 0, win);\
	if(_err_val != MPI_SUCCESS)\
	fprintf(stderr,"error occured for lock with val%d",_err_val);\
	MPI_Put(_array_name, _count, MPI_INT, _rank, base_address[G.get_num_processes()*_ba_index+_rank], _count, MPI_INT, win);\
	MPI_Win_unlock(_rank, win);\
}


#define lock_and_get_begin(_result, _node, _count, G)\
{\
	int _begin_index = G.get_local_node_num(_node);\
	int _ba_index = G.get_local_begin_index();\
	int _rank = G.get_rank_node(_node);\
	int _err_val;\
	_err_val=MPI_Win_lock(MPI_LOCK_EXCLUSIVE,_rank, 0, win);\
	if(_err_val != MPI_SUCCESS)\
	fprintf(stderr,"error occured for lock with val%d",_err_val);\
	MPI_Get(&_result, _count, MPI_INT, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_begin_index*sizeof(int), _count, MPI_INT, win);\
	MPI_Win_unlock(_rank, win);\
}

#define lock_and_get_r_begin(_result, _node, _count, G)\
{\
	int _begin_index = G.get_local_node_num(_node);\
	int _ba_index = G.get_local_r_begin_index();\
  fprintf(stderr,"_ba_index: %d\n", _ba_index);\
	int _rank = G.get_rank_node(_node);\
	int _err_val;\
	_err_val=MPI_Win_lock(MPI_LOCK_EXCLUSIVE,_rank, 0, win);\
	if(_err_val != MPI_SUCCESS)\
	fprintf(stderr,"error occured for lock with val%d",_err_val);\
  fprintf(stderr,"base_address_of_r_begin: %x\n",base_address[_ba_index]);\
	MPI_Get(&_result, _count, MPI_INT, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_begin_index*sizeof(int), _count, MPI_INT, win);\
	MPI_Win_unlock(_rank, win);\
}

#define lock_and_get_node_idx(_result, _node, _node_idx_index, _count, G)\
{\
	int _ba_index = G.get_local_node_idx_index();\
	int _rank = G.get_rank_node(_node);\
	int _err_val;\
	_err_val=MPI_Win_lock(MPI_LOCK_EXCLUSIVE,_rank, 0, win);\
	if(_err_val != MPI_SUCCESS)\
	fprintf(stderr,"error occured for lock with val%d",_err_val);\
	MPI_Get(&_result, _count, MPI_INT, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_node_idx_index*sizeof(int), _count, MPI_INT, win);\
	MPI_Win_unlock(_rank, win);\
}

#define lock_and_get_r_node_idx(_result, _node, _node_idx_index, _count, G)\
{\
	int _ba_index = G.get_local_r_node_idx_index();\
	int _rank = G.get_rank_node(_node);\
	int _err_val;\
	_err_val=MPI_Win_lock(MPI_LOCK_EXCLUSIVE,_rank, 0, win);\
	if(_err_val != MPI_SUCCESS)\
	fprintf(stderr,"error occured for lock with val%d",_err_val);\
	MPI_Get(&_result, _count, MPI_INT, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_node_idx_index*sizeof(int), _count, MPI_INT, win);\
	MPI_Win_unlock(_rank, win);\
}

#define adjust_begin_to_global_values(_begin, _node, G)\
{\
	int _rank = G.get_rank_node(_node);\
  _begin[0] += G.global_forward_edge_start_for_all[_rank];\
  _begin[1] += G.global_forward_edge_start_for_all[_rank];\
}

#define adjust_r_begin_to_global_values(_begin, _node, G)\
{\
	int _rank = G.get_rank_node(_node);\
  _begin[0] += G.global_reverse_edge_start_for_all[_rank];\
  _begin[1] += G.global_reverse_edge_start_for_all[_rank];\
}

#define get_begin_start_end(_node, _start, _end, G){\
	    int *_node_start_end = (int *)calloc(2, sizeof(int));\
	    lock_and_get_begin(_node_start_end[0], _node, 2, G);\
	    _start = _node_start_end[0];\
	    _end = _node_start_end[1];\
	    delete _node_start_end;\
}

//------------------------------------------------------------------------------------------
// Representation of Graph
//
// (1) Fronzen Form: CSR implementation (which is a compacted adjacnency list)
//       - Nodes are identified by node-idx (0 ~ N-1)
//       - Edges are idenfified by edge-idx (0 ~ M-1)
//       - Basic form consists of two arrays
//            edge_t begin    O(E)  : beginning the neighbor-list of each node-idx
//            node_t node_idx O(N)  : destination node-idx of each edge-idx.
//       - For instance, following code iterates all the (out) neighbors of node k;
//            edge_t begin = G.begin(k);
//            edge_t end = G.begin(k+1);
//            for(edge_t t = begin; t < end; t++) {
//               node_t n = G.node_idx[t];
//               ......
//            }
//
// (2) Properties are (assumed to be) stored in array.
//       - Node properties are stored in node-idx order.
//       - Edge properties are stored in edge-idx order.
//       - These indices are for when the graph is initially frozen (loaded).
//
//
// (3) Additional indicies are created by request.
//      - make_reverse_edges();  ==> create reverse edges
//      - do_semi_sort();        ==> sort edges from the same source by the order of destination nodes
//      - prepare_edge_source(); ==> create O(E) array of source node-idx (as opposed to destination node-idx)
//
//      * make_reverse_edge:
//           - create following data structures:
//              edge_t r_begin    O(E) :  beginning of in-neighbor list of each node-idx
//              node_t r_node_idx O(N) :  destination of each reverse edge, i.e. source of original edge
//              edge_t e_rev2idx  O(E) :  a mapping of reverse edge-idx ==> original edge-idx
//
//           - For instance, following code iterates all the incoming neighbors of node k;
//               edge_t begin = G.r_begin(k);
//               edge_t end = G.r_begin(k+1);
//               for(edge_t t = begin; t < end; t++) {
//                   node_t n = G.r_node_idx[t];
//                   ......
//                }
//           - For instance, following code look at all the edge values of incoming edges
//                edge_t begin = G.r_begin(k);
//                edge_t end = G.r_begin(k+1);
//                for(edge_t t = begin; t < end; t++) {
//                   value_t V = EdgePropA[e_rev2idx[t]]; 
//                   ......
//                }
//
//      * do_semi_sort:  // [XXX: to be changed as CSR representation MUST always be sorted]
//          - node_idx array is semi-sorted. If reverse-edge has been created, r_node_idx array is also semi-sorted.
//          - create following data structure:
//              edge_t e_idx2idx  O(M) : a mappping of sorted edge-idx ==> original edge-idx
//
//          - Henthforth, once semi-sorting has been applied, edge properties have to be indirected as in following example.
//                edge_t begin = G.r_begin(k);
//                edge_t end = G.r_begin(k+1);
//                for(edge_t t = begin; t < end; t++) {
//                   value_t V = EdgePropA[e_idx2idx[t]]; 
//                   ......
//                }
//          - e_rev2idx is automatically updated, as r_node_idx array is sorted.
//          
//
//      * prepare_edge_source:
//         - create following data structure 
//              node_t* node_idx_src     O(M) // source of each edge-idx
//              node_t* r_node_idx_src;  O(M) // source of each reverse edge-idx (i.e. org destination)
//
//        
//
//
//------------------------------------------------------------------------------------------


//--------------------------------------------------------------------------
// Representation of Graph
// 
// 1. IDX vs ID
//
// node_t(edge_t) is used to represent both node(edge) ID and node(edge) IDX. 
// ID is used to indicate a speicific node(edge) in flexible format
// IDX is to represent the position of node(edge) in the compact form.
//
// As for node, node ID is same to node IDX.
// As for edge, edge ID can be different from edge IDX.
// When freezing the graph, the system creates a mapping between ID -> IDX and IDX -> ID.
//
// 2. The graph is represented as two format.
//
//   Flexible Format
//     Map<node_ID, vector<Node_ID, Edge_ID> > ; neighborhood list
//
//--------------------------------------------------------------------------



struct edge_dest_t  // for flexible graph representation
{
    node_id dest;
    edge_id edge;
};

class gm_graph
{

// Give access to gm_graph_hdfs
friend class gm_graph_hdfs;

  public:
    gm_graph();
    ~gm_graph();

    //-----------------------------------------------------
    // Direct access (only avaiable after frozen) 
    // GM compiler will use direct access only.
    //-----------------------------------------------------
    edge_t* begin;             // O(N) array of edge_t
		//mpi
    edge_t* local_begin;
		int local_begin_index;
		//mpi
    
		node_t* node_idx;          // O(M) array of node_t (destination of each edge)
    //mpi
    node_t* local_node_idx;          // O(M) array of node_t (destination of each edge)
		int local_node_idx_index;
		//mpi

		node_t* node_idx_src;      // O(M) array of node_t (source of each edge)
		//mpi
		node_t* local_node_idx_src;      // O(M) array of node_t (source of each edge)
		//mpi

    edge_t* r_begin;           // O(N) array of edge_t
    //mpi
    edge_t* local_r_begin;
    int local_r_begin_index;
		//mpi

		node_t* r_node_idx;        // O(M) array of node_t (destination of each reverse edge, i.e. source of original edge)
    //mpi
		node_t* local_r_node_idx;
    int local_r_node_idx_index;

		//mpi

		node_t* r_node_idx_src;    // O(M) array of node_t (source of each reverse edge, i.e. destination of original edge)
		//mpi
		node_t* local_r_node_idx_src;    // O(M) array of node_t (source of each reverse edge, i.e. destination of original edge)
		//mpi

    edge_t* e_idx2idx;  	   // O(M) array of edge_t (a mapping sorted edge idx => original edge idx, created after semi-sorting)
		//mpi
    edge_t* local_e_idx2idx;  	   // O(M) array of edge_t (a mapping sorted edge idx => original edge idx, created after semi-sorting)
		//mpi
    
		edge_t* e_rev2idx;  	   // O(M) array of edge_t (a mapping reverse edge idx => original edge idx, created after reverse edge creation)
		//mpi
    edge_t* local_e_rev2idx;
    edge_t* global_forward_edge_start_for_all;
    edge_t* global_reverse_edge_start_for_all;
    node_t* blk_size;
    node_t* global_node_start_for_all;
    std::map<node_t, node_t> vmap;
    std::map<edge_t, edge_t> emap;
		//mpi

    static const node_t NIL_NODE = (node_t) -1;
    static const edge_t NIL_EDGE = (edge_t) -1;

    bool is_neighbor(node_t src, node_t to); // need semi sorting
    bool has_edge_to(node_t source, node_t to);
    edge_t get_edge_idx_for_src_dest(node_t src, node_t dest);

    node_t num_nodes() {
        return _numNodes;
    }

    edge_t num_edges() {
        return _numEdges;
    }

    bool has_reverse_edge() {
        return _reverse_edge;
    }

    bool is_frozen() {
        return _frozen;
    }

    bool is_directed() {
        return _directed;
    }

    bool is_semi_sorted() {
        return _semi_sorted;
    }

    bool has_separate_edge_idx() {
        return (e_id2idx != NULL);
    }

    bool is_edge_source_ready() {
        return (node_idx_src != NULL);
    }

		//mpi
    node_t get_num_of_local_nodes() {
        return _num_of_local_nodes;
    }
    edge_t get_num_of_local_forward_edges() {
        return _num_of_local_forward_edges;
    }
    edge_t get_num_of_local_reverse_edges() {
        return _num_of_local_reverse_edges;
    }
    edge_t get_global_forward_edge_start() {
        return _global_forward_edge_start;
    }
    edge_t get_global_reverse_edge_start() {
        return _global_reverse_edge_start;
    }
		int get_local_begin_index() {
			return local_begin_index;
		}
		int get_local_node_idx_index() {
			return local_node_idx_index;
		}
    int get_local_r_begin_index() {
      return local_r_begin_index;
    }
    int get_local_r_node_idx_index() {
      return local_r_node_idx_index;
    }
		//mpi
		int get_rank();
		int get_block_size();
		int get_num_processes();
		void create_dynamic_window();
		void create_base_address();
		void win_attach(int *, int);
		void win_attach_bool(bool *, int);
		void win_attach_int(int32_t *, int);
		void win_attach_float(float *, int);
		void win_attach_double(double *, int);
		void win_attach_uchar(unsigned char *, int);
    void win_attach_master(int *, int);
		int get_local_node_num(node_t);
		int get_rank_node(node_t);
		int get_local_edge_num(edge_t);
		int get_rank_edge(edge_t);
		int get_global_edge_num(edge_t);
		int get_global_edge_num_for_my_rank(edge_t);
		int get_global_node_num(node_t);
		void generate_input_for_bgl();
    void generate_input_for_dhfalcon(char *);
    void create_cyclic_distribution(int, char *);
    void create_random_distribution(int, char *);

		//mpi

    //------------------------------------------------
    // Methods for graph manipulation
    //------------------------------------------------
    void thaw();                        // change the graph into flexible form (vector of vectors)
    void freeze();                      // change the graph into CSR form (fast & compact but unmodifiable)
    void make_undirected() {
        assert(false);
    } // [XXX] to be added
		//mpi
		void distribute();
		int declare_dynamic_window();
		int declare_base_address();
		void print_begin_master();
		void print_node_idx_master();
		//mpi

    //-------------------------------------------------
    // mathods to be called in frozen mode
    //-------------------------------------------------
    void make_reverse_edges();          // Freeze the graph first. Then build-up reverse edges
    void do_semi_sort();                // Freeze the graph first. Sort the edge-list as in the order of destination idx.
    void prepare_edge_source();         // Prepare source information of each node. (To support edge.From())

    //-------------------------------------------------------
    // Interrface for flexible graph creation
    //-------------------------------------------------------
    node_id add_node();                             // returns ID of a node
    edge_id add_edge(node_id n, node_id m);         // add an edge n->m

    bool is_node(node_id n) {
        return (n < _numNodes);
    } // what if after

    bool is_edge(edge_id n) {
        return (n < _numEdges);
    }

    bool has_edge(node_id from, node_id to);
    edge_t get_num_edges(node_id from, node_id to);   // how many edges between two nodes

    edge_t get_num_edges(node_id from) {               // how many edges from this node
        return begin[from + 1] - begin[from];
    }

    //------------------------------------------------------------
    // Methods to be implemented for deletion
    //------------------------------------------------------------
    void detach_node(node_id n) {
        assert(false);
    }
    void remove_all_edges(node_id n, node_id m) {
        assert(false);
    }
    void remove_edge(edge_id n) {
        assert(false);
    }
    void compresss_graph() {
        assert(false);
    } // all the previous id become invalidated

    //--------------------------------------------------------------
    // Read and Write the graph from/to a file, using a custom binary format
    // The graph will be frozen automatically.
    //--------------------------------------------------------------
    #define MAGIC_WORD_BIN  0x03939999
    #define MAGIC_WORD_EBIN 0x99191191
    void prepare_external_creation(node_t n, edge_t m);
    void prepare_external_creation(node_t n, edge_t m, bool clean_key_id_mappings);
    bool store_binary(char* filename);          // attributes not saved
    bool load_binary(char* filename);           // call this to an empty graph object

    /*
     * A specialized function to load a graph represented using the adjacency list format.
     */
    bool load_adjacency_list(char* filename, char separator = '\t');  // to be depricated

    void load_adjacency_list_internal(std::vector<VALUE_TYPE> vprop_schema,
            std::vector<VALUE_TYPE> eprop_schema,
            std::vector<void *>& vertex_props,
            std::vector<void *>& edge_props,
            std::vector<edge_t>& EDGE_CNT,
            std::vector<node_t>& DEST,
            std::vector<void*>& node_prop_vectors,
            std::vector<void*>& edge_prop_vectors,
            node_t N,
            edge_t M
            );


    /*
     * A generic function to load a graph represented using the adjacency list format.
     * Adjacency List Format:
     *     vertex-id {vertex-val1 vertex-val2 ...} [nbr-vertex-id {edge-val1 edge-val2 ...}]*
     */
    bool load_adjacency_list(const char* filename, // input parameter
            std::vector<VALUE_TYPE> vprop_schema, // input parameter
            std::vector<VALUE_TYPE> eprop_schema, // input parameter
            std::vector<void *>& vertex_props, // output parameter
            std::vector<void *>& edge_props, // output parameter
            const char* separators = "\t", // optional input parameter
            bool use_hdfs = false // optional input parameter
            );
    /*
     * A generic function to store a graph represented using the adjacency list format.
     * Adjacency List Format:
     *     vertex-id {vertex-val1 vertex-val2 ...} [nbr-vertex-id {edge-val1 edge-val2 ...}]*
     */
    bool store_adjacency_list (const char* filename, // input parameter
            std::vector<VALUE_TYPE> vprop_schema, // input parameter
            std::vector<VALUE_TYPE> eprop_schema, // input parameter
            std::vector<void*>& vertex_props, // input parameter
            std::vector<void*>& edge_props, // input parameter
            const char* separators = "\t", // input parameter
            bool use_hdfs = false // input parameter
            );
    bool store_node_properties_list (const char* filename, // input parameter
            std::vector<VALUE_TYPE> vprop_schema, // input parameter
            std::vector<void*>& vertex_props, // input parameter
            const char* separators = "\t", // input parameter
            bool use_hdfs = false // input parameter
            );


    bool load_adjacency_list_avro(const char* filename, // input parameter
            std::vector<VALUE_TYPE>& vprop_schema, // output parameter
            std::vector<VALUE_TYPE>& eprop_schema, // output parameter
            std::vector<std::string>& vprop_names, // output parameter
            std::vector<std::string>& eprop_names, // output parameter
            std::vector<void *>& vertex_props, // output parameter
            std::vector<void *>& edge_props, // output parameter
            bool use_hdfs = false // input parameter
            );

    bool store_adjacency_list_avro(const char* filename, // input parameter
            std::vector<VALUE_TYPE> vprop_schema, // input parameter
            std::vector<VALUE_TYPE> eprop_schema, // input parameter
            std::vector<std::string> vprop_names, // input parameter
            std::vector<std::string> eprop_names, // input parameter
            std::vector<void*>& vertex_props, // input parameter
            std::vector<void*>& edge_props, // input parameter
            bool use_hdfs = false // input parameter
            );

    bool load_edge_list(
            char* filename,                          // input: filename
            std::vector<VALUE_TYPE>& vprop_schema,   // input: type of node properties
            std::vector<VALUE_TYPE>& eprop_schema,   // input: type of edge properties
            std::vector<void*>& vertex_props,        // output: vector of arrays
            std::vector<void*>& edge_props,          // output: vector of arrays,
            bool use_hdfs = false
            );

    bool store_edge_list(
            char* filename,                         // input: filename
            std::vector<VALUE_TYPE>& vprop_schema,  // input: type of node properties
            std::vector<VALUE_TYPE>& eprop_schema,  // input: type of edge properties
            std::vector<void*>& vertex_props,       // input: vector of arrays
            std::vector<void*>& edge_props,         // input: vector of arrays,
            bool use_hdfs = false
            );

    bool load_extended_binary(
            char* filename,                          // input: filename
            std::vector<VALUE_TYPE>& vprop_schema,   // input: type of node properties
            std::vector<VALUE_TYPE>& eprop_schema,   // input: type of edge properties
            std::vector<void*>& vertex_props,        // output: vector of arrays
            std::vector<void*>& edge_props,          // output: vector of arrays,
            bool use_hdfs = false
            );

    bool store_extended_binary(const char* filename, // input parameter
            std::vector<VALUE_TYPE> vprop_schema, // input parameter
            std::vector<VALUE_TYPE> eprop_schema, // input parameter
            std::vector<void*>& vertex_props, // input parameter
            std::vector<void*>& edge_props, // input parameter
            bool use_hdfs = false // input parameter
            );

    //--------------------------------------------------------------
    // conversion between idx and id
    //--------------------------------------------------------------
    inline edge_t get_edge_idx(edge_id e) {
        return e_id2idx == NULL ? e : e_id2idx[e];
    }
    inline node_t get_node_idx(node_id n) {
        return n;
    }
    inline edge_id get_edge_id(edge_t e) {
        return e_idx2id == NULL ? e : e_idx2id[e];
    }
    inline node_id get_node_id(node_t n) {
        return n;
    }

    inline edge_t get_org_edge_idx(edge_id e) {
        return e_idx2idx == NULL ? e : e_idx2idx[e];
    }

    void clear_graph(bool clean_key_id_mappings);    // invalidate everything and make the graph empty
    void clear_graph();                         

    //returns one of the outgoing neighbors of 'node' - by random choice
    // if 'node' does not have a neighbor, 'node' is returned -> assert(false)
    node_t pick_random_out_neighbor(node_t node); 

    node_t pick_random_node() {
        return rand() % num_nodes(); //TODO make 64bit compatible
    }

#ifdef HDFS
    bool load_binary_hdfs(char* filename); // deprecated
#endif  // HDFS

    inline node_t nodekey_to_nodeid(node_t key) {
      // not all graphs have this mapping defined (only those loaded from adjacency list format)
      return (_nodekey_defined ? _numeric_key[key] : key);
    }
    inline node_t nodeid_to_nodekey(node_t nodeid) {
      // not all graphs have this mapping defined (only those loaded from adjacency list format)
      return (_nodekey_defined ? _numeric_reverse_key[nodeid] : nodeid);
    }
  private:

    void delete_frozen_graph();
    void delete_local_graph();
    void allocate_memory_for_frozen_graph(node_t n, edge_t m);

    node_t _numNodes;
    edge_t _numEdges;
    bool _reverse_edge;
    bool _frozen;
    bool _directed;
    bool _semi_sorted;

		//mpi
		node_t _num_of_local_nodes;
		edge_t _num_of_local_forward_edges;
		edge_t _num_of_local_reverse_edges;
		edge_t _global_forward_edge_start;
		edge_t _global_reverse_edge_start;
		//mpi

    void do_semi_sort_reverse();
    void prepare_edge_source_reverse();

    //std::map<node_t, std::vector<edge_dest_t> > flexible_graph;
    std::unordered_map<node_t, std::vector<edge_dest_t> > flexible_graph;

    edge_t* e_id2idx;
    edge_t* e_idx2id;
		//mpi
    edge_t* local_e_id2idx;
    edge_t* local_e_idx2id;
		//mpi

    //-----------------------------------------------------
    // Use node key that is different from node idx.
    //-----------------------------------------------------
    bool _nodekey_defined;          // 
    std::unordered_map<node_t, node_t> _numeric_key;  // node_key -> node_idx
    std::vector<node_t>      _numeric_reverse_key;    // node_idx -> node_key

    void   prepare_nodekey();    // should call this function before graph is create
    void   delete_nodekey();
    node_t add_nodekey(node_t key);

    inline bool find_nodekey(node_t key) {return _numeric_key.find(key) != _numeric_key.end();}
    inline node_t get_num_nodekeys() {return (node_t) _numeric_key.size();}

    bool load_binary_internal(FILE* f, uint32_t magic_word, bool need_semisort);
    bool store_binary_internal(FILE* f, uint32_t magic_word);

    bool store_nodekey_binary(FILE*f);
    bool load_nodekey_binary(FILE*f);
};

#endif
