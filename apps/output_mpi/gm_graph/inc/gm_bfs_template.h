#ifndef GM_BFS_TEMPLATE_H
#define GM_BFS_TEMPLATE_H
#include <omp.h>
#include <string.h>
#include <map>
#include <set>

#include "gm_graph.h"
#include "gm_atomic_wrapper.h"
#include "gm_bitmap.h"


//mpi
extern MPI_Win win;
extern MPI_Aint *base_address;
extern int base_address_index;


#define lock_and_get_node_property_int(_result, _ba_index, _rank, _prop_index)\
{\
	    int _err_val;\
	    _err_val=MPI_Win_lock(MPI_LOCK_EXCLUSIVE,_rank, 0, win);\
	    if(_err_val != MPI_SUCCESS)\
	    fprintf(stderr,"error occured for lock with val%d",_err_val);\
	    MPI_Get(&_result, 1, MPI_INT, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_prop_index*sizeof(int), 1, MPI_INT, win);\
	    MPI_Win_unlock(_rank, win);\
}

#define lock_and_put_node_property_int(_result, _ba_index, _rank, _prop_index)\
{\
	    int _err_val;\
	    _err_val=MPI_Win_lock(MPI_LOCK_EXCLUSIVE,_rank, 0, win);\
	    if(_err_val != MPI_SUCCESS)\
	    fprintf(stderr,"error occured for lock with val%d",_err_val);\
	    MPI_Put(&_result, 1, MPI_INT, _rank, base_address[G.get_num_processes()*_ba_index +_rank] + _prop_index*sizeof(int), 1, MPI_INT, win);\
	    MPI_Win_unlock(_rank, win);\
}

#define lock_and_get_int(_result, _ba_index, _rank)\
{\
	    int _err_val;\
	    _err_val=MPI_Win_lock(MPI_LOCK_EXCLUSIVE,_rank, 0, win);\
	    if(_err_val != MPI_SUCCESS)\
	    fprintf(stderr,"error occured for lock with val%d",_err_val);\
	    MPI_Get(&_result, 1, MPI_INT, _rank, base_address[G.get_num_processes()*_ba_index+_rank], 1, MPI_INT, win);\
	    MPI_Win_unlock(_rank, win);\
}

#define lock_fetch_and_incr(_result, _ba_index, _rank)\
{\
	    int _err_val;\
	    _err_val=MPI_Win_lock(MPI_LOCK_EXCLUSIVE,_rank, 0, win);\
	    if(_err_val != MPI_SUCCESS)\
	    fprintf(stderr,"error occured for lock with val%d",_err_val);\
	    int incr_val = 1;\
	    MPI_Fetch_and_op(&incr_val, &_result, MPI_INT, _rank, base_address[G.get_num_processes()*_ba_index+_rank], MPI_SUM, win);\
	    MPI_Win_unlock(_rank, win);\
}

#define lock_and_incr(_ba_index, _rank)\
{\
	    int _err_val;\
	    _err_val=MPI_Win_lock(MPI_LOCK_EXCLUSIVE,_rank, 0, win);\
	    if(_err_val != MPI_SUCCESS)\
	    fprintf(stderr,"error occured for lock with val%d",_err_val);\
	    int incr_val = 1;\
	    MPI_Accumulate(&incr_val, 1, MPI_INT, _rank, base_address[G.get_num_processes()*_ba_index+_rank], 1, MPI_INT, MPI_SUM, win);\
	    MPI_Win_unlock(_rank, win);\
}
//mpi

// todo: consideration for non small-world graph
template<typename level_t, bool use_multithread, bool has_navigator, bool use_reverse_edge, bool save_child>
class gm_bfs_template
{

public:
	gm_bfs_template(gm_graph& _G) :
		G(_G) {
		visited_bitmap = NULL; // bitmap
		visited_level = NULL;

		//global_curr_level = NULL;
		//global_next_level = NULL;
		//global_queue = NULL;
		thread_local_next_level = NULL;
		down_edge_array = NULL;
		down_edge_set = NULL;

		//mpi
		small_visited = NULL;
		//mpi
		if (save_child) {
			down_edge_set = new std::set<edge_t>();
		}
	}

  virtual ~gm_bfs_template() {
    MPI_Barrier(MPI_COMM_WORLD);
    //delete [] local_vector;
    //delete [] small_visited;
    if (save_child) {
      // FIXME: Currently avoided deleting because of seg fault.
      //delete down_edge_set;
    }
  }

	void prepare(node_t root_node, int max_num_thread) {
		is_finished = false;
		curr_level = 0;
		root = root_node;
		state = ST_SMALL;
		//mpi
		local_vector_size = 0;
		local_vector_size_index = base_address_index;
		G.win_attach_int(&local_vector_size, 1);

		local_vector = new node_t[G.get_num_of_local_nodes()];
		local_vector_index = base_address_index;
		G.win_attach_int(local_vector, G.get_num_of_local_nodes());
		small_visited = new int[G.get_num_of_local_nodes()];
		int i;
		for (i = 0; i < G.get_num_of_local_nodes(); i++) {
			small_visited[i] = __INVALID_LEVEL;
		}
		small_visited_index = base_address_index;
		G.win_attach_int(small_visited, G.get_num_of_local_nodes());

		local_next_count_index = base_address_index;
		G.win_attach_int(&local_next_count, 1);
		//mpi
		//want to check how this assert can be implemented :MPI
		//assert(root != gm_graph::NIL_NODE);
		//mpi
		//mpi
	}

	edge_t get_t_neighbors_begin() {
		return t_neighbors_begin;
	}

	edge_t get_t_neighbors_end() {
		return t_neighbors_end;
	}

	edge_t get_t_neighbor(int ind)
	{
		return t_neighbors[ind-t_neighbors_begin];
	}

/*
	void gather_all()
	{
		//do mpi_gather of local_vectors to global_vector and broadcast global_vector to all processes(all_gather).
		//apply reduction on local_next_count to next_count (all_reduce).
		//edge_set gather(all_gather).
		//TODO: not sure where to apply gather_all.==>it may be optimal to apply it at change_state

		int * local_next_counts = new int[G.get_num_processes()];
		MPI_Allgather(&local_next_count, 1, MPI_INT, local_next_counts, 1, MPI_INT, MPI_COMM_WORLD);

		int *recv_disp = new int[G.get_num_processes()];
		int i;
		recv_disp[0] = 0;
		for(i=1; i<G.get_num_processes(); i++)
		{
			recv_disp[i] = recv_disp[i-1]+local_next_counts[i-1];
		}

		int *global_vector_array = new int[G.num_nodes()];
		MPI_Allgatherv(local_vector, local_next_count, MPI_INT, global_vector_array, local_next_counts, recv_disp, MPI_INT, MPI_COMM_WORLD);

		int num_of_new_global_vector_entries = recv_disp[G.get_num_processes() - 1] + local_next_counts[G.get_num_processes()-1];

		for(i=0; i<num_of_new_global_vector_entries; i++)
			global_vector.push_back(global_vector_array[i]);
		//==========================================================================================================

		next_count += num_of_new_global_vector_entries;

		//==========================================================================================================

		int *global_down_edge_set_array = new int[G.num_nodes()];
		MPI_Allgatherv(local_down_edge_set, local_next_count, MPI_INT, global_down_edge_set_array, local_next_counts, recv_disp, MPI_INT, MPI_COMM_WORLD);

		int num_of_new_global_edge_set_entries = num_of_new_global_vector_entries;

		for(i=0; i<num_of_new_global_edge_set_entries; i++)
			down_edge_set->insert(global_down_edge_set_array[i]);

		//=========================================================================================================
		//resetting local_next_count
		local_next_count = 0;

	}
*/
	void do_bfs_forward() {
		//---------------------------------
		// prepare root node
		//---------------------------------
		curr_level = 0;
		curr_count = 0;
		local_next_count = 0;

		if(G.get_rank_node(root) == G.get_rank())
		{
			int root_local_no = G.get_local_node_num(root);
			small_visited[root_local_no] = curr_level;
			curr_count++;
			local_vector[local_vector_size] = root;
			local_vector_size++;
		}

		local_curr_level_begin = 0;
		local_next_level_begin = curr_count;
		level_count.push_back(curr_count);
		level_queue_begin.push_back(local_curr_level_begin);

#define PROFILE_LEVEL_TIME  0
#if PROFILE_LEVEL_TIME
		struct timeval T1, T2;
#endif

		bool is_done = false;
    while (!is_done) {
#if PROFILE_LEVEL_TIME
      gettimeofday(&T1,NULL);
      printf("state = %d, curr_count=%d, ", state, curr_count);
#endif
      switch (state) {
        case ST_SMALL:
          for (node_t i = 0; i < curr_count; i++) {
            node_t t = local_vector[local_curr_level_begin + i];
            iterate_neighbor_small(t);
            //gather_all();
            int t_local = G.get_local_node_num(t);
            visit_fw(t_local);            // visit after iteration. in that way, one can check  down-neighbors quite easily
          }
          MPI_Barrier(MPI_COMM_WORLD);
          break;
      } // end of switch

      do_end_of_level_fw();
      is_done = get_next_state();
#if PROFILE_LEVEL_TIME
      gettimeofday(&T2, NULL);
      printf("time = %6.5f ms\n", (T2.tv_sec - T1.tv_sec)*1000 + (T2.tv_usec - T1.tv_usec)*0.001);
#endif
    } // end of while
    MPI_Barrier(MPI_COMM_WORLD);
	}


	/*void set_neighbors(node_t t) {

		edge_t begin,end;
		if(G.get_rank_node(t) == G.get_rank())
		{
			node_t t_local = G.get_local_node_num(t);
			get_range(begin, end, t_local);
			//getting global number of begin and end
			begin += G.get_global_forward_edge_start();
			end += G.get_global_forward_edge_start();

		}
		MPI_Bcast(&begin, 1, MPI_INT, G.get_rank_node(t), MPI_COMM_WORLD);
		MPI_Bcast(&end, 1, MPI_INT, G.get_rank_node(t), MPI_COMM_WORLD);

		// Copying begin and end to be used in visit_fw and visit_rw.
		t_neighbors_begin = begin;
		t_neighbors_end= end;

		t_neighbors = new node_t[end-begin];
		if(G.get_rank_node(t) == G.get_rank())
		{
			edge_t i;
			int local_begin = begin - G.get_global_forward_edge_start();
			for(i=0; i< end - begin; i++)
			{
				t_neighbors[i] = G.local_node_idx[local_begin+i];//
			}
		}

		MPI_Bcast(t_neighbors, end-begin, MPI_INT, G.get_rank_node(t), MPI_COMM_WORLD);

	}*/

	void do_bfs_reverse() {
		// This function should be called only after do_bfs_foward has finished.
		// assumption: small-world graph
		level_t& level = curr_level;
		while (true) {
			node_t count = level_count[level];
			//node_t* queue_ptr = level_start_ptr[level];
			node_t* queue_ptr;
			node_t begin_idx = level_queue_begin[level];
			if (begin_idx == -1) {
				queue_ptr = NULL;
			} else {
				queue_ptr = & (local_vector[begin_idx]);
			}

			if (queue_ptr == NULL) {
				{
					for (node_t i = 0; i < G.num_nodes(); i++) {
						if (visited_level[i] != curr_level) continue;
						visit_rv(i);
					}
				}
			} else {
					for (node_t i = 0; i < count; i++) {
						node_t u = queue_ptr[i];
						//mpi
						
						//set_neighbors(u);
						//TODO:make this a function
						
						if(G.get_rank_node(u) == G.get_rank())
						{
							int u_local = G.get_local_node_num(u);
							visit_rv(u_local);
						}
						//mpi






					}
			}

			do_end_of_level_rv();
			MPI_Barrier(MPI_COMM_WORLD);
			if (level == 0) break;
			level--;
		}
		MPI_Barrier(MPI_COMM_WORLD);
	}

	bool is_down_edge(edge_t idx) {
		//following comments are done for MPI
		//if (state == ST_SMALL)
		return (down_edge_set->find(idx) != down_edge_set->end());
		//else
		//  return down_edge_array[idx];
	}

protected:
	virtual void visit_fw(node_t t)=0;
	virtual void visit_rv(node_t t)=0;
	virtual bool check_navigator(node_t t, edge_t nx)=0;
	virtual void do_end_of_level_fw() {
	}
	virtual void do_end_of_level_rv() {
	}

	node_t get_root() {
		return root;
	}

  level_t get_level(node_t t) {
    // FIXMI: This works only for state == SMALL.
    int small_visited_t;//mpiget from u
    int t_rank = G.get_rank_node(t);
    int t_local_num = G.get_local_node_num(t);
    lock_and_get_node_property_int(small_visited_t, small_visited_index, t_rank, t_local_num);		

    return small_visited_t;
    // GCC expansion
    /*
       if (__builtin_expect((state == ST_SMALL), 0)) {
       return small_visited[t];
       } else {
       return visited_level[t];
       }
     */
  }

	level_t get_curr_level() {
		return curr_level;
	}


private:
	bool get_next_state() {
		const char* state_name[5] = {"SMALL","QUEUE","Q2R","RD","R2Q"};
		int next_count_of_all_procs = 0;
		MPI_Allreduce(&local_next_count, &next_count_of_all_procs, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
		if (next_count_of_all_procs == 0) return true;  // BFS is finished

		int next_state = state;
		bool already_expanded = false;
		const float RRD_THRESHOLD = 0.05;
    switch (state) {
      case ST_SMALL:
        next_state = ST_SMALL;
        break;
    }
		finish_level(state);
		state = next_state;
    MPI_Barrier(MPI_COMM_WORLD);
		return false;
  }

  void finish_level(int state) {
    local_curr_level_begin = local_next_level_begin;
    local_next_level_begin = local_next_level_begin + local_next_count;

    curr_count = local_next_count;
    local_next_count = 0;
    curr_level++;
    // save 'new current' level status
    level_count.push_back(curr_count);

    level_queue_begin.push_back(local_curr_level_begin);
  }

	void get_range(edge_t& begin, edge_t& end, node_t t) {
		if (use_reverse_edge) {
			begin = G.local_r_begin[t];
			end = G.local_r_begin[t + 1];
		} else {
			begin = G.local_begin[t];
			end = G.local_begin[t + 1];
		}
	}
	void get_range_rev(edge_t& begin, edge_t& end, node_t t) {
		if (use_reverse_edge) {
			begin = G.begin[t];
			end = G.begin[t + 1];
		} else {
			begin = G.r_begin[t];
			end = G.r_begin[t + 1];
		}
	}

	node_t get_node(edge_t& n) {
		if (use_reverse_edge) {
			return G.local_r_node_idx[n];
		} else {
			return G.local_node_idx[n];
		}
	}
	node_t get_node_rev(edge_t& n) {
		if (use_reverse_edge) {
			return G.node_idx[n];
		} else {
			return G.r_node_idx[n];
		}
	}

	void iterate_neighbor_small(node_t t) {
		edge_t begin;
		edge_t end;

		node_t t_local = G.get_local_node_num(t);
		get_range(begin, end, t_local);

    for (edge_t nx = begin; nx < end; nx++) {
      node_t u = get_node(nx);

      // check visited
      //check at master if u is visited
      int small_visited_u;//mpiget from u
      int u_rank = G.get_rank_node(u);
      int u_local_num = G.get_local_node_num(u);
      lock_and_get_node_property_int(small_visited_u, small_visited_index, u_rank, u_local_num);		
      if (small_visited_u == __INVALID_LEVEL) {
        if (has_navigator) {
          if (check_navigator(u, nx) == false) continue;
        }
        if (save_child) {
          save_down_edge_small(nx);
        }

        int u_level = curr_level + 1;
        //insert u_level to small_visited[u] with MPI_Put
        lock_and_put_node_property_int(u_level, small_visited_index, u_rank, u_local_num);
        //insert u to local_vector of u with MPI_Put
        int curr_local_vector_size;
        lock_fetch_and_incr(curr_local_vector_size, local_vector_size_index, u_rank);
        lock_and_put_node_property_int(u, local_vector_index, u_rank, curr_local_vector_size);
        //MPI_Accumulate 1 to local_next_count of processor of u.
        lock_and_incr(local_next_count_index, u_rank);
      }
      else if (save_child) {
        if (has_navigator) {
          if (check_navigator(u, nx) == false) continue;
        }
        if (small_visited_u == (curr_level+1)){ //mpi if it is visited already and is in the same level
          //to be done at master
          save_down_edge_small(nx);
        }
      }
    }
	}

	// should be used only when save_child is enabled
	void save_down_edge_small(edge_t idx) {
		down_edge_set->insert(idx);
		//local_down_edge_set[local_next_count] = idx;
	}

	void save_down_edge_large(edge_t idx) {
		down_edge_array[idx] = 1;
	}

	void prepare_que() {

		global_vector.reserve(G.num_nodes());

		// create bitmap and edges
		visited_bitmap = new unsigned char[(G.num_nodes() + 7) / 8];
		visited_level = new level_t[G.num_nodes()];
		if (save_child) {
			down_edge_array = new unsigned char[G.num_edges()];
		}

		if (use_multithread) {
#pragma omp parallel
			{
#pragma omp for nowait //schedule(dynamic,65536)
				for (node_t i = 0; i < (G.num_nodes() + 7) / 8; i++)
					visited_bitmap[i] = 0;

#pragma omp for nowait //schedule(dynamic,65536)
				for (node_t i = 0; i < G.num_nodes(); i++)
					visited_level[i] =  __INVALID_LEVEL;

				if (save_child) {
#pragma omp for nowait //schedule(dynamic,65536)
					for (edge_t i = 0; i < G.num_edges(); i++)
						down_edge_array[i] = 0;
				}
			}
		} else {
			for (node_t i = 0; i < (G.num_nodes() + 7) / 8; i++)
				visited_bitmap[i] = 0;
			for (node_t i = 0; i < G.num_nodes(); i++)
				visited_level[i] =  __INVALID_LEVEL;
			if (save_child) {
				for (edge_t i = 0; i < G.num_edges(); i++)
					down_edge_array[i] = 0;
			}
		}

		int II;
		for (II = 0; II < G.get_num_of_local_nodes(); II++) {
			if(small_visited[II] != __INVALID_LEVEL)
			{
			node_t u = II;
			_gm_set_bit(visited_bitmap, u);
			visited_level[u] = small_visited[II];
			}
		}

		if (save_child) {
			typename std::set<edge_t>::iterator J;
			for (J = down_edge_set->begin(); J != down_edge_set->end(); J++) {
				down_edge_array[*J] = 1;
			}
		}
	}

	void iterate_neighbor_que(node_t t, int tid) {
		edge_t begin;
		edge_t end;
		get_range(begin, end, t);
		for (edge_t nx = begin; nx < end; nx++) {
			node_t u = get_node(nx);

			// check visited bitmap
			// test & test& set
			if (_gm_get_bit(visited_bitmap, u) == 0) {
				if (has_navigator) {
					if (check_navigator(u, nx) == false) continue;
				}

				bool re_check_result;
				if (use_multithread) {
					re_check_result = _gm_set_bit_atomic(visited_bitmap, u);
				} else {
					re_check_result = true;
					_gm_set_bit(visited_bitmap, u);
				}

				if (save_child) {
					save_down_edge_large(nx);
				}

				if (re_check_result) {
					// add to local q
					thread_local_next_level[tid].push_back(u);
					visited_level[u] = (curr_level + 1);
				}
			}
			else if (save_child) {
				if (has_navigator) {
					if (check_navigator(u, nx) == false) continue;
				}
				if (visited_level[u] == (curr_level +1)) {
					save_down_edge_large(nx);
				}
			}
		}
	}

	void finish_thread_que(int tid) {
		node_t local_cnt = thread_local_next_level[tid].size();
		//copy curr_cnt to next_cnt
		if (local_cnt > 0) {
			node_t old_idx = _gm_atomic_fetch_and_add_node(&next_count, local_cnt);
			// copy to global vector
			memcpy(&(local_vector[local_next_level_begin + old_idx]),
					&(thread_local_next_level[tid][0]),
					local_cnt * sizeof(node_t));//just renamed global to local for MPI
		}
		thread_local_next_level[tid].clear();
	}

	void prepare_read() {
		// nothing to do
	}

	void iterate_neighbor_rd(node_t t, node_t& local_cnt) {
		edge_t begin;
		edge_t end;
		get_range(begin, end, t);
		for (edge_t nx = begin; nx < end; nx++) {
			node_t u = get_node(nx);

			// check visited bitmap
			// test & test& set
			if (_gm_get_bit(visited_bitmap, u) == 0) {
				if (has_navigator) {
					if (check_navigator(u, nx) == false) continue;
				}

				bool re_check_result;
				if (use_multithread) {
					re_check_result = _gm_set_bit_atomic(visited_bitmap, u);
				} else {
					re_check_result = true;
					_gm_set_bit(visited_bitmap, u);
				}

				if (save_child) {
					save_down_edge_large(nx);
				}

				if (re_check_result) {
					// add to local q
					visited_level[u] = curr_level + 1;
					local_cnt++;
				}
			}
			else if (save_child) {
				if (has_navigator) {
					if (check_navigator(u, nx) == false) continue;
				}
				if (visited_level[u] == (curr_level +1)) {
					save_down_edge_large(nx);
				}
			}
		}
	}

	void finish_thread_rd(node_t local_cnt) {
		_gm_atomic_fetch_and_add_node(&next_count, local_cnt);
	}

	// return true if t is next level
	bool check_parent_rrd(node_t t)
	{
		bool ret = false;
		edge_t begin;
		edge_t end;
		get_range_rev(begin, end, t);

		if (has_navigator) {
			// [XXX] fixme
			if (check_navigator(t, 0) == false) return false;
		}

		for (edge_t nx = begin; nx < end; nx++) {
			node_t u = get_node_rev(nx);
			//if (_gm_get_bit(visited_bitmap, u) == 0) continue;
			if (visited_level[u] == (curr_level ))
				return true;
		}
		return false;
	}


	//-----------------------------------------------------
	//-----------------------------------------------------
	static const int ST_SMALL = 0;
	static const int ST_QUE = 1;
	static const int ST_Q2R = 2;
	static const int ST_RD = 3;
	static const int ST_R2Q = 4;
	static const int ST_RRD = 5;
	static const int ST_RR2Q = 6;
	static const int THRESHOLD1 = 128;  // single threaded
	static const int THRESHOLD2 = 1024; // move to RD-based

	// not -1.
	//(why? because curr_level-1 might be -1, when curr_level = 0)
	static const level_t __INVALID_LEVEL = -2;

	int state;

	unsigned char* visited_bitmap; // bitmap
	level_t* visited_level; // assumption: small_world graph
	bool is_finished;
	level_t curr_level;
	node_t root;
	gm_graph& G;
	node_t curr_count;
	node_t next_count;
	//mpi
	//mpi

	// mpi
	//std::map<node_t, level_t> small_visited;
	// Changed from map to array of level_t for mpi implementation.
	int* small_visited;
	int small_visited_index;
	//mpi
	std::set<edge_t>* down_edge_set;
	unsigned char* down_edge_array;

	//node_t* global_next_level;
	//node_t* global_curr_level;
	//node_t* global_queue;
	std::vector<node_t> global_vector;
	//mpi
	node_t * local_vector;
	int local_vector_size;
	int local_vector_size_index;
	int local_vector_index;
	int local_next_count;
	int local_next_count_index;
	node_t local_curr_level_begin;
	node_t local_next_level_begin;
	//mpi

	//std::vector<node_t*> level_start_ptr;
	std::vector<node_t> level_queue_begin;
	std::vector<node_t> level_count;

	std::vector<node_t>* thread_local_next_level;

	//mpi
	// Variables to store the neighbors of currently processing
	// node, t.
	node_t *t_neighbors;
	edge_t t_neighbors_begin;
	edge_t t_neighbors_end;
	//mpi
};

#endif
