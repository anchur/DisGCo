#ifndef GM_MAP_H_
#define GM_MAP_H_
#include <map>
#include "gm_internal.h"
#include "gm_limits.h"
#include "gm_lock.h"
#include "map_util.h"

#define MASTER 0
#define MAX_VALUE INT_MAX
#define MIN_VALUE INT_MIN

using namespace std;

//map-interface
template<class Key, class Value>
class gm_map
{
  public:
    virtual ~gm_map() {
    }
    ;

    virtual bool hasKey_seq(const Key key) = 0;

    virtual bool hasKey_par(const Key key) {
        return hasKey_seq(key);
    }

    /**
     * Return the value that has been set for the key - if none has been specified, the defaultValue is returned
     */
    virtual Value getValue(const Key key) = 0;

    virtual void setValue_par(const Key key, Value value) = 0;

    virtual void setValue_seq(const Key key, Value value) = 0;

    /**
     * Sets the value mapped to the given key to the default value
     */
    virtual void removeKey_par(const Key key) = 0;

    virtual void removeKey_seq(const Key key) = 0;

    /**
     * Returns true if the key corresponds to the highest value in the map.
     * If there has no value been set in the map, false is returned
     */
    virtual bool hasMaxValue_seq(const Key key) = 0;

    virtual bool hasMaxValue_par(const Key key) {
        return hasMaxValue_seq(key);
    }

    /**
     * Returns true if the key corresponds to the lowest value in the map.
     * If there has no value been set in the map, false is returned
     */
    virtual bool hasMinValue_seq(const Key key) = 0;

    virtual bool hasMinValue_par(const Key key) {
        return hasMinValue_seq(key);
    }

    /**
     * Returns the key that corresponds to the highest value in the map.
     * If there has no value been set in the map, the behavior is unspecified.
     */
    virtual Key getMaxKey_seq() = 0;

    virtual Key getMaxKey_par() {
        return getMaxKey_seq();
    }

    /**
     * Returns the key that corresponds to the lowest value in the map.
     * If there has no value been set in the map, the behavior is unspecified.
     */
    virtual Key getMinKey_seq() = 0;

    virtual Key getMinKey_par() {
        return getMinKey_seq();
    }

    /**
     * Returns the highest value in the map.
     * If there has no value been set in the map, the behavior is unspecified.
     */
    virtual Value getMaxValue_seq() = 0;

    virtual Value getMaxValue_par() {
        return getMaxValue_seq();
    }

    /**
     * Returns the lowest value in the map.
     * If there has no value been set in the map, the behavior is unspecified.
     */
    virtual Value getMinValue_seq() = 0;

    virtual Value getMinValue_par() {
        return getMinValue_seq();
    }

    /**
     * Adds the value of summand to the value mapped to key and returns the result.
     * This operation is atomic. If no value if mapped to key, key is mapped to summand
     * and summand is returned.
     */
    virtual Value changeValueAtomicAdd(const Key key, const Value summand) = 0;

    /**
     * This operation is equivalent to 'changeValueAtomicAdd(key, -1 * subtrahend)'
     */
    virtual Value changeValueAtomicSubtract(const Key key, const Value subtrahend) {
        return changeValueAtomicAdd(key, -1 * subtrahend);
    }

    virtual size_t size() = 0;

    virtual void clear() = 0;

  protected:

    virtual Value getDefaultValue() = 0;

    static bool compare_smaller(Value a, Value b) {
        return a < b;
    }

    static bool compare_greater(Value a, Value b) {
        return a > b;
    }

    static Value max(Value a, Value b) {
        return std::max<Value>(a, b);
    }

    static Value min(Value a, Value b) {
        return std::min<Value>(a, b);
    }

};

class gm_map_node_int 
{
  private:

    // head_ptrs store the address of first node of the linked_list
    // which will be a dummy node with key and value as -1
    // FIXME : check if initialising with -1 is appropriate. 
    hash_node_ptr_t head_ptrs[TABLE_SIZE];
    
    //map_util.h
    KeyHash hashFunc;
    MPI_Datatype EX_INFO_STRUCT_TYPE;
    
    int my_rank;
		int num_processes;


    // windows ->3 windows are used. The entire hash_map entries are
    // stored in a dynamic window whereas two static windows are used 
    // for storing meta_data like min_key, min_value, size...
    
    // Dynamic window
    MPI_Win hashmap_win;
    // Static window for storing addition info like min value,
    // min key, max value, maxkey
    MPI_Win hash_table_extra_info_win;
    // Static window for storing size
    MPI_Win hash_table_size_win;

    // Array to be attached to static window.
    hash_table_extra_info_t *hash_table_extra_info_array;
    // Size to be attached to static window
    int hash_map_size;

		MPI_Aint alloc_element(node_t key, int value, MPI_Win win) {
			MPI_Aint disp;
			hash_node_t *elem_ptr;
		
      /* Allocate the new element and register it with the window */
			MPI_Alloc_mem(sizeof(hash_node_t), MPI_INFO_NULL, &elem_ptr);
			elem_ptr->key = key;
			elem_ptr->value = value;
			elem_ptr->next  = nil;
      elem_ptr->remove_status = false;


			MPI_Win_attach(win, elem_ptr, sizeof(hash_node_t));
			/* TODO: Add the element to the list of local elements so we can free
				 it later. */
			
      if (my_elems_size == my_elems_count) {
        my_elems_size += 100;
        my_elems = (hash_node_t **)realloc(my_elems, my_elems_size*sizeof(void*));
      }
      my_elems[my_elems_count] = elem_ptr;
      my_elems_count++;

			MPI_Get_address(elem_ptr, &disp);
			return disp;
		}

    // if key is present : p will be updated to point to node containing key and returns true,
    // else, p will point to the tail of the list and return false.
    // Note: The caller should take RMA lock.
    bool find_hash_node_ptr(hash_node_ptr_t& p, node_t key) {
      hash_node_ptr_t prev = p;

      while (p.rank != nil.rank) {
        node_t k;
        bool is_removed;
        // TODO: Combine the following four MPI_Get_accumulate calls.
        // We can get the whole structure pointed by p.disp in one call.
        MPI_Get_accumulate(NULL, 0, MPI_INT, // origin (unused)
            &is_removed, 1, MPI_C_BOOL,      // result
            p.rank, (MPI_Aint) &(((hash_node_t*)p.disp)->remove_status), // target
            1, MPI_C_BOOL, MPI_NO_OP, hashmap_win);
        MPI_Win_flush(p.rank, hashmap_win);
        if(!is_removed) {
          // Get key in k
          MPI_Get_accumulate(NULL, 0, MPI_INT, //origin (unused)
              &k, 1, MPI_INT, //result
              p.rank, (MPI_Aint) &(((hash_node_t*)p.disp)->key), // target
              1, MPI_INT, MPI_NO_OP, hashmap_win);
          MPI_Win_flush(p.rank, hashmap_win);
          //check key!=-1 is added to avoid checking for the dummy 
          //element ==> key can never be -1
          if(k != -1 && key == k) {
            return true;
          }
        }
        prev.rank = p.rank;
        prev.disp = p.disp;
        // Get its rank
        MPI_Get_accumulate(NULL, 0, MPI_INT, // origin (unused)
            &p.rank, 1, MPI_INT, // result
            prev.rank, (MPI_Aint) &(((hash_node_t*)prev.disp)->next.rank), // target
            1, MPI_INT, MPI_NO_OP, hashmap_win);
        MPI_Win_flush(prev.rank, hashmap_win);
        // Get its displacement
        // Assume the case two processes are concurrently trying to insert to same hashkey k for keys key0, key1.
        // Assume process 0 has executed find_hash_node_ptr, and found that key0 is not present in the hashmap.
        // Assume that process 0 has updated its rank to ptr.disp->next.rank but have not updated the disp.
        // Mean-while, let process 1 trying to insert key1 executes find_hash_node_ptr. Assume key1 is also
        // not present in hashmap. Now at this point it may see an updated rank but not an updated disp. 
        // So it will busy wait as below until process 0 has updated the disp. Busy waiting is done only if
        // process 1 sees a valid rank and an invalid disp.
        do {
          MPI_Get_accumulate(NULL, 0, MPI_INT, // origin (unused)
              &p.disp, 1, MPI_AINT, // result
              prev.rank, (MPI_Aint) &(((hash_node_t*)prev.disp)->next.disp), // target
              1, MPI_AINT, MPI_NO_OP, hashmap_win);
          MPI_Win_flush(prev.rank, hashmap_win);
        } while(p.rank != nil.rank && p.disp == nil.disp);
      }
      p.rank = prev.rank;
      p.disp = prev.disp;
      return false;
      //return data.find(key) != data.end();
    }

    // FIXME: The do while logic should be added to this function.
    //int insert_at_tail(hash_node_ptr_t new_elem_ptr) {
    //  hash_node_ptr_t next_tail_ptr = nil;
    //  /* nil.rank is compared with tail_ptr->next.rank, if it is equal, new_elem_ptr.rank
    //   * can be updated to tail_ptr->next.rank. The compared value of tail_ptr->next.rank
    //   * will be returned in next_tail_ptr.rank
    //   */
    //  MPI_Compare_and_swap((void*) &(new_elem_ptr.rank), (void*) &nil.rank,
    //      (void*)&(next_tail_ptr.rank), MPI_INT, tail_ptrs[hashValue].rank,
    //      (MPI_Aint) &(((hash_node_t*)tail_ptrs[hashValue].disp)->next.rank),
    //      hashmap_win);
    //  MPI_Win_flush(tail_ptrs[hashValue].rank, hashmap_win);
    //  /* If the next_tail_ptr.rank is equal to nil.rank, it implies that new_elem_ptr.rank
    //   * is updated to tail_ptr->next.rank
    //   */
    //  success = (next_tail_ptr.rank == nil.rank);
    //  if (success) {
    //    /* Accumulate disp to target.
    //     */
    //    MPI_Accumulate(&(new_elem_ptr.disp), 1, MPI_AINT, tail_ptrs[hashValue].rank,
    //        (MPI_Aint) &(((hash_node_t*)tail_ptrs[hashValue].disp)->next.disp), 1,
    //        MPI_AINT, MPI_REPLACE, hashmap_win);
    //    MPI_Win_flush(tail_ptrs[hashValue].rank, hashmap_win);
    //    /*tail pointer is updated on success*/
    //    tail_ptrs[hashValue] = new_elem_ptr;
    //  } else {
    //    /* Tail pointer is stale, fetch the displacement.  May take
    //       multiple tries if it is being updated. */
    //    /* If tail pointer is stale, process gets displacement of next entry and updates target_rank to
    //     * next_tail_ptr. A while loop is required because a successful process will have updated the rank
    //     * mean-while, but may not have updated the disp. So until disp has a valid value, we will have to loop.
    //     */
    //    do {
    //      MPI_Get_accumulate( NULL, 0, MPI_AINT, &next_tail_ptr.disp,
    //          1, MPI_AINT, tail_ptrs[hashValue].rank,
    //          (MPI_Aint) &(((hash_node_t*)tail_ptrs[hashValue].disp)->next.disp),
    //          1, MPI_AINT, MPI_NO_OP, hashmap_win);
    //      MPI_Win_flush(tail_ptrs[hashValue].rank, hashmap_win);
    //    } while (next_tail_ptr.disp == nil.disp);
    //    tail_ptrs[hashValue] = next_tail_ptr;
    //  }

    //  return success;
    //}

    
    
    // Note that this function updates ptr (because it is a reference), which
    // is required in the caller.
    // Note: caller should take the lock
    bool try_insert_after_ptr(hash_node_ptr_t new_elem_ptr, hash_node_ptr_t& ptr) {
      hash_node_ptr_t next_ptr = nil;
      /* nil.rank is compared with ptr->next.rank, if it is equal, new_elem_ptr.rank
       * can be updated to ptr->next.rank. The compared value of ptr->next.rank
       * will be returned in next_ptr.rank
       */
      MPI_Compare_and_swap((void*) &(new_elem_ptr.rank) /*origin_address*/,
          (void*) &nil.rank /* compare address*/,
          (void*)&(next_ptr.rank) /* result address */, MPI_INT,
          ptr.rank, (MPI_Aint) &(((hash_node_t*)ptr.disp)->next.rank) /* target_address */,
          hashmap_win);
      MPI_Win_flush(ptr.rank, hashmap_win);

      /* If the next_ptr.rank is equal to nil.rank, it implies that ptr->next.rank
       * is updated to new_elem_ptr.rank
       */
      bool success = (next_ptr.rank == nil.rank);
      if (success) {
        /* Accumulate disp to target.
         */
        MPI_Accumulate(&(new_elem_ptr.disp), 1, MPI_AINT, // origin
            ptr.rank, (MPI_Aint) &(((hash_node_t*)ptr.disp)->next.disp), // target
            1, MPI_AINT, MPI_REPLACE, hashmap_win);
        MPI_Win_flush(ptr.rank, hashmap_win);
        /*tail pointer is updated on success*/
        // Following is not required because insertion is success.
        ptr = new_elem_ptr;
      } else {
        /* ptr is stale, fetch the displacement.  May take
           multiple tries if it is being updated. */
        /* If ptr is stale, process gets displacement of next entry and updates target_rank to
         * next_tail_ptr. A while loop is required because a successful process will have updated the rank
         * mean-while, but may not have updated the disp. So until disp has a valid value, we will have to loop.
         */
        do {
          MPI_Get_accumulate(NULL, 0, MPI_AINT, // origin (unused)
              &next_ptr.disp, 1, MPI_AINT, // result
              ptr.rank, (MPI_Aint) &(((hash_node_t*)ptr.disp)->next.disp), // target
              1, MPI_AINT, MPI_NO_OP, hashmap_win);
          MPI_Win_flush(ptr.rank, hashmap_win);
        } while (next_ptr.disp == nil.disp);
        ptr = next_ptr;
      }

      return success;
    }
    
    void insert_value_at_ptr(node_t value, hash_node_ptr_t ptr) {
      MPI_Accumulate(&value, 1, MPI_INT, ptr.rank,
          (MPI_Aint) &(((hash_node_t*)ptr.disp)->value), 1,
          MPI_INT, MPI_REPLACE, hashmap_win);
      MPI_Win_flush(ptr.rank, hashmap_win);
    }

    int get_and_insert_value_at_ptr(node_t value, hash_node_ptr_t ptr) {
      int old_val;  
      MPI_Get_accumulate(&value, 1, MPI_INT,
          &old_val, 1, MPI_INT,
          ptr.rank, (MPI_Aint) &(((hash_node_t*)ptr.disp)->value), 1,
          MPI_INT, MPI_SUM, hashmap_win);
      MPI_Win_flush(ptr.rank, hashmap_win);
      return old_val;
    }

    int get_value_at_ptr( hash_node_ptr_t ptr) {
      int value;  
      MPI_Get(&value, 1, MPI_INT,
          ptr.rank, (MPI_Aint) &(((hash_node_t*)ptr.disp)->value), 1,
          MPI_INT, hashmap_win);
      MPI_Win_flush(ptr.rank, hashmap_win);
      return value;
    }

    // TODO: May be changed to shared lock (read lock).
    void invalidate_min_max_entries(node_t key) {
      int hash_value = hashFunc(key);

      // Update min key
      MPI_Win_lock(MPI_LOCK_EXCLUSIVE, MASTER, 0, hash_table_extra_info_win);
      hash_table_extra_info_t hash_table_extra_info;
      MPI_Get(&hash_table_extra_info, 1, EX_INFO_STRUCT_TYPE, MASTER, hash_value, 1,
              EX_INFO_STRUCT_TYPE, hash_table_extra_info_win);
      MPI_Win_flush(MASTER, hash_table_extra_info_win);
      bool is_changed = false;

      // Update min key is_valid
      if (hash_table_extra_info.is_min_key_valid &&
          key == hash_table_extra_info.min_key_key) {
        is_changed = true;
        hash_table_extra_info.is_min_key_valid = false;
      }

      // Update min value is_valid 
      if (hash_table_extra_info.is_min_value_valid &&
          key == hash_table_extra_info.min_value_key) {
        is_changed = true;
        hash_table_extra_info.is_min_value_valid = false;
      }
      
      // Update max key is_valid 
      if (hash_table_extra_info.is_max_key_valid &&
          key == hash_table_extra_info.max_key_key) {
        is_changed = true;
        hash_table_extra_info.is_max_key_valid = false;
      }

      // Update max value is_valid 
      if (hash_table_extra_info.is_max_value_valid &&
          key == hash_table_extra_info.max_value_key) {
        is_changed = true;
        hash_table_extra_info.is_max_value_valid = false;
      }

      if (is_changed) {
        is_changed = false;
        MPI_Put(&hash_table_extra_info, 1, EX_INFO_STRUCT_TYPE, MASTER, hash_value, 1,
              EX_INFO_STRUCT_TYPE, hash_table_extra_info_win);
      }

      MPI_Win_unlock(MASTER, hash_table_extra_info_win);
    }

    void update_hash_table_extra_info(node_t key, int value) {
      int hash_value = hashFunc(key);

      // Update min key
      // TODO: May be changed to shared lock (read lock).
      // Done: Changed to read lock. If there is a read while doing an
      // update it is a race in the input program itself.
      //MPI_Win_lock(MPI_LOCK_EXCLUSIVE, MASTER, 0, hash_table_extra_info_win);
      MPI_Win_lock_all(0, hash_table_extra_info_win);
      hash_table_extra_info_t hash_table_extra_info;
      MPI_Get(&hash_table_extra_info, 1, EX_INFO_STRUCT_TYPE, MASTER, hash_value, 1,
              EX_INFO_STRUCT_TYPE, hash_table_extra_info_win);
      MPI_Win_flush(MASTER, hash_table_extra_info_win);
      bool is_changed = false;

      // Update min key
      if (hash_table_extra_info.is_min_key_valid && key <
          hash_table_extra_info.min_key_key) {
        is_changed = true;
        hash_table_extra_info.min_key_key = key;
        hash_table_extra_info.min_key_value = value;
      }

      // Update min value 
      if (hash_table_extra_info.is_min_value_valid && value <
          hash_table_extra_info.min_value_value) {
        is_changed = true;
        hash_table_extra_info.min_value_key = key;
        hash_table_extra_info.min_value_value = value;
      }
      
      // Update max key 
      if (hash_table_extra_info.is_max_key_valid && key
          > hash_table_extra_info.max_key_key) {
        is_changed = true;
        hash_table_extra_info.max_key_key = key;
        hash_table_extra_info.max_key_value = value;
      }

      // Update max value 
      if (hash_table_extra_info.is_max_value_valid && value >
          hash_table_extra_info.max_value_value) {
        is_changed = true;
        hash_table_extra_info.max_value_key = key;
        hash_table_extra_info.max_value_value = value;
      }

      if (is_changed) {
        is_changed = false;
        MPI_Put(&hash_table_extra_info, 1, EX_INFO_STRUCT_TYPE, MASTER, hash_value, 1,
              EX_INFO_STRUCT_TYPE, hash_table_extra_info_win);
      }

      MPI_Win_unlock_all(hash_table_extra_info_win);
    }
    //template<class Function>
		//Value getValue_generic(Function compare) {
    //    assert(size() > 0);
    //    Iterator iter = data.begin();
    //    Value value = iter->second;
    //    for (iter++; iter != data.end(); iter++) {
    //        if (compare(iter->second, value)) {
    //            value = iter->second;
    //        }
    //    }
    //    return value;
    //}

    //template<class Function>
    //Key getKey_generic(Function compare) {
    //    assert(size() > 0);
    //    Iterator iter = data.begin();
    //    Key key = iter->first;
    //    Value value = iter->second;
    //    for (iter++; iter != data.end(); iter++) {
    //        if (compare(iter->second, value)) {
    //            key = iter->first;
    //            value = iter->second;
    //        }
    //    }
    //    return key;
    //}

    //template<class Function>
    //bool hasValue_generic(Function compare, const Key key) {
    //    if (size() == 0 || !hasKey_seq(key)) return false;
    //    Value value = data[key];
    //    bool result = true;
    //    for (Iterator iter = data.begin(); iter != data.end(); iter++) {
    //        if (compare(iter->second, value)) result = false;
    //    }
    //    return result;
    //}

  //protected:
   // Value getDefaultValue() {
   //     return defaultValue;
   // }

    void init_hash_table_extra_info() {
      if (my_rank == MASTER) {
        int i;
        for(i = 0; i < TABLE_SIZE; i++) {
          hash_table_extra_info_array[i].min_key_key = INT_MAX;
          hash_table_extra_info_array[i].is_min_key_valid = true;

          hash_table_extra_info_array[i].min_value_value = INT_MAX;
          hash_table_extra_info_array[i].is_min_value_valid = true;

          hash_table_extra_info_array[i].max_key_key = INT_MIN;
          hash_table_extra_info_array[i].is_max_key_valid = true;

          hash_table_extra_info_array[i].max_value_value = INT_MIN;
          hash_table_extra_info_array[i].is_max_value_valid = true;
        }
        
      }
      MPI_Barrier(MPI_COMM_WORLD);
    }


   void increment_size() {
      MPI_Win_lock(MPI_LOCK_EXCLUSIVE, MASTER, 0, hash_table_size_win);
      
      int incr_val = 1;
      MPI_Accumulate(&incr_val, 1, MPI_INT, MASTER, 0, 1, MPI_INT, MPI_SUM, hash_table_size_win);

      MPI_Win_unlock(MASTER, hash_table_size_win);
   }

   void decrement_size() {
      MPI_Win_lock(MPI_LOCK_EXCLUSIVE, MASTER, 0, hash_table_size_win);
      int size;
      MPI_Get(&size, 1, MPI_INT, MASTER, 0, 1, MPI_INT, hash_table_size_win);
      MPI_Win_flush(MASTER, hash_table_size_win);
      size--;
      MPI_Put(&size, 1, MPI_INT, MASTER, 0, 1, MPI_INT, hash_table_size_win);

      MPI_Win_unlock(MASTER, hash_table_size_win);
   }

   void create_hash_table_extra_info_struct_type()
   {
      MPI_Datatype oldtypes[2];
      int blockcounts[2];

      MPI_Aint offsets[2], extent;

			// setup description of the 4 MPI_FLOAT fields x, y, z, velocity
			offsets[0] = 0;
			oldtypes[0] = MPI_INT;
			blockcounts[0] = 8;

			// setup description of the 2 MPI_INT fields n, type
			// need to first figure offset by getting size of MPI_FLOAT
			MPI_Type_extent(MPI_INT, &extent);
			offsets[1] = 8 * extent;
			oldtypes[1] = MPI_C_BOOL;
			blockcounts[1] = 4;

			// define structured type and commit it
			MPI_Type_struct(2, blockcounts, offsets, oldtypes, &EX_INFO_STRUCT_TYPE);
			MPI_Type_commit(&EX_INFO_STRUCT_TYPE); 
			MPI_Barrier(MPI_COMM_WORLD);		
	 }

	 //FIXME: locks hash_table_extra_info and tries to lock hashmap_win ==> check if there is a dead-lock.
   // Caller should take lock on both hashmap_win and hash_table_extra_info_win
	 void traverse_and_update_hash_table_extra_info(int i) {

		 //p is initialized
		 hash_node_ptr_t p = head_ptrs[i];

		 //initializations
		 hash_table_extra_info_t hash_table_extra_info;
		 hash_table_extra_info.min_key_key = MAX_VALUE;
		 hash_table_extra_info.max_key_key = MIN_VALUE;
		 hash_table_extra_info.min_value_value = MAX_VALUE;
		 hash_table_extra_info.max_value_value = MIN_VALUE;
		 hash_table_extra_info.is_min_key_valid = true;
		 hash_table_extra_info.is_max_value_valid = true;
		 hash_table_extra_info.is_min_value_valid = true;
		 hash_table_extra_info.is_max_value_valid = true;

		 while (p.rank != nil.rank) {
			 node_t k;
			 int value;
			 bool is_removed;

			 MPI_Get_accumulate(NULL, 0, MPI_INT,
           &is_removed, 1, MPI_INT,
           p.rank, (MPI_Aint) &(((hash_node_t*)p.disp)->remove_status),
					 1, MPI_C_BOOL, MPI_NO_OP, hashmap_win);
			 MPI_Win_flush(p.rank, hashmap_win);

			 //check only if the node is an non-deleted one
			 if(!is_removed) {

				 // Get key in k
				 MPI_Get_accumulate(NULL, 0, MPI_INT,
             &k, 1, MPI_INT,
             p.rank, (MPI_Aint) &(((hash_node_t*)p.disp)->key),
						 1, MPI_INT, MPI_NO_OP, hashmap_win);
				 MPI_Get_accumulate( NULL, 0, MPI_INT,
             &value, 1, MPI_INT,
             p.rank, (MPI_Aint) &(((hash_node_t*)p.disp)->value),
						 1, MPI_INT, MPI_NO_OP, hashmap_win);
				 MPI_Win_flush(p.rank, hashmap_win);

				 //check key!=-1 is added to avoid checking for the dummy 
				 //element ==> key can never be -1
				 //update hash_table_extra_info accordingly
				 if(k != -1) {
					 if(k < hash_table_extra_info.min_key_key) {
						 hash_table_extra_info.min_key_key = k;
						 hash_table_extra_info.min_key_value = value; 
					 }
					 if(k > hash_table_extra_info.max_key_key) {
						 hash_table_extra_info.max_key_key = k;
						 hash_table_extra_info.max_key_value = value; 
					 }
					 if(value < hash_table_extra_info.min_value_value) {
						 hash_table_extra_info.min_value_key = k;
						 hash_table_extra_info.min_value_value = value; 
					 }
					 if(value > hash_table_extra_info.max_value_value) {
						 hash_table_extra_info.max_value_key = k;
						 hash_table_extra_info.max_value_value = value; 
					 }
				 }
			 }

			 int p_rank = p.rank;
			 MPI_Aint p_disp = p.disp;

			 // Get its rank
			 MPI_Get_accumulate(NULL, 0, MPI_INT,
           &p.rank, 1, MPI_INT,
           p_rank, (MPI_Aint) &(((hash_node_t*)p_disp)->next.rank),
					 1, MPI_INT, MPI_NO_OP, hashmap_win);
			 MPI_Win_flush(p_rank, hashmap_win);

			 // Get its displacement
			 // Assume the case two processes are concurrently trying to insert to same hashkey k for keys key0, key1.
			 // Assume process 0 has executed find_hash_node_ptr, and found that key0 is not present in the hashmap.
			 // Assume that process 0 has updated its rank to ptr.disp->next.rank but have not updated the disp.
			 // Mean-while, let process 1 trying to insert key1 executes find_hash_node_ptr. Assume key1 is also
			 // not present in hashmap. Now at this point it may see an updated rank but not an updated disp. 
			 // So it will busy wait as below until process 0 has updated the disp. Busy waiting is done only if
			 // process 1 sees a valid rank and an invalid disp.
			 do {
				 MPI_Get_accumulate( NULL, 0, MPI_INT,
             &p.disp, 1, MPI_AINT,
             p_rank, (MPI_Aint) &(((hash_node_t*)p_disp)->next.disp),
						 1, MPI_AINT, MPI_NO_OP, hashmap_win);
				 MPI_Win_flush(p_rank, hashmap_win);
			 } while(p.rank != nil.rank && p.disp == nil.disp);
		 }

		 //update the ith location of hash_table_extra_info_array at MASTER 
		 MPI_Put(&hash_table_extra_info, 1, EX_INFO_STRUCT_TYPE, MASTER, i, 1,
				 EX_INFO_STRUCT_TYPE, hash_table_extra_info_win);
		 return ;
	 }

  public:
    gm_map_node_int(){
      int i;
      MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
      MPI_Comm_size(MPI_COMM_WORLD, &num_processes);

      MPI_Win_create_dynamic(MPI_INFO_NULL, MPI_COMM_WORLD, &hashmap_win);

      // Create the static windows for holding min and max values
      int table_size = 0;
      if (my_rank == MASTER) {
        table_size = TABLE_SIZE;
      }

      MPI_Win_allocate(table_size * sizeof(hash_table_extra_info_t),
                       sizeof(hash_table_extra_info_t),
                       MPI_INFO_NULL, MPI_COMM_WORLD, &hash_table_extra_info_array,
                       &hash_table_extra_info_win);
      hash_map_size=0;
      MPI_Win_create(&hash_map_size, 1 * sizeof(int), 
          sizeof(int),MPI_INFO_NULL, MPI_COMM_WORLD, &hash_table_size_win);


      // Initialize hash_table_extra_info_array 
      init_hash_table_extra_info();

      /* initialize head and tail pointers to nil*/
      /* Initially all head and tail pointers for each hashValue is
       * created by MASTER.
       */
      MPI_Win_lock_all(0, hashmap_win);
      if(my_rank == MASTER) {
        for(i=0; i<TABLE_SIZE; i++) {
            head_ptrs[i].disp = alloc_element(-1, -1, hashmap_win);
        }
      }

      MPI_Win_unlock_all(hashmap_win);
			/* Broadcast the head pointers to all processes.
       */
			for(i=0; i<TABLE_SIZE; i++) {
				MPI_Bcast(&head_ptrs[i].disp, 1, MPI_AINT, 0, MPI_COMM_WORLD );
				head_ptrs[i].rank = MASTER;
				/* Initially set the tail pointers as head pointers.
				 */ 
			}

			create_hash_table_extra_info_struct_type();

		}

    ~gm_map_node_int() {
      //MPI_Barrier(MPI_COMM_WORLD);
      //MPI_Free_mem(hash_table_extra_info_array);
      /* Free all the elements in the list */
      //for ( ; my_elems_count > 0; my_elems_count--) {
        //MPI_Win_detach(hashmap_win,my_elems[my_elems_count-1]);
        //MPI_Free_mem(my_elems[my_elems_count-1]);
      //}
    }

    bool hasKey_seq(const node_t key) {
      bool found = false;
      if(my_rank == MASTER) {
        found = hasKey_par(key);
      }
      
      MPI_Bcast(&found, 1, MPI_C_BOOL, MASTER, MPI_COMM_WORLD);
      return found;
    }

    bool hasKey_par(const node_t key) {
      
      MPI_Win_lock_all(0, hashmap_win);
      int hashValue = hashFunc(key);

      hash_node_ptr_t p = head_ptrs[hashValue];

      bool found = find_hash_node_ptr(p, key);

      MPI_Win_unlock_all(hashmap_win);
      return found;
    }

    //Value getValue(const Key key) {
    //    if (hasKey_seq(key)) {
    //        return data[key];
    //    } else {
    //        return defaultValue;
    //    }
    //}

    void setValue_par(node_t key, int value) {
	      MPI_Win_lock_all(0, hashmap_win);
        
       unsigned long hashValue = hashFunc(key);
        hash_node_ptr_t new_elem_ptr, ptr;
        new_elem_ptr.disp = alloc_element(key, value, hashmap_win);
        new_elem_ptr.rank = my_rank;


				bool success = false;
        ptr = head_ptrs[hashValue];
				do {
          // Passing ptr as reference. So it will be updated.
          bool found_key = find_hash_node_ptr(ptr, key);
          if (!found_key) {
            success = try_insert_after_ptr(new_elem_ptr, ptr);
            if (success == true) {
              // increment_size should take lock on hash_table_size_win.
              increment_size();
            }
          } else {
            // If there is a concurrent remove happening, that is a race condition
            // induced by the input program itself.
            // In that case, it may reset the value of a deleted node (as we follow
            // a flag-based deletion). However, it is ensured that a race will not 
            // end in a seg-fault.
            success = true;
            insert_value_at_ptr(value, ptr);
            // Here the new element pointer is not required so free
            // the memory allocated in disp. 
            free((hash_node_t *)(new_elem_ptr.disp));
          }
				} while (!success);
        
        // FIXME: Check if this is function should be called
        // here.
        update_hash_table_extra_info(key, value);

	      MPI_Win_unlock_all(hashmap_win);
        //try to insert value to tail_ptr of hashValue
      //data[key] = value;
    }

    void setValue_seq(node_t key, int value) {
        if(my_rank == MASTER) {
          setValue_par(key, value);
        }
        MPI_Barrier(MPI_COMM_WORLD);
    }

    void removeKey_par(node_t key) {

      MPI_Win_lock_all(0, hashmap_win);
      int hashValue = hashFunc(key);
      hash_node_ptr_t p = head_ptrs[hashValue];

      while (p.rank != nil.rank) {
        node_t k;
        bool is_removed;
        // Get key in k
        // TODO: Combine the following MPI_Get_accumulate calls.
        // We can get the whole structure pointed by p.disp in one call.
        MPI_Get_accumulate(NULL, 0, MPI_INT, // origin (unused)
            &is_removed, 1, MPI_C_BOOL, // result
            p.rank, (MPI_Aint) &(((hash_node_t*)p.disp)->remove_status), // target
            1, MPI_C_BOOL, MPI_NO_OP, hashmap_win);
        MPI_Win_flush(p.rank, hashmap_win);
        if(!is_removed)
        {
          MPI_Get_accumulate(NULL, 0, MPI_INT, // origin (unused)
              &k, 1, MPI_INT, // result
              p.rank, (MPI_Aint) &(((hash_node_t*)p.disp)->key), //target
              1, MPI_INT, MPI_NO_OP, hashmap_win);
          MPI_Win_flush(p.rank, hashmap_win);
        //check key!=-1 is added to avoid checking for the dummy 
        //element ==> key can never be -1
          if(k != -1 && key == k) {
            bool new_remove_status = true;
            bool compare_remove_status = false;
            bool return_remove_status;
            MPI_Compare_and_swap((void*) &(new_remove_status),  // origin address
                (void*) &compare_remove_status, // compare address
                (void*)&(return_remove_status), MPI_C_BOOL, // result address
                p.rank, (MPI_Aint) &(((hash_node_t*)p.disp)->remove_status), // target
                hashmap_win);
            MPI_Win_flush(p.rank, hashmap_win);

            // If the key is removed invalidate the min and max enrties
            // in the hash_table_extra_info.
            // This is done when the removal is succeeded.
            if (return_remove_status == false) { // condition for successful removal
              decrement_size();
              invalidate_min_max_entries(key);
            }
            break;
          }
        }
        int p_rank = p.rank;
        MPI_Aint p_disp = p.disp;
        // Get its rank
        MPI_Get_accumulate(NULL, 0, MPI_INT,
            &p.rank, 1, MPI_INT,
            p_rank, (MPI_Aint) &(((hash_node_t*)p_disp)->next.rank),
            1, MPI_INT, MPI_NO_OP, hashmap_win);
        MPI_Win_flush(p_rank, hashmap_win);
        // Get its displacement
        // Assume the case two processes are concurrently trying to insert to same hashkey k for keys key0, key1.
        // Assume process 0 has executed find_hash_node_ptr, and found that key0 is not present in the hashmap.
        // Assume that process 0 has updated its rank to ptr.disp->next.rank but have not updated the disp.
        // Mean-while, let process 1 trying to insert key1 executes find_hash_node_ptr. Assume key1 is also
        // not present in hashmap. Now at this point it may see an updated rank but not an updated disp. 
        // So it will busy wait as below until process 0 has updated the disp. Busy waiting is done only if
        // process 1 sees a valid rank and an invalid disp.
        do {
          MPI_Get_accumulate(NULL, 0, MPI_INT, &p.disp,
              1, MPI_AINT, p_rank,
              (MPI_Aint) &(((hash_node_t*)p_disp)->next.disp),
              1, MPI_AINT, MPI_NO_OP, hashmap_win);
          MPI_Win_flush(p_rank, hashmap_win);
        } while(p.rank != nil.rank && p.disp == nil.disp);
      }
      MPI_Win_unlock_all(hashmap_win);
    }


    void removeKey_seq(node_t key) {
      if(my_rank == MASTER) {
        removeKey_par(key);
      }
      MPI_Barrier(MPI_COMM_WORLD);
    }

    bool hasMaxValue_par(node_t key) {
      if (size() == 0) {
        return true;
      }
      int i;
      node_t max_value = MIN_VALUE;

      MPI_Win_lock_all(0, hash_table_extra_info_win);
      for (i = 0; i < TABLE_SIZE; i++) {
        //computing the hash_table_extra_info_array is made parallel
        hash_table_extra_info_t hash_table_extra_info;
        MPI_Get(&hash_table_extra_info, 1, EX_INFO_STRUCT_TYPE, MASTER, i, 1,
            EX_INFO_STRUCT_TYPE, hash_table_extra_info_win);
        MPI_Win_flush(MASTER, hash_table_extra_info_win);
        if (hash_table_extra_info.is_max_value_valid == true) {
          continue;
        } else {
          MPI_Win_lock_all(0, hashmap_win);
          traverse_and_update_hash_table_extra_info(i);
          MPI_Win_unlock_all(hashmap_win);
        }
      }

      MPI_Win_unlock_all(hash_table_extra_info_win);
      MPI_Barrier(MPI_COMM_WORLD);

      MPI_Win_lock(MPI_LOCK_EXCLUSIVE, MASTER, 0, hash_table_extra_info_win);

      // FIXME: Here we are copying the entire hash_table_extra_info_array. 
      // It may be efficient to perform individual MPI_Gets to find the max_value.
      hash_table_extra_info_t hash_table_extra_info_array_copy[TABLE_SIZE];
      MPI_Get(hash_table_extra_info_array_copy, TABLE_SIZE, EX_INFO_STRUCT_TYPE, MASTER, 0, TABLE_SIZE,
              EX_INFO_STRUCT_TYPE, hash_table_extra_info_win);
      MPI_Win_flush(MASTER, hash_table_extra_info_win);

      node_t max_value_key;
      for (i = 0; i <TABLE_SIZE; i++) {
        if (hash_table_extra_info_array_copy[i].max_value_value > max_value) {
          max_value = hash_table_extra_info_array_copy[i].max_value_value;
          max_value_key = hash_table_extra_info_array_copy[i].max_value_key;
        }
      }

      MPI_Win_unlock(MASTER, hash_table_extra_info_win);
      // even if an insertion happens concurrently at this point, as it in sequential
      // region, the insertion must be a statement after getMaxValue_seq(). 
      // In that case even if there is concurrency at this point, the right value of max_value
      // will be returned.

      if (max_value_key == key) {
        return true;
      }
      return false;
    }


    bool hasMaxValue_seq(node_t key) {
      if (size() == 0) {
        return true;
      }
      int i;
      node_t max_value = MIN_VALUE;

      MPI_Win_lock_all(0, hash_table_extra_info_win);
      for (i = 0; i < TABLE_SIZE; i++) {
        //computing the hash_table_extra_info_array is made parallel
        if ((i % num_processes) == my_rank) {
          hash_table_extra_info_t hash_table_extra_info;
          MPI_Get(&hash_table_extra_info, 1, EX_INFO_STRUCT_TYPE, MASTER, i, 1,
              EX_INFO_STRUCT_TYPE, hash_table_extra_info_win);
          MPI_Win_flush(MASTER, hash_table_extra_info_win);
          if (hash_table_extra_info.is_max_value_valid == true) {
            continue;
          } else {
            MPI_Win_lock_all(0, hashmap_win);
            traverse_and_update_hash_table_extra_info(i);
            MPI_Win_unlock_all(hashmap_win);
          }
        }
      }

      MPI_Win_unlock_all(hash_table_extra_info_win);
      MPI_Barrier(MPI_COMM_WORLD);

      MPI_Win_lock(MPI_LOCK_EXCLUSIVE, MASTER, 0, hash_table_extra_info_win);
      node_t max_value_key;
      if (my_rank == MASTER) {
        MPI_Win_sync(hash_table_extra_info_win);
        for (i = 0; i <TABLE_SIZE; i++) {
          if (hash_table_extra_info_array[i].max_value_value > max_value) {
            max_value = hash_table_extra_info_array[i].max_value_value;
            max_value_key = hash_table_extra_info_array[i].max_value_key;
          }
        }
      }
      MPI_Win_unlock(MASTER, hash_table_extra_info_win);
      // even if an insertion happens concurrently at this point, as it in sequential
      // region, the insertion must be a statement after getMaxValue_seq(). 
      // In that case even if there is concurrency at this point, the right value of max_value
      // will be returned.
      MPI_Bcast(&max_value_key, 1, MPI_INT, MASTER, MPI_COMM_WORLD);

      if (max_value_key == key) {
        return true;
      }
      return false;
    }

    bool hasMinValue_par(node_t key) {
      if (size() == 0) {
        return true;
      }
      int i;
      node_t min_value = MAX_VALUE;

      MPI_Win_lock_all(0, hash_table_extra_info_win);
      for (i = 0; i < TABLE_SIZE; i++) {
        hash_table_extra_info_t hash_table_extra_info;
        MPI_Get(&hash_table_extra_info, 1, EX_INFO_STRUCT_TYPE, MASTER, i, 1,
            EX_INFO_STRUCT_TYPE, hash_table_extra_info_win);
        MPI_Win_flush(MASTER, hash_table_extra_info_win);
        if (hash_table_extra_info.is_min_value_valid == true) {
          continue;
        } else {
          MPI_Win_lock_all(0, hashmap_win);
          traverse_and_update_hash_table_extra_info(i);
          MPI_Win_unlock_all(hashmap_win);
        }
      }

      MPI_Win_unlock_all(hash_table_extra_info_win);
      MPI_Barrier(MPI_COMM_WORLD);

      MPI_Win_lock(MPI_LOCK_EXCLUSIVE, MASTER, 0, hash_table_extra_info_win);
      // It may be efficient to perform individual MPI_Gets to find the max_value.
      hash_table_extra_info_t hash_table_extra_info_array_copy[TABLE_SIZE];
      MPI_Get(hash_table_extra_info_array_copy, TABLE_SIZE, EX_INFO_STRUCT_TYPE, MASTER, 0, TABLE_SIZE,
              EX_INFO_STRUCT_TYPE, hash_table_extra_info_win);
      MPI_Win_flush(MASTER, hash_table_extra_info_win);

      node_t min_value_key;
      for (i = 0; i <TABLE_SIZE; i++) {
        if (hash_table_extra_info_array_copy[i].min_value_value < min_value) {
          min_value = hash_table_extra_info_array_copy[i].min_value_value;
          min_value_key = hash_table_extra_info_array_copy[i].min_value_key;
        }
      }
      MPI_Win_unlock(MASTER, hash_table_extra_info_win);
      // even if an insertion happens concurrently at this point, as it in sequential
      // region, the insertion must be a statement after getMinValue_seq(). 
      // In that case even if there is concurrency at this point, the right value of min_value
      // will be returned.

      if (min_value_key == key) {
        return true;
      }
      return false;
    }

    bool hasMinValue_seq(node_t key) {
      if (size() == 0) {
        return true;
      }
      int i;
      node_t min_value = MAX_VALUE;

      MPI_Win_lock_all(0, hash_table_extra_info_win);
      for (i = 0; i < TABLE_SIZE; i++) {
        //computing the hash_table_extra_info_array is made parallel
        if ((i % num_processes) == my_rank) {
          hash_table_extra_info_t hash_table_extra_info;
          MPI_Get(&hash_table_extra_info, 1, EX_INFO_STRUCT_TYPE, MASTER, i, 1,
              EX_INFO_STRUCT_TYPE, hash_table_extra_info_win);
          MPI_Win_flush(MASTER, hash_table_extra_info_win);
          if (hash_table_extra_info.is_min_value_valid == true) {
            continue;
          } else {
            MPI_Win_lock_all(0, hashmap_win);
            traverse_and_update_hash_table_extra_info(i);
            MPI_Win_unlock_all(hashmap_win);
          }
        }
      }

      MPI_Win_unlock_all(hash_table_extra_info_win);
      MPI_Barrier(MPI_COMM_WORLD);

      MPI_Win_lock(MPI_LOCK_EXCLUSIVE, MASTER, 0, hash_table_extra_info_win);
      node_t min_value_key;
      if (my_rank == MASTER) {
        MPI_Win_sync(hash_table_extra_info_win);
        for (i = 0; i <TABLE_SIZE; i++) {
          if (hash_table_extra_info_array[i].min_value_value < min_value) {
            min_value = hash_table_extra_info_array[i].min_value_value;
            min_value_key = hash_table_extra_info_array[i].min_value_key;
          }
        }
      }
      MPI_Win_unlock(MASTER, hash_table_extra_info_win);
      // even if an insertion happens concurrently at this point, as it in sequential
      // region, the insertion must be a statement after getMinValue_seq(). 
      // In that case even if there is concurrency at this point, the right value of min_value
      // will be returned.
      MPI_Bcast(&min_value_key, 1, MPI_INT, MASTER, MPI_COMM_WORLD);

      if (min_value_key == key) {
        return true;
      }
      return false;
    }

    node_t getMaxKey_par() {
      if (size() == 0) {
        return -1;
      }
      int i;
      node_t max_key = MIN_VALUE;

      MPI_Win_lock_all(0, hash_table_extra_info_win);
      for (i = 0; i < TABLE_SIZE; i++) {
        hash_table_extra_info_t hash_table_extra_info;
        MPI_Get(&hash_table_extra_info, 1, EX_INFO_STRUCT_TYPE, MASTER, i, 1,
            EX_INFO_STRUCT_TYPE, hash_table_extra_info_win);
        MPI_Win_flush(MASTER, hash_table_extra_info_win);
        if (hash_table_extra_info.is_max_key_valid == true) {
          continue;
        } else {
          MPI_Win_lock_all(0, hashmap_win);
          traverse_and_update_hash_table_extra_info(i);
          MPI_Win_unlock_all(hashmap_win);
        }
      }

      MPI_Win_unlock_all(hash_table_extra_info_win);

      MPI_Barrier(MPI_COMM_WORLD);

      MPI_Win_lock(MPI_LOCK_EXCLUSIVE, MASTER, 0, hash_table_extra_info_win);
 
      // FIXME: Here we are copying the entire hash_table_extra_info_array. 
      // It may be efficient to perform individual MPI_Gets to find the max_value.
      hash_table_extra_info_t hash_table_extra_info_array_copy[TABLE_SIZE];
      MPI_Get(hash_table_extra_info_array_copy, TABLE_SIZE, EX_INFO_STRUCT_TYPE, MASTER, 0, TABLE_SIZE,
              EX_INFO_STRUCT_TYPE, hash_table_extra_info_win);
      MPI_Win_flush(MASTER, hash_table_extra_info_win);

      for (i = 0; i <TABLE_SIZE; i++) {
        if (hash_table_extra_info_array_copy[i].max_key_key > max_key) {
          max_key = hash_table_extra_info_array_copy[i].max_key_key;
        }
      }
      MPI_Win_unlock(MASTER, hash_table_extra_info_win);
      // even if an insertion happens concurrently at this point, as it in sequential
      // region, the insertion must be a statement after getMaxKey_seq(). 
      // In that case even if there is concurrency at this point, the right value of max_key
      // will be returned.

      return max_key;
    }

    node_t getMaxKey_seq() {
      if (size() == 0) {
        return -1;
      }
      int i;
      node_t max_key = MIN_VALUE;

      MPI_Win_lock_all(0, hash_table_extra_info_win);
      for (i = 0; i < TABLE_SIZE; i++) {
        //computing the hash_table_extra_info_array is made parallel
        if ((i % num_processes) == my_rank) {
          hash_table_extra_info_t hash_table_extra_info;
          MPI_Get(&hash_table_extra_info, 1, EX_INFO_STRUCT_TYPE, MASTER, i, 1,
              EX_INFO_STRUCT_TYPE, hash_table_extra_info_win);
          MPI_Win_flush(MASTER, hash_table_extra_info_win);
          if (hash_table_extra_info.is_max_key_valid == true) {
            continue;
          } else {
            MPI_Win_lock_all(0, hashmap_win);
            traverse_and_update_hash_table_extra_info(i);
            MPI_Win_unlock_all(hashmap_win);
          }
        }
      }

      MPI_Win_unlock_all(hash_table_extra_info_win);

      MPI_Barrier(MPI_COMM_WORLD);

      MPI_Win_lock(MPI_LOCK_EXCLUSIVE, MASTER, 0, hash_table_extra_info_win);
      if (my_rank == MASTER) {
        MPI_Win_sync(hash_table_extra_info_win);
        for (i = 0; i <TABLE_SIZE; i++) {
          if (hash_table_extra_info_array[i].max_key_key > max_key) {
            max_key = hash_table_extra_info_array[i].max_key_key;
          }
        }
      }
      MPI_Win_unlock(MASTER, hash_table_extra_info_win);
      // even if an insertion happens concurrently at this point, as it in sequential
      // region, the insertion must be a statement after getMaxKey_seq(). 
      // In that case even if there is concurrency at this point, the right value of max_key
      // will be returned.
      MPI_Bcast(&max_key, 1, MPI_INT, MASTER, MPI_COMM_WORLD);

      return max_key;
    }

    node_t getMinKey_par() {
      if (size() == 0) {
        return -1;
      }
      int i;
      node_t min_key = MAX_VALUE;

      MPI_Win_lock_all(0, hash_table_extra_info_win);
      for (i = 0; i < TABLE_SIZE; i++) {
        hash_table_extra_info_t hash_table_extra_info;
        MPI_Get(&hash_table_extra_info, 1, EX_INFO_STRUCT_TYPE, MASTER, i, 1,
            EX_INFO_STRUCT_TYPE, hash_table_extra_info_win);
        MPI_Win_flush(MASTER, hash_table_extra_info_win);
        if (hash_table_extra_info.is_min_key_valid == true) {
          continue;
        } else {
          MPI_Win_lock_all(0, hashmap_win);
          traverse_and_update_hash_table_extra_info(i);
          MPI_Win_unlock_all(hashmap_win);
        }
      }

      MPI_Win_unlock_all(hash_table_extra_info_win);
      MPI_Barrier(MPI_COMM_WORLD);

      MPI_Win_lock(MPI_LOCK_EXCLUSIVE, MASTER, 0, hash_table_extra_info_win);

      // FIXME: Here we are copying the entire hash_table_extra_info_array. 
      // It may be efficient to perform individual MPI_Gets to find the max_value.
      hash_table_extra_info_t hash_table_extra_info_array_copy[TABLE_SIZE];
      MPI_Get(hash_table_extra_info_array_copy, TABLE_SIZE, EX_INFO_STRUCT_TYPE, MASTER, 0, TABLE_SIZE,
              EX_INFO_STRUCT_TYPE, hash_table_extra_info_win);
      MPI_Win_flush(MASTER, hash_table_extra_info_win);

      for (i = 0; i <TABLE_SIZE; i++) {
        if (hash_table_extra_info_array_copy[i].min_key_key < min_key) {
          min_key = hash_table_extra_info_array_copy[i].min_key_key;
        }
      }
      MPI_Win_unlock(MASTER, hash_table_extra_info_win);
      // even if an insertion happens concurrently at this point, as it in sequential
      // region, the insertion must be a statement after getMinKey_seq(). 
      // In that case even if there is concurrency at this point, the right value of min_key
      // will be returned.

      return min_key;
    }

    node_t getMinKey_seq() {
      if (size() == 0) {
        return -1;
      }
      int i;
      //node_t min_key = MAX_VALUE;
      int min_key = MAX_VALUE;

      MPI_Win_lock_all(0, hash_table_extra_info_win);
      for (i = 0; i < TABLE_SIZE; i++) {
        //computing the hash_table_extra_info_array is made parallel
        if ((i % num_processes) == my_rank) {
          hash_table_extra_info_t hash_table_extra_info;
          MPI_Get(&hash_table_extra_info, 1, EX_INFO_STRUCT_TYPE, MASTER, i, 1,
              EX_INFO_STRUCT_TYPE, hash_table_extra_info_win);
          MPI_Win_flush(MASTER, hash_table_extra_info_win);
          if (hash_table_extra_info.is_min_key_valid == true) {
            continue;
          } else {
            MPI_Win_lock_all(0, hashmap_win);
            traverse_and_update_hash_table_extra_info(i);
            MPI_Win_unlock_all(hashmap_win);
          }
        }
      }

      MPI_Win_unlock_all(hash_table_extra_info_win);
      MPI_Barrier(MPI_COMM_WORLD);

      MPI_Win_lock(MPI_LOCK_EXCLUSIVE, MASTER, 0, hash_table_extra_info_win);
      if (my_rank == MASTER) {
        MPI_Win_sync(hash_table_extra_info_win);
        for (i = 0; i <TABLE_SIZE; i++) {
          if (hash_table_extra_info_array[i].min_key_key < min_key) {
            min_key = hash_table_extra_info_array[i].min_key_key;
          }
        }
      }
      MPI_Win_unlock(MASTER, hash_table_extra_info_win);
      // even if an insertion happens concurrently at this point, as it in sequential
      // region, the insertion must be a statement after getMinKey_seq(). 
      // In that case even if there is concurrency at this point, the right value of min_key
      // will be returned.
      MPI_Bcast(&min_key, 1, MPI_INT, MASTER, MPI_COMM_WORLD);

      return min_key;
    }

    node_t getMinValue_par() {
      if (size() == 0) {
        return -1;
      }
      int i;
      node_t min_value = MAX_VALUE;

      MPI_Win_lock_all(0, hash_table_extra_info_win);
      for (i = 0; i < TABLE_SIZE; i++) {
        hash_table_extra_info_t hash_table_extra_info;
        MPI_Get(&hash_table_extra_info, 1, EX_INFO_STRUCT_TYPE, MASTER, i, 1,
            EX_INFO_STRUCT_TYPE, hash_table_extra_info_win);
        MPI_Win_flush(MASTER, hash_table_extra_info_win);
        if (hash_table_extra_info.is_min_value_valid == true) {
          continue;
        } else {
          MPI_Win_lock_all(0, hashmap_win);
          traverse_and_update_hash_table_extra_info(i);
          MPI_Win_unlock_all(hashmap_win);
        }
      }

      MPI_Win_unlock_all(hash_table_extra_info_win);
      MPI_Barrier(MPI_COMM_WORLD);

      MPI_Win_lock(MPI_LOCK_EXCLUSIVE, MASTER, 0, hash_table_extra_info_win);

      // FIXME: Here we are copying the entire hash_table_extra_info_array. 
      // It may be efficient to perform individual MPI_Gets to find the max_value.
      hash_table_extra_info_t hash_table_extra_info_array_copy[TABLE_SIZE];
      MPI_Get(hash_table_extra_info_array_copy, TABLE_SIZE, EX_INFO_STRUCT_TYPE, MASTER, 0, TABLE_SIZE,
              EX_INFO_STRUCT_TYPE, hash_table_extra_info_win);
      MPI_Win_flush(MASTER, hash_table_extra_info_win);

      for (i = 0; i <TABLE_SIZE; i++) {
        if (hash_table_extra_info_array_copy[i].min_value_value < min_value) {
          min_value = hash_table_extra_info_array_copy[i].min_value_value;
        }
      }
      MPI_Win_unlock(MASTER, hash_table_extra_info_win);
      // even if an insertion happens concurrently at this point, as it in sequential
      // region, the insertion must be a statement after getMinValue_seq(). 
      // In that case even if there is concurrency at this point, the right value of min_value
      // will be returned.

      return min_value;
    }

    node_t getMinValue_seq() {
      if (size() == 0) {
        return -1;
      }
      int i;
      node_t min_value = MAX_VALUE;

      MPI_Win_lock_all(0, hash_table_extra_info_win);
      for (i = 0; i < TABLE_SIZE; i++) {
        //computing the hash_table_extra_info_array is made parallel
        if ((i % num_processes) == my_rank) {
          hash_table_extra_info_t hash_table_extra_info;
          MPI_Get(&hash_table_extra_info, 1, EX_INFO_STRUCT_TYPE, MASTER, i, 1,
              EX_INFO_STRUCT_TYPE, hash_table_extra_info_win);
          MPI_Win_flush(MASTER, hash_table_extra_info_win);
          if (hash_table_extra_info.is_min_value_valid == true) {
            continue;
          } else {
            MPI_Win_lock_all(0, hashmap_win);
            traverse_and_update_hash_table_extra_info(i);
            MPI_Win_unlock_all(hashmap_win);
          }
        }
      }

      MPI_Win_unlock_all(hash_table_extra_info_win);
      MPI_Barrier(MPI_COMM_WORLD);

      MPI_Win_lock(MPI_LOCK_EXCLUSIVE, MASTER, 0, hash_table_extra_info_win);
      if (my_rank == MASTER) {
        MPI_Win_sync(hash_table_extra_info_win);
        for (i = 0; i <TABLE_SIZE; i++) {
          if (hash_table_extra_info_array[i].min_value_value < min_value) {
            min_value = hash_table_extra_info_array[i].min_value_value;
          }
        }
      }
      MPI_Win_unlock(MASTER, hash_table_extra_info_win);
      // even if an insertion happens concurrently at this point, as it in sequential
      // region, the insertion must be a statement after getMinValue_seq(). 
      // In that case even if there is concurrency at this point, the right value of min_value
      // will be returned.
      MPI_Bcast(&min_value, 1, MPI_INT, MASTER, MPI_COMM_WORLD);

      return min_value;
    }

    node_t getMaxValue_par() {
      if (size() == 0) {
        return -1;
      }
      int i;
      node_t max_value = MIN_VALUE;

      MPI_Win_lock_all(0, hash_table_extra_info_win);
      for (i = 0; i < TABLE_SIZE; i++) {
        hash_table_extra_info_t hash_table_extra_info;
        MPI_Get(&hash_table_extra_info, 1, EX_INFO_STRUCT_TYPE, MASTER, i, 1,
            EX_INFO_STRUCT_TYPE, hash_table_extra_info_win);
        MPI_Win_flush(MASTER, hash_table_extra_info_win);
        if (hash_table_extra_info.is_max_value_valid == true) {
          continue;
        } else {
          MPI_Win_lock_all(0, hashmap_win);
          traverse_and_update_hash_table_extra_info(i);
          MPI_Win_unlock_all(hashmap_win);
        }
      }

      MPI_Win_unlock_all(hash_table_extra_info_win);
      MPI_Barrier(MPI_COMM_WORLD);

      MPI_Win_lock(MPI_LOCK_EXCLUSIVE, MASTER, 0, hash_table_extra_info_win);

      // FIXME: Here we are copying the entire hash_table_extra_info_array. 
      // It may be efficient to perform individual MPI_Gets to find the max_value.
      hash_table_extra_info_t hash_table_extra_info_array_copy[TABLE_SIZE];
      MPI_Get(hash_table_extra_info_array_copy, TABLE_SIZE, EX_INFO_STRUCT_TYPE, MASTER, 0, TABLE_SIZE,
              EX_INFO_STRUCT_TYPE, hash_table_extra_info_win);
      MPI_Win_flush(MASTER, hash_table_extra_info_win);

      for (i = 0; i <TABLE_SIZE; i++) {
        if (hash_table_extra_info_array_copy[i].max_value_value > max_value) {
          max_value = hash_table_extra_info_array_copy[i].max_value_value;
        }
      }
      MPI_Win_unlock(MASTER, hash_table_extra_info_win);
      // even if an insertion happens concurrently at this point, as it in sequential
      // region, the insertion must be a statement after getMaxValue_seq(). 
      // In that case even if there is concurrency at this point, the right value of max_value
      // will be returned.

      return max_value;
    }

    node_t getMaxValue_seq() {
      if (size() == 0) {
        return -1;
      }
      int i;
      node_t max_value = MIN_VALUE;

      MPI_Win_lock_all(0, hash_table_extra_info_win);
      for (i = 0; i < TABLE_SIZE; i++) {
        //computing the hash_table_extra_info_array is made parallel
        if ((i % num_processes) == my_rank) {
          hash_table_extra_info_t hash_table_extra_info;
          MPI_Get(&hash_table_extra_info, 1, EX_INFO_STRUCT_TYPE, MASTER, i, 1,
              EX_INFO_STRUCT_TYPE, hash_table_extra_info_win);
          MPI_Win_flush(MASTER, hash_table_extra_info_win);
          if (hash_table_extra_info.is_max_value_valid == true) {
            continue;
          } else {
            MPI_Win_lock_all(0, hashmap_win);
            traverse_and_update_hash_table_extra_info(i);
            MPI_Win_unlock_all(hashmap_win);
          }
        }
      }

      MPI_Win_unlock_all(hash_table_extra_info_win);
      MPI_Barrier(MPI_COMM_WORLD);

      MPI_Win_lock(MPI_LOCK_EXCLUSIVE, MASTER, 0, hash_table_extra_info_win);
      if (my_rank == MASTER) {
        MPI_Win_sync(hash_table_extra_info_win);
        for (i = 0; i <TABLE_SIZE; i++) {
          if (hash_table_extra_info_array[i].max_value_value > max_value) {
            max_value = hash_table_extra_info_array[i].max_value_value;
          }
        }
      }
      MPI_Win_unlock(MASTER, hash_table_extra_info_win);
      // even if an insertion happens concurrently at this point, as it in sequential
      // region, the insertion must be a statement after getMaxValue_seq(). 
      // In that case even if there is concurrency at this point, the right value of max_value
      // will be returned.
      MPI_Bcast(&max_value, 1, MPI_INT, MASTER, MPI_COMM_WORLD);

      return max_value;
    }

    void removeMinKey_seq() {
      node_t key = getMinKey_seq();
      if(key != -1) {
        removeKey_seq(key);
      }
    }

    int getValue_par(node_t key) {
      int value=0;
      hash_node_ptr_t ptr;
      MPI_Win_lock(MPI_LOCK_EXCLUSIVE, MASTER, 0, hashmap_win);

      int hashValue = hashFunc(key);

      ptr = head_ptrs[hashValue];
      // Passing ptr as reference. So it will be updated.
      bool found_key = find_hash_node_ptr(ptr, key);
      if (found_key) {
        value = get_value_at_ptr( ptr);
      }
      MPI_Win_unlock(MASTER, hashmap_win);

     return value; 
    }
    int getValue_seq(node_t key) {
      int value;
      if(my_rank == MASTER) {
        value = getValue_par(key);
      }
      MPI_Bcast(&value, 1, MPI_INT, MASTER, MPI_COMM_WORLD);
      return value;
    }

    int changeValueAtomicAdd(node_t key, int summand) {
      int newValue = summand;
      int old_val;
			hash_node_ptr_t ptr;
      
      MPI_Win_lock(MPI_LOCK_EXCLUSIVE, MASTER, 0, hashmap_win);

      int hashValue = hashFunc(key);

      ptr = head_ptrs[hashValue];
      // Passing ptr as reference. So it will be updated.
      bool found_key = find_hash_node_ptr(ptr, key);
      if (found_key) {
        old_val = get_and_insert_value_at_ptr(summand, ptr);
        newValue += old_val;
      }
      else{
        hash_node_ptr_t new_elem_ptr;
        new_elem_ptr.disp = alloc_element(key, newValue, hashmap_win);
        new_elem_ptr.rank = my_rank;
        try_insert_after_ptr(new_elem_ptr, ptr);
        increment_size();
      }

      // FIXME: Check if this is function should be called
      // here.
      update_hash_table_extra_info(key, summand);

      MPI_Win_unlock(MASTER, hashmap_win);
      return newValue;
    }

    size_t size() {

      int map_size;

      MPI_Win_lock(MPI_LOCK_EXCLUSIVE, MASTER, 0, hash_table_size_win);
      MPI_Get(&map_size, 1, MPI_INT, MASTER, 0, 1, MPI_INT,hash_table_size_win);
      MPI_Win_unlock(MASTER, hash_table_size_win);

      return map_size;
    }

    size_t size_par() {
      return size();
    }

    size_t size_seq() {
      int map_size;
      if(my_rank == MASTER) {
        map_size = size_par();
      }
      MPI_Bcast(&map_size, 1, MPI_INT, 0, MPI_COMM_WORLD );
      return map_size;
    }

    void clear() {
      //TODO: Needs to be implemented
    }
};


template<class Key, class Value>
class gm_map_large : public gm_map<Key, Value>
{
  private:
    const size_t size_;
    const Value defaultValue;
    Value* const data;
    bool * const valid;

    template<class Function>
    Value getValue_generic_seq(Function func, const Value initialValue) {

        Value value = initialValue;
        #pragma omp parallel
        {
            Value value_private = value;

            #pragma omp for nowait
            for (Key i = 0; i < size_; i++) {
                if (valid[i]) value_private = func(value_private, data[i]);
            }
            // reduction
            {
                Value value_old;
                Value value_new;
                do {
                    value_old = value;
                    value_new = func(value_old, value_private);
                    if (value_old == value_new) break;
                } while (_gm_atomic_compare_and_swap(&(value), value_old, value_new) == false);
            }
        }
        return value;
    }

    template<class Function>
    Value getValue_generic_par(Function func, const Value initialValue) {

        Value value = initialValue;
        for (Key i = 0; i < size_; i++) {
            if (valid[i]) value = func(value, data[i]);
        }
        return value;
    }

    template<class Function>
    Key getKey_generic_par(Function compare, const Value initialValue) {

        Value value = initialValue;
        Key key = 0;

        for (Key i = 0; i < size_; i++) {
            if (valid[i] && compare(data[i], value)) {
                value = data[i];
                key = i;
            }
        }
        return key;
    }

    template<class Function>
    Key getKey_generic_seq(Function compare, const Value initialValue) {

        Value value = initialValue;
        Key key = 0;

        #pragma omp parallel
        {
            Value value_private = value;
            Key key_private = key;

            #pragma omp for nowait
            for (Key i = 0; i < size_; i++) {
                if (valid[i] && compare(data[i], value_private)) {
                    value_private = data[i];
                    key_private = i;
                }
            }
            // reduction
            if(compare(value_private, value)) {
                #pragma omp critical
                {
                    if(compare(value_private, value)) {
                        value = value_private;
                        key = key_private;
                    }
                }
            }
        }
        return key;
    }

    template<class Function>
    bool hasValue_generic_seq(Function compare, const Key key) {
        if (size() == 0 || !hasKey_seq(key)) return false;
        Value value = data[key];
        bool result = true;
        #pragma omp parallel for
        for (int i = 0; i < size_; i++)
            if (valid[i] && compare(data[i], value)) result = false;
        return result;
    }

    template<class Function>
    bool hasValue_generic_par(Function compare, const Key key) {
        if (size() == 0 || !hasKey_seq(key)) return false;
        Value value = data[key];
        for (int i = 0; i < size_; i++)
            if (valid[i] && compare(data[i], value)) return false;
    }

  protected:
    Value getDefaultValue() {
        return defaultValue;
    }

  public:
    gm_map_large(size_t size, Value defaultValue) :
            size_(size), data(new Value[size]), valid(new bool[size]), defaultValue(defaultValue) {
        #pragma omp parallel for
        for (int i = 0; i < size; i++) {
            valid[i] = false;
        }
    }

    ~gm_map_large() {
        delete[] data;
        delete[] valid;
    }

    bool hasKey_seq(const Key key) {
        return key < size_ && valid[key];
    }

    Value getValue(const Key key) {
        if (hasKey_seq(key))
            return data[key];
        else
            return defaultValue;
    }

    void setValue_par(const Key key, Value value) {
        setValue_seq(key, value);
    }

    void setValue_seq(const Key key, Value value) {
        data[key] = value;
        valid[key] = true;
    }

    void removeKey_par(const Key key) {
        removeKey_seq(key);
    }

    void removeKey_seq(const Key key) {
        valid[key] = false;
    }

    bool hasMaxValue_seq(const Key key) {
        return hasValue_generic_seq(&gm_map<Key, Value>::compare_greater, key);
    }

    bool hasMaxValue_par(const Key key) {
        return hasValue_generic_par(&gm_map<Key, Value>::compare_greater, key);
    }

    bool hasMinValue_seq(const Key key) {
        return hasValue_generic_seq(&gm_map<Key, Value>::compare_smaller, key);
    }

    bool hasMinValue_par(const Key key) {
        return hasValue_generic_par(&gm_map<Key, Value>::compare_smaller, key);
    }

    Key getMaxKey_seq() {
        return getKey_generic_seq(&gm_map<Key, Value>::compare_greater, gm_get_min<Value>());
    }

    Key getMaxKey_par() {
        return getKey_generic_par(&gm_map<Key, Value>::compare_greater, gm_get_min<Value>());
    }

    Key getMinKey_seq() {
        return getKey_generic_seq(&gm_map<Key, Value>::compare_smaller, gm_get_max<Value>());
    }

    Key getMinKey_par() {
        return getKey_generic_par(&gm_map<Key, Value>::compare_smaller, gm_get_max<Value>());
    }

    Value getMaxValue_seq() {
        return getValue_generic_seq(&gm_map<Key, Value>::max, gm_get_min<Value>());
    }

    Value getMaxValue_par() {
        return getValue_generic_par(&gm_map<Key, Value>::max, gm_get_min<Value>());
    }

    Value getMinValue_seq() {
        return getValue_generic_seq(&gm_map<Key, Value>::min, gm_get_max<Value>());
    }

    Value getMinValue_par() {
        return getValue_generic_par(&gm_map<Key, Value>::min, gm_get_max<Value>());
    }

    Value changeValueAtomicAdd(const Key key, const Value summand) {

        Value oldValue;
        Value newValue;

        do {
            oldValue = data[key];
            newValue = valid[key] ? (oldValue + summand) : summand;
        } while (_gm_atomic_compare_and_swap(data + key, oldValue, newValue) == false);
        valid[key] = true;
        return newValue;
    }

    size_t size() {
        size_t result = 0;
        for(int i = 0; i < size_; i++)
            result += valid[i];
        return result;
    }

    void clear() {
        #pragma omp parallel for
        for (Key key = 0; key < size(); key++) {
            valid[key] = false;
        }
    }

};


// Map is implemnted with set of inner-maps

template<class Key, class Value>
class gm_map_medium : public gm_map<Key, Value>
{
  private:
    const int innerSize;
    const Value defaultValue;
    map<Key, Value>* innerMaps;
    gm_spinlock_t* locks;
    typedef typename map<Key, Value>::iterator Iterator;
    const uint32_t bitmask;

    inline uint32_t getPositionFromKey(const Key key)
    {
        uint32_t P = 0;

        if (sizeof(Key) == 1) { 
            const uint8_t* c = (const uint8_t*) &key;
            P = *c;
        } else if (sizeof(Key) == 2) {
            const uint16_t* c = (const uint16_t*) &key;
            P = *c;
        } else if (sizeof(Key) >= 4) {
            const uint32_t* c = (const uint32_t*) &key;
            P = *c;
        } 
        return P & bitmask;
    }

    template<class FunctionCompare, class FunctionMinMax>
    Value getValue_generic_par(FunctionCompare compare, FunctionMinMax func, const Value initialValue) {

        Value value = initialValue;
        for (int i = 0; i < innerSize; i++) {
                if (innerMaps[i].size() > 0) {
                    for (Iterator iter = innerMaps[i].begin(); iter != innerMaps[i].end(); iter++) {
                        if (compare(iter->second, value)) {
                            value = iter->second;
                        }
                    }
                }
        }
        return value;
    }

    template<class FunctionCompare, class FunctionMinMax>
    Value getValue_generic_seq(FunctionCompare compare, FunctionMinMax func, const Value initialValue) {

        Value value = initialValue;
        #pragma omp parallel
        {
            Value value_private = initialValue;

            #pragma omp for nowait
            for (int i = 0; i < innerSize; i++) {
                if (innerMaps[i].size() > 0) value_private = func(value_private, getValueAtPosition_generic(i, compare));
            }
            // reduction
            {
                Value value_old;
                Value value_new;
                do {
                    value_old = value;
                    value_new = func(value_old, value_private);
                    if (value_old == value_new) break;
                } while (_gm_atomic_compare_and_swap(&(value), value_old, value_new) == false);
            }
        }
        return value;
    }

    template<class Function>
    Value getValueAtPosition_generic(int position, Function compare) {
        Iterator iter = innerMaps[position].begin();
        Value value = iter->second;
        for (iter++; iter != innerMaps[position].end(); iter++) {
            if (compare(iter->second, value)) {
                value = iter->second;
            }
        }
        return value;
    }

    template<class Function>
    Key getKey_generic_seq(Function compare, const Value initialValue) {
        Key key = 0;
        Value value = initialValue;

        #pragma omp parallel for
        for(int i = 0; i < innerSize; i++) {
            if(innerMaps[i].size() > 0) {
                Iterator iter = getKeyAtPosition_generic(i, compare);
                Key privateKey = iter->first;
                Value privateValue = iter->second;
                if(compare(privateValue, value)) {
                    #pragma omp critical
                    if(compare(privateValue, value)) {
                        value = privateValue;
                        key = privateKey;
                    }
                }
            }
        }
        return key;
    }

    template<class Function>
    Key getKey_generic_par(Function compare, const Value initialValue) {
        Key key = 0;
        Value value = initialValue;

        for(int i = 0; i < innerSize; i++) {
            if(innerMaps[i].size() > 0) {
                for (Iterator iter = innerMaps[i].begin(); iter != innerMaps[i].end(); iter++) {
                    if(compare(iter->second, value)) {
                        value = iter->second;
                        key = iter->first;
                    }
                }
            }
        }
        return key;
    }

    template<class Function>
    Iterator getKeyAtPosition_generic(int position, Function compare) {
        Iterator iter = innerMaps[position].begin();
        Iterator currentBest = iter;
        for (iter++; iter != innerMaps[position].end(); iter++) {
            if (compare(iter->second, currentBest->second)) {
                currentBest = iter;
            }
        }
        return currentBest;
    }

    template<class Function>
    bool hasValue_generic_par(Function compare, const Key key) {
        uint32_t position = getPositionFromKey(key);
        Value reference = getValueFromPosition(position, key);

        for(int i = 0; i < innerSize; i++) {
            if (innerMaps[i].size() > 0) {
                for (Iterator iter = innerMaps[i].begin(); iter != innerMaps[i].end(); iter++) {
                    if (compare(iter->second, reference)) return false;
                }
            }
        }
        return true;
    }

    template<class Function>
    bool hasValue_generic_seq(Function compare, const Key key) {
        bool result = true;
        uint32_t position = getPositionFromKey(key);
        Value reference = getValueFromPosition(position, key);
        #pragma omp parallel for
        for(int i = 0; i < innerSize; i++) {
            bool tmp = hasValueAtPosition_generic(i, compare, reference);
            if(!tmp) result = false;
        }
        return result;
    }

    template<class Function>
    bool hasValueAtPosition_generic(int position, Function compare, const Value reference) {
        map<Key, Value>& currentMap = innerMaps[position];
        if (currentMap.size() == 0) return false;
        for (Iterator iter = currentMap.begin(); iter != currentMap.end(); iter++) {
            if (compare(iter->second, reference)) return false;
        }
    }

    bool positionHasKey(int position, const Key key) {
        return innerMaps[position].find(key) != innerMaps[position].end();
    }

    Value getValueFromPosition(int position, const Key key) {
        Iterator iter = innerMaps[position].find(key);
        if(iter != innerMaps[position].end())
            return iter->second;
        else
            return defaultValue;
    }

    void setValueAtPosition(int position, const Key key, Value value) {
        innerMaps[position][key] = value;
    }

    void removeKeyAtPosition(int position, const Key key) {
        innerMaps[position].erase(key);
    }

    static unsigned getBitMask(int innerSize) {
        unsigned tmpMask = innerSize - 1;
        return tmpMask;
    }

    static int getSize(int threadCount) {
        int tmpSize = 32;
        while(tmpSize < threadCount) {
            tmpSize *= 2;
        }
        // we will use only up to 4B for positioninig
        assert(tmpSize <= 1024*1024*1024);
        return tmpSize;
    }

  protected:
    Value getDefaultValue() {
        return defaultValue;
    }


  public:
    gm_map_medium(int threadCount, Value defaultValue) : innerSize(getSize(threadCount)), bitmask(getBitMask(innerSize)), defaultValue(defaultValue) {
        locks = new gm_spinlock_t[innerSize];
        innerMaps = new map<Key, Value>[innerSize];
        #pragma omp parallel for
        for(int i = 0; i < innerSize; i++) {
            locks[i] = 0;
        }
    }

    ~gm_map_medium() {
        delete[] innerMaps;
        delete[] locks;
    }

    bool hasKey_seq(const Key key) {
        uint32_t position = getPositionFromKey(key);
        return positionHasKey(position, key);
    }

    Value getValue(const Key key) {
        uint32_t position = getPositionFromKey(key);
        return getValueFromPosition(position, key);
    }

    void setValue_par(const Key key, Value value) {
        uint32_t position = getPositionFromKey(key);
        gm_spinlock_acquire(locks + position);
        setValueAtPosition(position, key, value);
        gm_spinlock_release(locks + position);
    }

    void setValue_seq(const Key key, Value value) {
        uint32_t position = getPositionFromKey(key);
        setValueAtPosition(position, key, value);
    }

    void removeKey_par(const Key key) {
        uint32_t position = getPositionFromKey(key);
        gm_spinlock_acquire(locks + position);
        removeKeyAtPosition(position, key);
        gm_spinlock_release(locks + position);
    }

    void removeKey_seq(const Key key) {
        uint32_t position = getPositionFromKey(key);
        removeKeyAtPosition(position, key);
    }

    bool hasMaxValue_seq(const Key key) {
        return hasValue_generic_par(&gm_map<Key, Value>::compare_greater, key);
    }

    bool hasMinValue_seq(const Key key) {
        return hasValue_generic_par(&gm_map<Key, Value>::compare_smaller, key);
    }

    Key getMaxKey_seq() {
        return getKey_generic_seq(&gm_map<Key, Value>::compare_greater, gm_get_min<Value>());
    }

    Key getMaxKey_par() {
        return getKey_generic_par(&gm_map<Key, Value>::compare_greater, gm_get_min<Value>());
    }

    Key getMinKey_seq() {
        return getKey_generic_seq(&gm_map<Key, Value>::compare_smaller, gm_get_max<Value>());
    }

    Key getMinKey_par() {
        return getKey_generic_par(&gm_map<Key, Value>::compare_smaller, gm_get_max<Value>());
    }

    Value getMaxValue_seq() {
        return getValue_generic_seq(&gm_map<Key, Value>::compare_greater, &gm_map<Key, Value>::max, gm_get_min<Value>());
    }

    Value getMaxValue_par() {
        return getValue_generic_par(&gm_map<Key, Value>::compare_greater, &gm_map<Key, Value>::max, gm_get_min<Value>());
    }

    Value getMinValue_seq() {
        return getValue_generic_seq(&gm_map<Key, Value>::compare_smaller, &gm_map<Key, Value>::min, gm_get_max<Value>());
    }

    Value getMinValue_par() {
        return getValue_generic_par(&gm_map<Key, Value>::compare_smaller, &gm_map<Key, Value>::min, gm_get_max<Value>());
    }

    Value changeValueAtomicAdd(const Key key, const Value summand) {
        uint32_t position = getPositionFromKey(key);
        gm_spinlock_acquire(locks + position);
        Value newValue = summand;
        if(positionHasKey(position, key)) newValue += getValueFromPosition(position, key);
        setValueAtPosition(position, key, newValue);
        gm_spinlock_release(locks + position);
        return newValue;
    }

    size_t size() {
        size_t size = 0;
        #pragma omp parallel for reduction(+ : size)
        for(int i = 0; i < innerSize; i++) {
            size += innerMaps[i].size();
        }
        return size;
    }

    void clear() {
        #pragma omp parallel for
        for(int i = 0; i < innerSize; i++) {
            innerMaps[i].clear();
        }
    }

};

#endif /* GM_MAP_H_ */
