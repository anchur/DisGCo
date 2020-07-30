#define TABLE_SIZE 1024
/* Linked list pointer */
typedef struct {
	int      rank;
	MPI_Aint disp;
} hash_node_ptr_t ;
const hash_node_ptr_t nil = { -1, (MPI_Aint) MPI_BOTTOM };

/* A Hash node element */
typedef struct {
  hash_node_ptr_t next;
	
  node_t key;
  int value;

  bool remove_status;
} hash_node_t;

/* List of locally allocated list elements. */
static hash_node_t **my_elems = NULL;
static int my_elems_size  = 0;
static int my_elems_count = 0;
// Default hash function class
typedef struct {
    int operator()(const node_t & key) const
    {
        return reinterpret_cast<int>(key) % TABLE_SIZE;
    }
} KeyHash;

typedef struct {
  node_t min_key_key;
  int min_key_value;

  node_t min_value_key;
  int min_value_value;

  node_t max_key_key;
  int max_key_value;

  node_t max_value_key;
  int max_value_value;

  bool is_min_key_valid;
  bool is_max_key_valid;
  bool is_min_value_valid;
  bool is_max_value_valid;
} hash_table_extra_info_t;



