#ifndef GM_BACKEND_MPI
#define GM_BACKEND_MPI

#ifdef __USE_VAR_REUSE_OPT__
#include <stack>
#include <string>
#include <bits/stdc++.h>
#endif

#include "gm_backend_cpp.h"
using namespace std;
//mpi
//---------------------------------------------------------------------------
//
//---------------------------------------------------------------------------

class gm_mpi_get_info
{
public:
	ast_field * f;
	//const char * tempvar_name;
	const char * tempvarcount_str;
	//const char * index_name;
	//const char * tempvar_type;

	gm_mpi_get_info(){}
	gm_mpi_get_info(ast_field * f1,  const char *tmp_no_str)
	{
		f = f1;
		tempvarcount_str = tmp_no_str;
	}

};

//mpi

class gm_mpi_gen: public gm_cpp_gen {

public:


  bool inside_red;
  bool red_lock_emitted;
  ast_node* L;
  ast_expr* R;
#ifdef __USE_BSP_OPT__
  bool bsp_canonical;
  bool bsp_nested_1;
  bool bsp_nested_2;
  bool in_bsp_prepass;
  vector<ast_field *> updated_props;
  //to emit opt_body.
  int beg_fp;
  int beg_cur_int;
  int st_blk_fp;
  int st_blk_cur_int;
  int prop_temp_no;
  GM_REDUCE_T r_type;
  bool gen_post_opt_body;
  vector<char *> prop_names;
  vector<char *> prop_types;
#endif

#ifdef __USE_BARRIER_OPT
  bool need_barrier_before_put;
  bool need_barrier_after_put;
  vector<const char *> * read_props_before_barrier;
  vector<const char *> * written_props_before_barrier;
  bool s_w;
  bool barrier_pt_set;
  int curr_ind_BO;
  int f_ptr_BO;
  bool found_atleat_a_prop;
#endif

#ifdef __USE_VAR_REUSE_OPT__
  /* Data structures to deal with the rank and
   * local number optimization.
   */
  int nesting_level;
  // the nesting level is mapped to a list of fields, whose initial access 
  // appears in this nesting level.
  map <int, vector<const char *> * > vars_in_scope;
  // index name is mapped to rankvar_countstr;
  map <const char *, string> field_to_reuse_vars_map;
#endif

	vector <gm_mpi_get_info *> mpi_get_details;
	vector <const char *> local_access_nodes;
	vector <const char *> local_access_edges;
	int tmpvar_count;
	int rankvar_count;
	char rankvar_countstr[32];
	char tempvar_countstr[32];
	ast_procdef *proc;
	int in_bfs_body;
 
  // Variable to remember if the control enters a parallel loop.
  bool in_parallel_loop;
  bool assign_lhs_scalar;
  bool lhs_local_node;
  bool lhs_local_edge;

	gm_mpi_gen() {
		tmpvar_count = 0;
		rankvar_count = 0;
		in_bfs_body = false;
    assign_lhs_scalar = false;
    lhs_local_edge = false;
    lhs_local_node = false;
#ifdef __USE_BSP_OPT__
    bsp_canonical = false;
    bsp_nested_1 = false;
    bsp_nested_2 = false;
    inside_red = false;
    red_lock_emitted = false;
    prop_temp_no = 0;
    r_type = GMREDUCE_MIN;
    gen_post_opt_body = false;
#endif
#ifdef __USE_VAR_REUSE_OPT__
    nesting_level = 0;
#endif
#ifdef __USE_BARRIER_OPT
    //start of while is marked in this variable
    s_w = false;
    barrier_pt_set = false;
    need_barrier_before_put = false;
    need_barrier_after_put = true;
    read_props_before_barrier = new vector<const char *> ();
    written_props_before_barrier = new vector<const char *> ();
#endif
	}


#ifdef __USE_BSP_OPT__
  bool is_bsp_canonical() {
    return bsp_canonical;
  }

  void set_bsp_canonical() {
    bsp_canonical = true;
  }

  void reset_bsp_canonical() {
    bsp_canonical = false;
  }

  bool is_bsp_nested_1() {
    return bsp_nested_1;
  }

  void set_bsp_nested_1() {
    bsp_nested_1 = true;
  }


  void reset_bsp_nested_1() {
    bsp_nested_1 = false;
  }

  bool is_bsp_nested_2() {
    return bsp_nested_2;
  }

  void set_bsp_nested_2() {
    bsp_nested_2 = true;
  }


  void reset_bsp_nested_2() {
    bsp_nested_2 = false;
  }


  bool is_in_bsp_prepass() {
    return in_bsp_prepass;
  }

  void set_in_bsp_prepass() {
    in_bsp_prepass = true;
  }

  void reset_in_bsp_prepass() {
    in_bsp_prepass = false;
  }
#endif

  bool is_inside_red(){
    return inside_red;
  }

  void set_inside_red() {
    inside_red = true;
  }

  void reset_inside_red() {
    inside_red = false;
  }

  bool is_red_lock_emitted() {
    return red_lock_emitted;
  }

  void set_red_lock_emitted() {
    red_lock_emitted = true;
  }

  void reset_red_lock_emitted() {
    red_lock_emitted = false;
  }
  
  bool is_lhs_local_node(){
    return lhs_local_node;
  }

  void set_lhs_local_node() {
    lhs_local_node = true;
  }

  void reset_lhs_local_node(){
    lhs_local_node = false;
  }

  bool is_lhs_local_edge(){
    return lhs_local_edge;
  }

  void set_lhs_local_edge() {
    lhs_local_edge = true;
  }

  void reset_lhs_local_edge(){
    lhs_local_edge = false;
  }

  bool is_assign_lhs_scalar() {
    return assign_lhs_scalar;
  }

  void set_assign_lhs_scalar() {
    assign_lhs_scalar = true;
  }

  void reset_assign_lhs_scalar() {
    assign_lhs_scalar = false;
  }

  bool is_in_parallel_loop() {
    return in_parallel_loop;
  }

  void set_in_parallel_loop() {
    in_parallel_loop = true;
  }

  void reset_in_parallel_loop() {
    in_parallel_loop = false;
  }

	void set_in_bfs_body() {
		in_bfs_body = true;
    set_in_parallel_loop();
	}

	void reset_in_bfs_body() {
		in_bfs_body = false;
    reset_in_parallel_loop();
	}

	bool get_in_bfs_body() {
		return in_bfs_body;
	}

	bool is_present_in_local_access_nodes(const char *varname) {
		vector <const char *>::iterator I;
		for (I = local_access_nodes.begin(); I != local_access_nodes.end(); I++) {
			const char *vname = *I;
			if (!strcmp(vname, varname)) {
				return true;
			}
		}
		return false;
	}

	bool is_present_in_local_access_edges(const char *varname) {
		vector <const char *>::iterator I;
		for (I = local_access_edges.begin(); I != local_access_edges.end(); I++) {
			const char *vname = *I;
			if (!strcmp(vname, varname)) {
				return true;
			}
		}
		return false;
	}

	
#ifdef __USE_VAR_REUSE_OPT__
  void insert_index_to_vars_in_scope(const char *index_name) {
    map <int, vector<const char *> * >::iterator I = vars_in_scope.find(nesting_level);
    vector <const char *> *v = NULL;
    if (I == vars_in_scope.end()) {
      v = new vector <const char *> ();
      vars_in_scope[nesting_level] = v;
    } else {
      v = I->second;
    }
    v->push_back(index_name);
    vector<const char *>::iterator v_I = v->begin();
    printf("inserting %s to %d. The contents now are\n", index_name, nesting_level);
    for(; v_I!=v->end(); v_I++){
      printf("%s", *v_I);
    }
  }

  void insert_rank_countstr(const char *index_name, char *rankvar_countstr) {
    field_to_reuse_vars_map[index_name]=rankvar_countstr;
  }

  void inc_nesting_level() {
    nesting_level++;
    printf("nesting level incremented is %d\n", nesting_level);
  }

  void dec_nesting_level() {
    nesting_level--;
    printf("nesting level decremented is %d\n", nesting_level);
  }

  //char * get_rank_countstr(const char *index_name) {
  //  char temp[32];
  //  sprintf(temp, "%s", field_to_reuse_vars_map[index_name]);
  //  return temp;
  //}

  void clear_vars_in_curr_scope() {
    map <int, vector<const char *> * >::iterator I = vars_in_scope.find(nesting_level);
    if (I != vars_in_scope.end()) {
      vector <const char *> *v = vars_in_scope[nesting_level];
      vector <const char *> ::iterator I = v->begin();

      for(; I != v->end(); I++) {
        const char *index_name = *I;
        field_to_reuse_vars_map.erase(index_name); 
        // FIXME: v needs to be freed.
      }

      vars_in_scope.erase(nesting_level);
    }
  }

  bool check_in_vars_in_scope(const char *var_name) {
    map <int, vector<const char *> *>::iterator m_I = vars_in_scope.begin();
    for (; m_I != vars_in_scope.end(); m_I++) {
      vector <const char *> *v = m_I->second;
      vector <const char *> ::iterator I = v->begin();

      for(; I != v->end(); I++) {
        const char *index_name = *I;
        if (strcmp(index_name, var_name) == 0) {
          return true;
        }
      }
    }
    return false;
  }

  void remove_var(char *var_name) {
    field_to_reuse_vars_map.erase(var_name);

    map <int, vector<const char *> *>::iterator m_I = vars_in_scope.begin();
    for (; m_I != vars_in_scope.end(); m_I++) {
      vector <const char *> *v = m_I->second;
      vector <const char *> ::iterator I = v->begin();

      for(; I != v->end(); I++) {
        const char *index_name = *I;
        if (strcmp(index_name, var_name) == 0) {
          v->erase(I);
          // FIXME: Assuming there are only one entry with var_name as
          // index_name.
          return;
        }
      }
    }
  }

#endif

#ifdef __USE_BARRIER_OPT
  void set_need_barrier_before_put() {
    need_barrier_before_put = true;
  }

  void set_need_barrier_after_put() {
    need_barrier_after_put = true;
  }

  void reset_need_barrier_before_put() {
    need_barrier_before_put = false;
  }

  void reset_need_barrier_after_put() {
    need_barrier_after_put = false;
  }

  bool get_need_barrier_before_put() {
    return need_barrier_before_put;
  }

  bool get_need_barrier_after_put() {
    return need_barrier_after_put;
  }

  void clear_props_before_barrier(){
    read_props_before_barrier->clear();
    written_props_before_barrier->clear();
  }

  void copyprops(vector<const char *> * rprops, vector<const char *> * wprops) {
    vector <const char *>::iterator I;
    for (I = rprops->begin(); I !=rprops->end(); I++) {
      read_props_before_barrier->push_back(*I);
    }
    for (I = wprops->begin(); I !=wprops->end(); I++) {
      written_props_before_barrier->push_back(*I);
    }
    
  }

  void union(vector<const char *> * props1, vector<const char *> * props2) {
    vector <const char *>::iterator I;
    for (I = props2->begin(); I !=props2->end(); I++) {
      props1->push_back(*I);
    }
    //delete props2;
  }

  bool check_and_set_need_barrier_before_put(const char * prop) {
		vector <const char *>::iterator I;
    bool prop_found = false;
		for (I = read_props_before_barrier->begin(); I != read_props_before_barrier->end(); I++) {
			const char *pname = *I;
			if (!strcmp(pname, prop)) {
				  prop_found = true;
          break;
			}
		}
    if(prop_found){
      set_need_barrier_before_put();
    }else{
      reset_need_barrier_before_put();
    }
		return need_barrier_before_put;
  }

  bool check_and_set_need_barrier_before_get(const char * prop) {
		vector <const char *>::iterator I;
    bool prop_found = false;
		for (I = written_props_before_barrier->begin(); I != written_props_before_barrier->end(); I++) {
			const char *pname = *I;
			if (!strcmp(pname, prop)) {
				  prop_found = true;
          break;
			}
		}
    if(prop_found){
      set_need_barrier_before_put();
    }else{
      reset_need_barrier_before_put();
    }
		return need_barrier_before_put;
  }

  void insert_into_read_props_before_barrier(const char * prop) {
    read_props_before_barrier->push_back(prop);
  }
#endif

	void generate_win_attach_in_param_props(gm_code_writer &Body)
	{
		std::list<ast_argdecl*>& lst = proc->get_in_args();
		std::list<ast_argdecl*>::iterator i;
		for (i = lst.begin(); i != lst.end(); i++) {


			ast_typedecl* t1 = (*i)->get_type();
			if(t1->is_property())
			{

				ast_typedecl* t2 = t1->get_target_type();
				char *param_name = (*i)->get_idlist()->get_item(0)->get_genname();


				Body.NL();
				Body.push("int ");
				Body.push(param_name);
				Body.push("_index = base_address_index;");
				Body.NL();



				switch (t2->getTypeSummary()) {
				case GMTYPE_INT:
					Body.push("G.win_attach_int");
					//Body.push(ALLOCATE_INT);
					break;
				case GMTYPE_LONG:
					Body.push("G.win_attach_long");
					break;
				case GMTYPE_BOOL:
					Body.push("G.win_attach_bool");
					//Body.push(ALLOCATE_BOOL);
					break;
				case GMTYPE_DOUBLE:
					Body.push("G.win_attach_double");
					break;
				case GMTYPE_FLOAT:
					Body.push("G.win_attach_float");
					break;
				case GMTYPE_NODE:
					Body.push("G.win_attach_int");
					break;
				case GMTYPE_EDGE:
					Body.push("G.win_attach_int");
					break;
				case GMTYPE_NSET:
				case GMTYPE_ESET:
				case GMTYPE_NSEQ:
				case GMTYPE_ESEQ:
				case GMTYPE_NORDER:
				case GMTYPE_EORDER: {
					//char temp[128];
					//bool lazyInitialization = false; //TODO: get some information here to check if lazy init is better
					//sprintf(temp, "%s<%s, %s>", ALLOCATE_COLLECTION, get_lib()->get_type_string(t2), (lazyInitialization ? "true" : "false"));
					//Body.push(temp);
					break;
				}
				default:
					assert(false);
					break;
				}

				if (t1->is_node_property()) {
					Body.push("(");
					Body.push(param_name);
					Body.push(", G.get_num_of_local_nodes());");
					//Body.push(get_lib()->max_node_index(t->get_target_graph_id()));
				} else if (t1->is_edge_property()) {
					Body.push("(");
					Body.push(param_name);
					Body.push(", G.get_num_of_local_forward_edges());");
					//Body.push(get_lib()->max_edge_index(t1->get_target_graph_id()));
				}
				Body.NL();




			}

		}
	}


	void generate_win_attach_out_param_props(gm_code_writer &Body)
	{
		std::list<ast_argdecl*>& lst = proc->get_out_args();
		std::list<ast_argdecl*>::iterator i;
		for (i = lst.begin(); i != lst.end(); i++) {


			ast_typedecl* t1 = (*i)->get_type();
			if(t1->is_property())
			{

				ast_typedecl* t2 = t1->get_target_type();
				char *param_name = (*i)->get_idlist()->get_item(0)->get_genname();


				Body.NL();
				Body.push("int ");
				Body.push(param_name);
				Body.push("_index = base_address_index;");
				Body.NL();



				switch (t2->getTypeSummary()) {
				case GMTYPE_INT:
					Body.push("G.win_attach_int");
					//Body.push(ALLOCATE_INT);
					break;
				case GMTYPE_LONG:
					Body.push("G.win_attach_long");
					break;
				case GMTYPE_BOOL:
					Body.push("G.win_attach_bool");
					//Body.push(ALLOCATE_BOOL);
					break;
				case GMTYPE_DOUBLE:
					Body.push("G.win_attach_double");
					break;
				case GMTYPE_FLOAT:
					Body.push("G.win_attach_float");
					break;
				case GMTYPE_NODE:
					Body.push("G.win_attach_int");
					break;
				case GMTYPE_EDGE:
					Body.push("G.win_attach_int");
					break;
				case GMTYPE_NSET:
				case GMTYPE_ESET:
				case GMTYPE_NSEQ:
				case GMTYPE_ESEQ:
				case GMTYPE_NORDER:
				case GMTYPE_EORDER: {
					char temp[128];
					bool lazyInitialization = false; //TODO: get some information here to check if lazy init is better
					sprintf(temp, "%s<%s, %s>", ALLOCATE_COLLECTION, get_lib()->get_type_string(t2), (lazyInitialization ? "true" : "false"));
					Body.push(temp);
					break;
				}
				default:
					assert(false);
					break;
				}

				if (t1->is_node_property()) {
					Body.push("(");
					Body.push(param_name);
					Body.push(", G.get_num_of_local_nodes());");
					//Body.push(get_lib()->max_node_index(t->get_target_graph_id()));
				} else if (t1->is_edge_property()) {
					Body.push("(");
					Body.push(param_name);
					Body.push(", G.get_num_of_local_forward_edges());");
					//Body.push(get_lib()->max_edge_index(t1->get_target_graph_id()));
				}
				Body.NL();




			}

		}
	}


	void generate_mpi_get_block(int fp1, int curr_indent1, gm_code_writer &Body){
    if(is_red_lock_emitted()) {
      generate_mpi_get(fp1, curr_indent1, Body, false, false);
    }else {
      generate_mpi_get(fp1, curr_indent1, Body, false, true);
    }
	}

	void generate_mpi_put_block_without_lock(ast_field *f, gm_code_writer &Body)
  {
    generate_mpi_put(f, Body, false);
  }


	void generate_mpi_get(int fp1, int curr_indent1, gm_code_writer &Body,
      bool is_flush_needed, bool is_lock_needed) {
    int cur_ind = 0;
    printf("inside generate_mpi_get_block before for loop fp is %d fp1 = %d\n", Body.get_write_ptr(cur_ind), fp1);
    char *tmp_write_buffer = new char[256];
    tmp_write_buffer[0]='\0';
    char rankvarcount_str[32];

    vector <gm_mpi_get_info *>::iterator Iter = mpi_get_details.begin();

    //iterate over all properties that are read in the statement
    for (; Iter != mpi_get_details.end(); Iter++) {

      gm_mpi_get_info *g_info = *Iter;
      ast_field *f = g_info->f;
      const char *tempvarcount_str = g_info->tempvarcount_str;
      ast_typedecl *tmp_type_decl = f->get_second()->getTargetTypeInfo();
      const char *tmp_type;// = get_type_string(tmp_type_decl);
      // Append _tmp to it when it is used in in Body.push.
      const char *tmp_name = f->get_second()->get_genname();



      switch (tmp_type_decl->getTypeSummary()) {
        case GMTYPE_INT:
          tmp_type = "int";
          //Body.push("G.win_attach_int");
          //Body.push(ALLOCATE_INT);
          break;
        case GMTYPE_LONG:
          tmp_type = "long";
          break;
        case GMTYPE_BOOL:
          tmp_type = "bool";
          //Body.push(ALLOCATE_BOOL);
          break;
        case GMTYPE_DOUBLE:
          tmp_type = "double";
          break;
        case GMTYPE_FLOAT:
          tmp_type = "float";
          break;
        case GMTYPE_NODE:
          tmp_type = "int";
          //Body.push(ALLOCATE_NODE);
          break;
        case GMTYPE_EDGE:
          tmp_type = "int";
          //Body.push(ALLOCATE_EDGE);
          break;
        case GMTYPE_NSET:
        case GMTYPE_ESET:
        case GMTYPE_NSEQ:
        case GMTYPE_ESEQ:
        case GMTYPE_NORDER:
        case GMTYPE_EORDER: {
                              //char temp[128];
                              //bool lazyInitialization = false; //TODO: get some information here to check if lazy init is better
                              //sprintf(temp, "%s<%s, %s>", ALLOCATE_COLLECTION, get_lib()->get_type_string(t2), (lazyInitialization ? "true" : "false"));
                              //Body.push(temp);
                              break;
                            }
        default:
                            assert(false);
                            break;
      }


      //declaring the temporary
      tmp_write_buffer[0]='\0';
      strcat(tmp_write_buffer, tmp_type);
      strcat(tmp_write_buffer, " ");
      strcat(tmp_write_buffer, tmp_name);
      strcat(tmp_write_buffer, "_tmp");
      strcat(tmp_write_buffer, tempvarcount_str);
      strcat(tmp_write_buffer, ";\n");
      printf("%s\n", tmp_write_buffer);
      int cur_ind = 0;
      printf("inside generate_mpi_get_block  fp is %d fp1 = %d\n", Body.get_write_ptr(cur_ind), fp1);
      Body.insert_at(fp1, curr_indent1, tmp_write_buffer);
      fp1 = fp1 + strlen(tmp_write_buffer) + curr_indent1*Body.get_tab_size();

      const char* ind_name;
      if (f->getTypeInfo()->is_node_property()) {
        ind_name = get_lib()->node_index(f->get_first());
        //Body.push(index_name);
      }
      else if (f->getTypeInfo()->is_edge_property()) {

        if (f->is_rarrow()) { // [XXX] this feature is still not finished, should be re-visited
          const char* alias_name = f->get_first()->getSymInfo()->find_info_string(CPPBE_INFO_NEIGHBOR_ITERATOR);
          assert(alias_name != NULL);
          assert(strlen(alias_name) > 0);
          ind_name = alias_name;
          //Body.push(alias_name);
        }
        // check if the edge is a back-edge
        else if (f->get_first()->find_info_bool(CPPBE_INFO_IS_REVERSE_EDGE)) {
          ind_name = get_lib()->fw_edge_index(f->get_first());
          //Body.push(index_name);
        }
        else {
          // original edge index if semi-sorted
          //sprintf(temp, "%s.%s(%s)",
          //f->get_first()->getTypeInfo()->get_target_graph_id()->get_genname(),
          //GET_ORG_IDX,
          //f->get_first()->get_genname()
          //);
          //Body.push(temp);
          ind_name = get_lib()->edge_index(f->get_first());
          //Body.push(index_name);
        }
      }
      else {
        assert(false);
      }
#ifdef __USE_VAR_REUSE_OPT__
      bool index_found_in_stack = false;
      if(is_in_parallel_loop()){
        index_found_in_stack = check_in_vars_in_scope(ind_name);
      }
      if (index_found_in_stack) {
        // TODO: Check if rankvar_countstr gets the correct value.
        strcpy(rankvarcount_str, field_to_reuse_vars_map[ind_name].c_str());
        //rankvarcount_str=field_to_reuse_vars_map[ind_name];
      } else {
        //declaring the rank variable
        rankvar_count++;
        if(is_in_parallel_loop()){
          insert_index_to_vars_in_scope(ind_name);
        }

        sprintf(rankvarcount_str, "%d", rankvar_count);

        insert_rank_countstr(ind_name, rankvarcount_str);



        tmp_write_buffer[0]='\0';
        strcat(tmp_write_buffer, "int r_");
        strcat(tmp_write_buffer, rankvarcount_str);
        strcat(tmp_write_buffer, ";\n");
        Body.insert_at(fp1, curr_indent1, tmp_write_buffer);
        fp1 = fp1 + strlen(tmp_write_buffer) + curr_indent1*Body.get_tab_size();


        //declaring ln variable;
        tmp_write_buffer[0]='\0';
        strcat(tmp_write_buffer, "int ln_");
        strcat(tmp_write_buffer, rankvarcount_str);
        strcat(tmp_write_buffer, ";\n");
        Body.insert_at(fp1, curr_indent1, tmp_write_buffer);
        fp1 = fp1 + strlen(tmp_write_buffer) + curr_indent1*Body.get_tab_size();
      }

      if (f->getTypeInfo()->is_node_property()) {
        if (!index_found_in_stack) {

          //getting the rank
          tmp_write_buffer[0]='\0';
          strcat(tmp_write_buffer, "r_");
          strcat(tmp_write_buffer, rankvarcount_str);
          strcat(tmp_write_buffer, "= G.get_rank_node(");
          strcat(tmp_write_buffer, ind_name);
          strcat(tmp_write_buffer, ");\n");
          Body.insert_at(fp1, curr_indent1, tmp_write_buffer);
          fp1 = fp1 + strlen(tmp_write_buffer) + curr_indent1*Body.get_tab_size();


          //getting the local no
          tmp_write_buffer[0]='\0';
          strcat(tmp_write_buffer, "ln_");
          strcat(tmp_write_buffer, rankvarcount_str);
          strcat(tmp_write_buffer, "= G.get_local_node_num(");
          strcat(tmp_write_buffer, ind_name);
          strcat(tmp_write_buffer, ");\n");
          Body.insert_at(fp1, curr_indent1, tmp_write_buffer);
          fp1 = fp1 + strlen(tmp_write_buffer) + curr_indent1*Body.get_tab_size();

        }
#ifdef __USE_BARRIER_OPT
    if (!is_in_parallel_loop()) {
      MPI_GEN.found_atleat_a_prop = true;
        if(check_and_set_need_barrier_before_get()){
          Body.pushln("MPI_Barrier(MPI_COMM_WORLD);");
          //if barrier is emitted clear all dependencies.
          clear_props_before_barrier();
        }
    }
#endif
        //printing the get
        tmp_write_buffer[0]='\0';
        if(is_lock_needed) { 
          strcat(tmp_write_buffer, "lock_and_get_node_property_");
        } else {
          strcat(tmp_write_buffer, "get_node_property_");
        }
        strcat(tmp_write_buffer, tmp_type);
        strcat(tmp_write_buffer, "(");
        strcat(tmp_write_buffer, tmp_name);
        strcat(tmp_write_buffer, "_tmp");
        strcat(tmp_write_buffer, tempvarcount_str);
        strcat(tmp_write_buffer, ",");
        strcat(tmp_write_buffer, tmp_name);
        strcat(tmp_write_buffer, "_index");
        strcat(tmp_write_buffer, ",");
        strcat(tmp_write_buffer, "r_");
        strcat(tmp_write_buffer, rankvarcount_str);
        strcat(tmp_write_buffer, ",");
        strcat(tmp_write_buffer, "ln_");
        strcat(tmp_write_buffer, rankvarcount_str);
        strcat(tmp_write_buffer, ");\n");
        Body.insert_at(fp1, curr_indent1, tmp_write_buffer);
        fp1 = fp1 + strlen(tmp_write_buffer) + curr_indent1*Body.get_tab_size();
      }
      else if (f->getTypeInfo()->is_edge_property()) {
        if (!index_found_in_stack) {
          //getting the rank
          tmp_write_buffer[0]='\0';
          strcat(tmp_write_buffer, "r_");
          strcat(tmp_write_buffer, rankvarcount_str);
          strcat(tmp_write_buffer, "= G.get_rank_edge(");
          strcat(tmp_write_buffer, ind_name);
          strcat(tmp_write_buffer, ");\n");
          Body.insert_at(fp1, curr_indent1, tmp_write_buffer);
          fp1 = fp1 + strlen(tmp_write_buffer) + curr_indent1*Body.get_tab_size();


          //getting the local no
          tmp_write_buffer[0]='\0';
          strcat(tmp_write_buffer, "ln_");
          strcat(tmp_write_buffer, rankvarcount_str);
          strcat(tmp_write_buffer, "= G.get_local_edge_num(");
          strcat(tmp_write_buffer, ind_name);
          strcat(tmp_write_buffer, ");\n");
          Body.insert_at(fp1, curr_indent1, tmp_write_buffer);
          fp1 = fp1 + strlen(tmp_write_buffer) + curr_indent1*Body.get_tab_size();

        }
        //printing the get
        tmp_write_buffer[0]='\0';

        if(is_lock_needed) { 
          strcat(tmp_write_buffer, "lock_and_get_edge_property_");
        } else {
          strcat(tmp_write_buffer, "get_edge_property_");
        }
        strcat(tmp_write_buffer, tmp_type);
        strcat(tmp_write_buffer, "(");
        strcat(tmp_write_buffer, tmp_name);
        strcat(tmp_write_buffer, "_tmp");
        strcat(tmp_write_buffer, tempvarcount_str);
        strcat(tmp_write_buffer, ",");
        strcat(tmp_write_buffer, tmp_name);
        strcat(tmp_write_buffer, "_index");
        strcat(tmp_write_buffer, ",");
        strcat(tmp_write_buffer, "r_");
        strcat(tmp_write_buffer, rankvarcount_str);
        strcat(tmp_write_buffer, ",");
        strcat(tmp_write_buffer, "ln_");
        strcat(tmp_write_buffer, rankvarcount_str);
        strcat(tmp_write_buffer, ");\n");
        Body.insert_at(fp1, curr_indent1, tmp_write_buffer);
        fp1 = fp1 + strlen(tmp_write_buffer) + curr_indent1*Body.get_tab_size();
      }

      if (is_flush_needed) {
        tmp_write_buffer[0]='\0';
        strcat(tmp_write_buffer, "MPI_Win_flush(r_");
        strcat(tmp_write_buffer, rankvarcount_str);
        strcat(tmp_write_buffer, ", win);\n");
        Body.insert_at(fp1, curr_indent1, tmp_write_buffer);
        fp1 = fp1 + strlen(tmp_write_buffer) + curr_indent1*Body.get_tab_size();
      }
#else
      //declaring the rank variable
      rankvar_count++;
      sprintf(rankvarcount_str, "%d", rankvar_count);

      tmp_write_buffer[0]='\0';
      strcat(tmp_write_buffer, "int r_");
      strcat(tmp_write_buffer, rankvarcount_str);
      strcat(tmp_write_buffer, ";\n");
      Body.insert_at(fp1, curr_indent1, tmp_write_buffer);
      fp1 = fp1 + strlen(tmp_write_buffer) + curr_indent1*Body.get_tab_size();


      //declaring ln variable;
      tmp_write_buffer[0]='\0';
      strcat(tmp_write_buffer, "int ln_");
      strcat(tmp_write_buffer, rankvarcount_str);
      strcat(tmp_write_buffer, ";\n");
      Body.insert_at(fp1, curr_indent1, tmp_write_buffer);
      fp1 = fp1 + strlen(tmp_write_buffer) + curr_indent1*Body.get_tab_size();

      if (f->getTypeInfo()->is_node_property()) {

        //getting the rank
        tmp_write_buffer[0]='\0';
        strcat(tmp_write_buffer, "r_");
        strcat(tmp_write_buffer, rankvarcount_str);
        strcat(tmp_write_buffer, "= G.get_rank_node(");
        strcat(tmp_write_buffer, ind_name);
        strcat(tmp_write_buffer, ");\n");
        Body.insert_at(fp1, curr_indent1, tmp_write_buffer);
        fp1 = fp1 + strlen(tmp_write_buffer) + curr_indent1*Body.get_tab_size();


        //getting the local no
        tmp_write_buffer[0]='\0';
        strcat(tmp_write_buffer, "ln_");
        strcat(tmp_write_buffer, rankvarcount_str);
        strcat(tmp_write_buffer, "= G.get_local_node_num(");
        strcat(tmp_write_buffer, ind_name);
        strcat(tmp_write_buffer, ");\n");
        Body.insert_at(fp1, curr_indent1, tmp_write_buffer);
        fp1 = fp1 + strlen(tmp_write_buffer) + curr_indent1*Body.get_tab_size();

        //printing the get
        tmp_write_buffer[0]='\0';
#ifdef __USE_BARRIER_OPT
    if (!is_in_parallel_loop()) {
      MPI_GEN.found_atleat_a_prop = true;
        if(check_and_set_need_barrier_before_get()){
          Body.pushln("MPI_Barrier(MPI_COMM_WORLD);");
          //if barrier is emitted clear all dependencies.
          clear_props_before_barrier();
        }
    }
#endif
        if(is_lock_needed) { 
          strcat(tmp_write_buffer, "lock_and_get_node_property_");
        } else {
          strcat(tmp_write_buffer, "get_node_property_");
        }
        strcat(tmp_write_buffer, tmp_type);
        strcat(tmp_write_buffer, "(");
        strcat(tmp_write_buffer, tmp_name);
        strcat(tmp_write_buffer, "_tmp");
        strcat(tmp_write_buffer, tempvarcount_str);
        strcat(tmp_write_buffer, ",");
        strcat(tmp_write_buffer, tmp_name);
        strcat(tmp_write_buffer, "_index");
        strcat(tmp_write_buffer, ",");
        strcat(tmp_write_buffer, "r_");
        strcat(tmp_write_buffer, rankvarcount_str);
        strcat(tmp_write_buffer, ",");
        strcat(tmp_write_buffer, "ln_");
        strcat(tmp_write_buffer, rankvarcount_str);
        strcat(tmp_write_buffer, ");\n");
        Body.insert_at(fp1, curr_indent1, tmp_write_buffer);
        fp1 = fp1 + strlen(tmp_write_buffer) + curr_indent1*Body.get_tab_size();
      }
      else if (f->getTypeInfo()->is_edge_property()) {

        //getting the rank
        tmp_write_buffer[0]='\0';
        strcat(tmp_write_buffer, "r_");
        strcat(tmp_write_buffer, rankvarcount_str);
        strcat(tmp_write_buffer, "= G.get_rank_edge(");
        strcat(tmp_write_buffer, ind_name);
        strcat(tmp_write_buffer, ");\n");
        Body.insert_at(fp1, curr_indent1, tmp_write_buffer);
        fp1 = fp1 + strlen(tmp_write_buffer) + curr_indent1*Body.get_tab_size();


        //getting the local no
        tmp_write_buffer[0]='\0';
        strcat(tmp_write_buffer, "ln_");
        strcat(tmp_write_buffer, rankvarcount_str);
        strcat(tmp_write_buffer, "= G.get_local_edge_num(");
        strcat(tmp_write_buffer, ind_name);
        strcat(tmp_write_buffer, ");\n");
        Body.insert_at(fp1, curr_indent1, tmp_write_buffer);
        fp1 = fp1 + strlen(tmp_write_buffer) + curr_indent1*Body.get_tab_size();

        //printing the get
        tmp_write_buffer[0]='\0';

        if(is_lock_needed) { 
          strcat(tmp_write_buffer, "lock_and_get_edge_property_");
        } else {
          strcat(tmp_write_buffer, "get_edge_property_");
        }
        strcat(tmp_write_buffer, tmp_type);
        strcat(tmp_write_buffer, "(");
        strcat(tmp_write_buffer, tmp_name);
        strcat(tmp_write_buffer, "_tmp");
        strcat(tmp_write_buffer, tempvarcount_str);
        strcat(tmp_write_buffer, ",");
        strcat(tmp_write_buffer, tmp_name);
        strcat(tmp_write_buffer, "_index");
        strcat(tmp_write_buffer, ",");
        strcat(tmp_write_buffer, "r_");
        strcat(tmp_write_buffer, rankvarcount_str);
        strcat(tmp_write_buffer, ",");
        strcat(tmp_write_buffer, "ln_");
        strcat(tmp_write_buffer, rankvarcount_str);
        strcat(tmp_write_buffer, ");\n");
        Body.insert_at(fp1, curr_indent1, tmp_write_buffer);
        fp1 = fp1 + strlen(tmp_write_buffer) + curr_indent1*Body.get_tab_size();
      }

      if (is_flush_needed) {
        tmp_write_buffer[0]='\0';
        strcat(tmp_write_buffer, "MPI_Win_flush(r_");
        strcat(tmp_write_buffer, rankvarcount_str);
        strcat(tmp_write_buffer, ", win);\n");
        Body.insert_at(fp1, curr_indent1, tmp_write_buffer);
        fp1 = fp1 + strlen(tmp_write_buffer) + curr_indent1*Body.get_tab_size();
      }
#endif
#ifdef __USE_BARRIER_OPT
      if (!is_in_parallel_loop()) {
      MPI_GEN.found_atleat_a_prop = true;
        read_props_before_barrier->push_back(tmp_name);
      }
#endif
    }
    free(tmp_write_buffer);
  }





	void generate_mpi_get_block_without_lock(int fp1, int curr_indent1, gm_code_writer &Body,
      bool is_flush_needed){
    generate_mpi_get(fp1, curr_indent1, Body, is_flush_needed, false);
	}

	void generate_mpi_put_block(ast_field *f, gm_code_writer &Body)
	{
    if(is_red_lock_emitted()) {
      generate_mpi_put(f, Body, false);
    }else {
      generate_mpi_put(f, Body, true);
    }
	}

  void generate_mpi_put(ast_field *f, gm_code_writer &Body, bool need_lock)
  {
    const char* index_name;
    if (f->getTypeInfo()->is_node_property()) {
      index_name = get_lib()->node_index(f->get_first());
      //Body.push(index_name);
    } else if (f->getTypeInfo()->is_edge_property()) {

      if (f->is_rarrow()) { // [XXX] this feature is still not finished, should be re-visited
        const char* alias_name = f->get_first()->getSymInfo()->find_info_string(CPPBE_INFO_NEIGHBOR_ITERATOR);
        assert(alias_name != NULL);
        assert(strlen(alias_name) > 0);
        index_name = alias_name;
        //Body.push(alias_name);
      }
      // check if the edge is a back-edge
      else if (f->get_first()->find_info_bool(CPPBE_INFO_IS_REVERSE_EDGE)) {
        index_name = get_lib()->fw_edge_index(f->get_first());
        //Body.push(index_name);
      }
      else {
        // original edge index if semi-sorted
        //sprintf(temp, "%s.%s(%s)",
        //f->get_first()->getTypeInfo()->get_target_graph_id()->get_genname(),
        //GET_ORG_IDX,
        //f->get_first()->get_genname()
        //);
        //Body.push(temp);
        index_name = get_lib()->edge_index(f->get_first());
        //Body.push(index_name);
      }
    }
    else {
      assert(false);
    }

#ifdef __USE_VAR_REUSE_OPT__
    bool index_found_in_stack = false;
    if(is_in_parallel_loop())
      index_found_in_stack = check_in_vars_in_scope(index_name);
    if (index_found_in_stack) {
      // TODO: Check if rankvar_countstr gets the correct value.
        strcpy(rankvar_countstr, field_to_reuse_vars_map[index_name].c_str());
    } else {
      rankvar_count++;
      if(is_in_parallel_loop())
        insert_index_to_vars_in_scope(index_name);

      sprintf(rankvar_countstr, "%d", rankvar_count);

      insert_rank_countstr(index_name, rankvar_countstr);

      // int r_count;
      Body.push("int r_"); Body.push(rankvar_countstr); Body.push(";");
      Body.NL();
      // int ln_count;
      Body.push("int ln_"); Body.push(rankvar_countstr); Body.push(";");
      Body.NL();
    }

    if (f->getTypeInfo()->is_node_property()) {
      if (!index_found_in_stack) {
        // r_count = G.get_rank_node(node);
        Body.push("r_"); Body.push(rankvar_countstr); Body.push(" = ");
        Body.push("G.get_rank_node(");
        Body.push(index_name); Body.push(");");
        Body.NL();
        // ln_count = G.get_local_node_num(node);
        Body.push("ln_");
        Body.push(rankvar_countstr); Body.push(" = ");
        Body.push("G.get_local_node_num(");
        Body.push(index_name); Body.push(");");
        Body.NL();
      }

      if (!is_in_parallel_loop()) {
#ifdef __USE_BARRIER_OPT
        if(check_and_set_need_barrier_before_put()){
      MPI_GEN.found_atleat_a_prop = true;
          Body.pushln("MPI_Barrier(MPI_COMM_WORLD);");
          //if barrier is emitted clear all dependencies.
          clear_props_before_barrier();
        }
#else
        Body.pushln("MPI_Barrier(MPI_COMM_WORLD);");
#endif
        Body.push("if(r_"); Body.push(rankvar_countstr); Body.push(" == G.get_rank()) {"); Body.NL();
        const char *property_name = f->get_second()->get_genname();
#ifdef __USE_BARRIER_OPT
        written_props_before_barrier->push_back(property_name);
#endif
        Body.push(property_name); Body.push("[ln_"); Body.push(rankvar_countstr); Body.push(" ] = ");
        Body.push(property_name); Body.push("_tmp"); Body.push(tempvar_countstr); Body.push(";");
        Body.NL();
        Body.push("}");
        Body.NL();
#ifndef __USE_BARRIER_OPT
      Body.push("MPI_Barrier(MPI_COMM_WORLD);");
#endif
        Body.NL();
      } else {
        // MPI_Put(&node_prop_name_tmp, 1, node_prop_type, r_count,
        //         base_address[G.num_processess() * node_prop_name_index] +
        //         ln_count*sizeof(node_prop_type), 1, node_prop_type, win);
        // i.e, put_node_property_<type>(node_prop_name_temp, node_prop_name_index,
        //                               r_count, ln_count);
        //get_tempvar_name_and_type(&tmp_type, &tmp_name, f);
        ast_typedecl *tmp_type_decl = f->get_second()->getTargetTypeInfo();
        char *tmp_type;// = get_type_string(tmp_type_decl);
        // Append _tmp to it when it is used in in Body.push.


        switch (tmp_type_decl->getTypeSummary()) {
          case GMTYPE_INT:
            tmp_type = "int";
            //Body.push("G.win_attach_int");
            //Body.push(ALLOCATE_INT);
            break;
          case GMTYPE_LONG:
            tmp_type = "long";
            break;
          case GMTYPE_BOOL:
            tmp_type = "bool";
            //Body.push(ALLOCATE_BOOL);
            break;
          case GMTYPE_DOUBLE:
            tmp_type = "double";
            break;
          case GMTYPE_FLOAT:
            tmp_type = "float";
            break;
          case GMTYPE_NODE:
            tmp_type = "int";
            //Body.push(ALLOCATE_NODE);
            break;
          case GMTYPE_EDGE:
            tmp_type = "int";
            //Body.push(ALLOCATE_EDGE);
            break;
          case GMTYPE_NSET:
          case GMTYPE_ESET:
          case GMTYPE_NSEQ:
          case GMTYPE_ESEQ:
          case GMTYPE_NORDER:
          case GMTYPE_EORDER: {
                                //char temp[128];
                                //bool lazyInitialization = false; //TODO: get some information here to check if lazy init is better
                                //sprintf(temp, "%s<%s, %s>", ALLOCATE_COLLECTION, get_lib()->get_type_string(t2), (lazyInitialization ? "true" : "false"));
                                //Body.push(temp);
                                break;
                              }
          default:
                              assert(false);
                              break;
        }

        const char *tmp_name = f->get_second()->get_genname();
        if(need_lock) {
          Body.push("lock_and_put_node_property_"); 
        }
        else {
          Body.push("put_node_property_"); 
        }
        Body.push(tmp_type); Body.push("(");
        Body.push(tmp_name); Body.push("_tmp"); Body.push(tempvar_countstr); Body.push(",");
        Body.push(tmp_name); Body.push("_index"); Body.push(",");
        Body.push("r_"); Body.push(rankvar_countstr); Body.push(",");
        Body.push("ln_"); Body.push(rankvar_countstr);
        Body.push(");");
        Body.NL();
      }
    }
    else if (f->getTypeInfo()->is_edge_property()) {
      if (!index_found_in_stack) {
        // r_count = G.get_rank_node(node);
        Body.push("r_"); Body.push(rankvar_countstr); Body.push(" = ");
        Body.push("G.get_rank_edge(");
        Body.push(index_name); Body.push(");");
        Body.NL();
        // ln_count = G.get_local_node_num(node);
        Body.push("ln_");
        Body.push(rankvar_countstr); Body.push(" = ");
        Body.push("G.get_local_edge_num(");
        Body.push(index_name); Body.push(");");
        Body.NL();
      }
      if (!is_in_parallel_loop()) {
        const char *property_name = f->get_second()->get_genname();
#ifdef __USE_BARRIER_OPT
        if(check_and_set_need_barrier_before_put()){
      MPI_GEN.found_atleat_a_prop = true;
          Body.pushln("MPI_Barrier(MPI_COMM_WORLD);");
          //if barrier is emitted clear all dependencies.
          clear_props_before_barrier();
        }
#else
        Body.pushln("MPI_Barrier(MPI_COMM_WORLD);");
#endif
#ifdef __USE_BARRIER_OPT
        written_props_before_barrier->push_back(property_name);
#endif
        Body.push("if(r_"); Body.push(rankvar_countstr); Body.push(" == G.get_rank()) {"); Body.NL();
        Body.push(property_name); Body.push("[ln_"); Body.push(rankvar_countstr); Body.push(" ] = ");
        Body.push(property_name); Body.push("_tmp"); Body.push(tempvar_countstr); Body.push(";");
        Body.NL();
        Body.push("}");
        Body.NL();
#ifndef __USE_BARRIER_OPT
      Body.push("MPI_Barrier(MPI_COMM_WORLD);");
#endif
        Body.NL();
      } else {
        // MPI_Put(&node_prop_name_tmp, 1, node_prop_type, r_count,
        //         base_address[G.num_processess() * node_prop_name_index] +
        //         ln_count*sizeof(node_prop_type), 1, node_prop_type, win);
        // i.e, put_node_property_<type>(node_prop_name_temp, node_prop_name_index,
        //                               r_count, ln_count);
        //get_tempvar_name_and_type(&tmp_type, &tmp_name, f);
        ast_typedecl *tmp_type_decl = f->get_second()->getTargetTypeInfo();
        const char *tmp_type;// = get_type_string(tmp_type_decl);
        // Append _tmp to it when it is used in in Body.push.


        switch (tmp_type_decl->getTypeSummary()) {
          case GMTYPE_INT:
            tmp_type = "int";
            //Body.push("G.win_attach_int");
            //Body.push(ALLOCATE_INT);
            break;
          case GMTYPE_LONG:
            tmp_type = "long";
            break;
          case GMTYPE_BOOL:
            tmp_type = "bool";
            //Body.push(ALLOCATE_BOOL);
            break;
          case GMTYPE_DOUBLE:
            tmp_type = "double";
            break;
          case GMTYPE_FLOAT:
            tmp_type = "float";
            break;
          case GMTYPE_NODE:
            //Body.push(ALLOCATE_NODE);
            break;
          case GMTYPE_EDGE:
            //Body.push(ALLOCATE_EDGE);
            break;
          case GMTYPE_NSET:
          case GMTYPE_ESET:
          case GMTYPE_NSEQ:
          case GMTYPE_ESEQ:
          case GMTYPE_NORDER:
          case GMTYPE_EORDER: {
                                //char temp[128];
                                //bool lazyInitialization = false; //TODO: get some information here to check if lazy init is better
                                //sprintf(temp, "%s<%s, %s>", ALLOCATE_COLLECTION, get_lib()->get_type_string(t2), (lazyInitialization ? "true" : "false"));
                                //Body.push(temp);
                                break;
                              }
          default:
                              assert(false);
                              break;
        }


        const char *tmp_name = f->get_second()->get_genname();
        if(need_lock) {
          Body.push("lock_and_put_edge_property_"); 
        }
        else {
          Body.push("put_edge_property_"); 
        }
        Body.push(tmp_type); Body.push("(");
        Body.push(tmp_name); Body.push("_tmp"); Body.push(tempvar_countstr); Body.push(",");
        Body.push(tmp_name); Body.push("_index"); Body.push(",");
        Body.push("r_"); Body.push(rankvar_countstr); Body.push(",");
        Body.push("ln_"); Body.push(rankvar_countstr);
        Body.push(");");
        Body.NL();
      }
    }

#else

  rankvar_count++;
  sprintf(rankvar_countstr, "%d", rankvar_count);

  // int r_count;
  Body.push("int r_"); Body.push(rankvar_countstr); Body.push(";");
  Body.NL();
  // int ln_count;
  Body.push("int ln_"); Body.push(rankvar_countstr); Body.push(";");
  Body.NL();

  if (f->getTypeInfo()->is_node_property()) {
    // r_count = G.get_rank_node(node);
    Body.push("r_"); Body.push(rankvar_countstr); Body.push(" = ");
    Body.push("G.get_rank_node(");
    Body.push(index_name); Body.push(");");
    Body.NL();
    // ln_count = G.get_local_node_num(node);
    Body.push("ln_");
    Body.push(rankvar_countstr); Body.push(" = ");
    Body.push("G.get_local_node_num(");
    Body.push(index_name); Body.push(");");
    Body.NL();

    if (!is_in_parallel_loop()) {
      const char *property_name = f->get_second()->get_genname();
#ifdef __USE_BARRIER_OPT
        if(check_and_set_need_barrier_before_put()){
      MPI_GEN.found_atleat_a_prop = true;
          Body.pushln("MPI_Barrier(MPI_COMM_WORLD);");
          //if barrier is emitted clear all dependencies.
          clear_props_before_barrier();
        }
#else
        Body.pushln("MPI_Barrier(MPI_COMM_WORLD);");
#endif
#ifdef __USE_BARRIER_OPT
        written_props_before_barrier->push_back(property_name);
#endif
      Body.push("if(r_"); Body.push(rankvar_countstr); Body.push(" == G.get_rank()) {"); Body.NL();
      Body.push(property_name); Body.push("[ln_"); Body.push(rankvar_countstr); Body.push(" ] = ");
      Body.push(property_name); Body.push("_tmp"); Body.push(tempvar_countstr); Body.push(";");
      Body.NL();
      Body.push("}");
      Body.NL();
#ifndef __USE_BARRIER_OPT
      Body.push("MPI_Barrier(MPI_COMM_WORLD);");
#endif
      Body.NL();
    } else {
      // MPI_Put(&node_prop_name_tmp, 1, node_prop_type, r_count,
      //         base_address[G.num_processess() * node_prop_name_index] +
      //         ln_count*sizeof(node_prop_type), 1, node_prop_type, win);
      // i.e, put_node_property_<type>(node_prop_name_temp, node_prop_name_index,
      //                               r_count, ln_count);
      //get_tempvar_name_and_type(&tmp_type, &tmp_name, f);
      ast_typedecl *tmp_type_decl = f->get_second()->getTargetTypeInfo();
      char *tmp_type;// = get_type_string(tmp_type_decl);
      // Append _tmp to it when it is used in in Body.push.


      switch (tmp_type_decl->getTypeSummary()) {
        case GMTYPE_INT:
          tmp_type = "int";
          //Body.push("G.win_attach_int");
          //Body.push(ALLOCATE_INT);
          break;
        case GMTYPE_LONG:
          tmp_type = "long";
          break;
        case GMTYPE_BOOL:
          tmp_type = "bool";
          //Body.push(ALLOCATE_BOOL);
          break;
        case GMTYPE_DOUBLE:
          tmp_type = "double";
          break;
        case GMTYPE_FLOAT:
          tmp_type = "float";
          break;
        case GMTYPE_NODE:
          tmp_type = "int";
          //Body.push(ALLOCATE_NODE);
          break;
        case GMTYPE_EDGE:
          tmp_type = "int";
          //Body.push(ALLOCATE_EDGE);
          break;
        case GMTYPE_NSET:
        case GMTYPE_ESET:
        case GMTYPE_NSEQ:
        case GMTYPE_ESEQ:
        case GMTYPE_NORDER:
        case GMTYPE_EORDER: {
                              //char temp[128];
                              //bool lazyInitialization = false; //TODO: get some information here to check if lazy init is better
                              //sprintf(temp, "%s<%s, %s>", ALLOCATE_COLLECTION, get_lib()->get_type_string(t2), (lazyInitialization ? "true" : "false"));
                              //Body.push(temp);
                              break;
                            }
        default:
                            assert(false);
                            break;
      }

      const char *tmp_name = f->get_second()->get_genname();
      if(need_lock) {
        Body.push("lock_and_put_node_property_"); 
      }
      else {
        Body.push("put_node_property_"); 
      }
      Body.push(tmp_type); Body.push("(");
      Body.push(tmp_name); Body.push("_tmp"); Body.push(tempvar_countstr); Body.push(",");
      Body.push(tmp_name); Body.push("_index"); Body.push(",");
      Body.push("r_"); Body.push(rankvar_countstr); Body.push(",");
      Body.push("ln_"); Body.push(rankvar_countstr);
      Body.push(");");
      Body.NL();
    }
  }
  else if (f->getTypeInfo()->is_edge_property()) {
    // r_count = G.get_rank_node(node);
    Body.push("r_"); Body.push(rankvar_countstr); Body.push(" = ");
    Body.push("G.get_rank_edge(");
    Body.push(index_name); Body.push(");");
    Body.NL();
    // ln_count = G.get_local_node_num(node);
    Body.push("ln_");
    Body.push(rankvar_countstr); Body.push(" = ");
    Body.push("G.get_local_edge_num(");
    Body.push(index_name); Body.push(");");
    Body.NL();
    if (!is_in_parallel_loop()) {
#ifdef __USE_BARRIER_OPT
        if(check_and_set_need_barrier_before_put()){
      MPI_GEN.found_atleat_a_prop = true;
          Body.pushln("MPI_Barrier(MPI_COMM_WORLD);");
          //if barrier is emitted clear all dependencies.
          clear_props_before_barrier();
        }
#else
        Body.pushln("MPI_Barrier(MPI_COMM_WORLD);");
#endif
      Body.push("if(r_"); Body.push(rankvar_countstr); Body.push(" == G.get_rank()) {"); Body.NL();
      const char *property_name = f->get_second()->get_genname();
#ifdef __USE_BARRIER_OPT
        written_props_before_barrier->push_back(property_name);
#endif
      Body.push(property_name); Body.push("[ln_"); Body.push(rankvar_countstr); Body.push(" ] = ");
      Body.push(property_name); Body.push("_tmp"); Body.push(tempvar_countstr); Body.push(";");
      Body.NL();
      Body.push("}");
      Body.NL();
#ifndef __USE_BARRIER_OPT
      Body.push("MPI_Barrier(MPI_COMM_WORLD);");
#endif
      Body.NL();
    } else {
      // MPI_Put(&node_prop_name_tmp, 1, node_prop_type, r_count,
      //         base_address[G.num_processess() * node_prop_name_index] +
      //         ln_count*sizeof(node_prop_type), 1, node_prop_type, win);
      // i.e, put_node_property_<type>(node_prop_name_temp, node_prop_name_index,
      //                               r_count, ln_count);
      //get_tempvar_name_and_type(&tmp_type, &tmp_name, f);
      ast_typedecl *tmp_type_decl = f->get_second()->getTargetTypeInfo();
      const char *tmp_type;// = get_type_string(tmp_type_decl);
      // Append _tmp to it when it is used in in Body.push.


      switch (tmp_type_decl->getTypeSummary()) {
        case GMTYPE_INT:
          tmp_type = "int";
          //Body.push("G.win_attach_int");
          //Body.push(ALLOCATE_INT);
          break;
        case GMTYPE_LONG:
          tmp_type = "long";
          break;
        case GMTYPE_BOOL:
          tmp_type = "bool";
          //Body.push(ALLOCATE_BOOL);
          break;
        case GMTYPE_DOUBLE:
          tmp_type = "double";
          break;
        case GMTYPE_FLOAT:
          tmp_type = "float";
          break;
        case GMTYPE_NODE:
          //Body.push(ALLOCATE_NODE);
          break;
        case GMTYPE_EDGE:
          //Body.push(ALLOCATE_EDGE);
          break;
        case GMTYPE_NSET:
        case GMTYPE_ESET:
        case GMTYPE_NSEQ:
        case GMTYPE_ESEQ:
        case GMTYPE_NORDER:
        case GMTYPE_EORDER: {
                              //char temp[128];
                              //bool lazyInitialization = false; //TODO: get some information here to check if lazy init is better
                              //sprintf(temp, "%s<%s, %s>", ALLOCATE_COLLECTION, get_lib()->get_type_string(t2), (lazyInitialization ? "true" : "false"));
                              //Body.push(temp);
                              break;
                            }
        default:
                            assert(false);
                            break;
      }


      const char *tmp_name = f->get_second()->get_genname();
      if(need_lock) {
        Body.push("lock_and_put_edge_property_"); 
      }
      else {
        Body.push("put_edge_property_"); 
      }
      Body.push(tmp_type); Body.push("(");
      Body.push(tmp_name); Body.push("_tmp"); Body.push(tempvar_countstr); Body.push(",");
      Body.push(tmp_name); Body.push("_index"); Body.push(",");
      Body.push("r_"); Body.push(rankvar_countstr); Body.push(",");
      Body.push("ln_"); Body.push(rankvar_countstr);
      Body.push(");");
      Body.NL();
    }
  }

#endif
}

void generate_rhs_for_local_reduction(ast_assign *a)
{
  GM_REDUCE_T r_type = (GM_REDUCE_T) a->get_reduce_type();
  bool is_scalar = (a->get_lhs_type() == GMASSIGN_LHS_SCALA);
  if (is_scalar)
  {
    switch (r_type) {
      case GMREDUCE_PLUS:
        generate_rhs_id(a->get_lhs_scala()); Body.push("+"); generate_expr(a->get_rhs()); Body.push(";");
        break;    
      case GMREDUCE_MULT:
        generate_rhs_id(a->get_lhs_scala()); Body.push("*"); generate_expr(a->get_rhs()); Body.push(";");
        break;    
      case GMREDUCE_AND:
        generate_rhs_id(a->get_lhs_scala()); Body.push("&"); Body.push("(");generate_expr(a->get_rhs()); Body.push(");");
        break;    
      case GMREDUCE_OR:
        generate_rhs_id(a->get_lhs_scala()); Body.push("|"); Body.push("(");generate_expr(a->get_rhs()); Body.push(");");
        break;    
      case GMREDUCE_MIN:
        Body.push("std::min(");generate_rhs_id(a->get_lhs_scala()); Body.push(",");generate_expr(a->get_rhs()); Body.push(");");
        break;    
      case GMREDUCE_MAX:
        Body.push("std::max(");generate_rhs_id(a->get_lhs_scala()); Body.push(",");generate_expr(a->get_rhs()); Body.push(");");
        break;    
      default:
        assert(false);
    }

  }
  else
  {
    switch (r_type) {
      case GMREDUCE_PLUS:
        generate_rhs_field(a->get_lhs_field()); Body.push("+"); generate_expr(a->get_rhs()); Body.push(";");
        break;    
      case GMREDUCE_MULT:
        generate_rhs_field(a->get_lhs_field()); Body.push("*"); generate_expr(a->get_rhs()); Body.push(";");
        break;    
      case GMREDUCE_AND:
        generate_rhs_field(a->get_lhs_field());  Body.push("&"); Body.push("(");generate_expr(a->get_rhs()); Body.push(");");
        break;    
      case GMREDUCE_OR:
        generate_rhs_field(a->get_lhs_field()); Body.push("|"); Body.push("(");generate_expr(a->get_rhs()); Body.push(");");
        break;    
      case GMREDUCE_MIN:
        Body.push("std::min("); generate_rhs_field(a->get_lhs_field()); Body.push(",");generate_expr(a->get_rhs()); Body.push(");");
        break;    
      case GMREDUCE_MAX:
        Body.push("std::max("); generate_rhs_field(a->get_lhs_field()); Body.push(",");generate_expr(a->get_rhs()); Body.push(");");
        break;    
      default:
        assert(false);
    }
  }
}


};

#endif
