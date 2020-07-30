#include <stdio.h>
#include <iostream>
#include <vector>
#include "gm_backend_cpp.h"
#include "gm_error.h"
#include "gm_code_writer.h"
#include "gm_frontend.h"
#include "gm_transform_helper.h"
#include "gm_builtin.h"
#include "gm_cpplib_words.h"
#include "gm_argopts.h"
#include "gm_backend_mpi.h"

using namespace std;
//bool remote_read = false;
//ast_field remote_read_field;

//mpi
//extern int tmpvar_count = 0;
//extern int rankvar_count = 0;
//extern char rankvar_countstr[32];
//extern char tempvar_countstr[32];

//extern vector <gm_mpi_get_info *> mpi_get_details;

gm_mpi_gen MPI_GEN;

void gm_cpp_gen::setTargetDir(const char* d) {
	assert(d != NULL);
	if (dname != NULL) delete[] dname;
	dname = new char[strlen(d) + 1];
	strcpy(dname, d);
}

void gm_cpp_gen::setFileName(const char* f) {
	assert(f != NULL);
	if (fname != NULL) delete[] fname;
	fname = new char[strlen(f) + 1];
	strcpy(fname, f);
}

bool gm_cpp_gen::open_output_files() {
	char temp[1024];
	assert(dname != NULL);
	assert(fname != NULL);

	sprintf(temp, "%s/%s.h", dname, fname);
	f_header = fopen(temp, "w");
	if (f_header == NULL) {
		gm_backend_error(GM_ERROR_FILEWRITE_ERROR, temp);
		return false;
	}
	Header.set_output_file(f_header);

	sprintf(temp, "%s/%s.cc", dname, fname);
	f_body = fopen(temp, "w");
	if (f_body == NULL) {
		gm_backend_error(GM_ERROR_FILEWRITE_ERROR, temp);
		return false;
	}
	Body.set_output_file(f_body);

	get_lib()->set_code_writer(&Body);

	if (OPTIONS.get_arg_bool(GMARGFLAG_CPP_CREATE_MAIN)) {
		sprintf(temp, "%s/%s_compile.mk", dname, fname);
		f_shell = fopen(temp, "w");
		if (f_shell == NULL) {
			gm_backend_error(GM_ERROR_FILEWRITE_ERROR, temp);
			return false;
		}
	}
	return true;
}

void gm_cpp_gen::close_output_files(bool remove_files) {
	char temp[1024];
	if (f_header != NULL) {
		Header.flush();
		fclose(f_header);
		if (remove_files) {
			sprintf(temp, "rm %s/%s.h", dname, fname);
			system(temp);
		}
		f_header = NULL;
	}
	if (f_body != NULL) {
		Body.flush();
		fclose(f_body);
		if (remove_files) {
			sprintf(temp, "rm %s/%s.cc", dname, fname);
			system(temp);
		}
		f_body = NULL;
	}

	if (f_shell != NULL) {
		fclose(f_shell);
		f_shell = NULL;
		if (remove_files) {
			sprintf(temp, "rm %s/%s_compile.mk", dname, fname);
			system(temp);
		}
	}
}

void gm_cpp_gen::add_include(const char* string, gm_code_writer& Out, bool is_clib, const char* str2) {
	Out.push("#include ");
	if (is_clib)
		Out.push('<');
	else
		Out.push('"');
	Out.push(string);
	Out.push(str2);
	if (is_clib)
		Out.push('>');
	else
		Out.push('"');
	Out.NL();
}
void gm_cpp_gen::add_ifdef_protection(const char* s) {
	Header.push("#ifndef GM_GENERATED_CPP_");
	Header.push_to_upper(s);
	Header.pushln("_H");
	Header.push("#define GM_GENERATED_CPP_");
	Header.push_to_upper(s);
	Header.pushln("_H");
	Header.NL();
}

void gm_cpp_gen::do_generate_begin() {
	//----------------------------------
	// header
	//----------------------------------
	add_ifdef_protection(fname);
	add_include("stdio.h", Header);
	add_include("stdlib.h", Header);
	add_include("stdint.h", Header);
	add_include("float.h", Header);
	add_include("limits.h", Header);
	add_include("cmath", Header);
	add_include("algorithm", Header);
	add_include("omp.h", Header);
	//add_include(get_lib()->get_header_info(), Header, false);
	add_include(RT_INCLUDE, Header, false);
	Header.NL();

	//----------------------------------------
	// Body
	//----------------------------------------
	sprintf(temp, "%s.h", fname);
	add_include(temp, Body, false);
	Body.NL();
#ifdef __USE_BSP_OPT__

	MPI_GEN.beg_cur_int = 0;
	MPI_GEN.beg_fp = Body.get_write_ptr(MPI_GEN.beg_cur_int);
  //generate_opt_inits();
#endif
}

#ifdef __USE_BSP_OPT__
void gm_cpp_gen::generate_opt_inits() {
  //ast_field *f = MPI_GEN.updated_props[0];

  char *temp = new char[4096]; 
  temp[0] = '\0';
  sprintf(temp, "win_attach_meta_props();\n");
  Body.insert_at(MPI_GEN.st_blk_fp, MPI_GEN.st_blk_cur_int,temp);
  temp[0] = '\0';
  sprintf(temp, "initialize(G);\n");
  Body.insert_at(MPI_GEN.st_blk_fp, MPI_GEN.st_blk_cur_int,temp);
  vector<char*> prop_names;
  vector<char*> prop_types;
  vector<ast_field *>::iterator itr = MPI_GEN.updated_props.begin();
  for(; itr != MPI_GEN.updated_props.end(); itr++){
    ast_field *f = *itr;
    MPI_GEN.prop_temp_no++;
    char *prop_name = new char[100];
    prop_name[0] = '\0';
    char prop_no[10];
    sprintf(prop_no, "%d",MPI_GEN.prop_temp_no);
    strcat(prop_name, "prop" ); 
    strcat(prop_name, prop_no ); 
    prop_names.push_back(prop_name);

    ast_typedecl *tmp_type_decl = f->get_second()->getTargetTypeInfo();
    char *tmp_type = new char[20];
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
      prop_types.push_back(tmp_type);
      Body.push("/*");
      Body.push(prop_types[0]);
      Body.push(prop_names[0]);
      Body.push("*/");


  }
  MPI_GEN.prop_names = prop_names;
  MPI_GEN.prop_types = prop_types;

  char *write_buf = new char[4096]; 
  write_buf[0] = '\0';
  strcat( write_buf, "int _my_rank, _block_size, _num_process, num_nodes, max_neighbors;\n");
   strcat( write_buf, "class proc_update_details {\n");
    strcat( write_buf, " public:\n"); 
     
    //TODO: Now handled only for one push; however a single reduction
    //could result in multiple pushes
      strcat( write_buf, prop_types[0]);
      strcat( write_buf, " * prop;\n");
     strcat( write_buf, "int32_t* v_to_be_updated;\n");
     strcat( write_buf, "int prop_max_size;\n");

     strcat( write_buf, "int index_of_v;\n");

      strcat( write_buf, "proc_update_details() {\n");

     strcat( write_buf, "   prop = new ");
      strcat( write_buf, prop_types[0]);
     strcat(write_buf, "[max_neighbors];\n");

     strcat( write_buf, "   v_to_be_updated = new int32_t[max_neighbors];\n");

 strcat( write_buf, "       prop_max_size = max_neighbors;\n");
       strcat( write_buf, " index_of_v = 0;\n");
 strcat( write_buf, "     }\n");

 strcat( write_buf, "void set_prop(int32_t v, ");
  strcat (write_buf, prop_types[0]);
  strcat(write_buf, " d) {\n");
 strcat( write_buf, "prop[index_of_v] = d;\n");
 strcat( write_buf, "v_to_be_updated[index_of_v] = v;\n");
 strcat( write_buf, "index_of_v++;\n");	
 strcat( write_buf, "}\n");

 strcat( write_buf, "void reset() {\n");
 strcat( write_buf, "  index_of_v = 0;\n");
 strcat( write_buf, "}\n");

 strcat( write_buf, "};\n");

 strcat( write_buf, " MPI_Win *_meta_propwin;\n");
 strcat( write_buf, " MPI_Win *_meta_v_win;\n");
 strcat( write_buf, " MPI_Win *_meta_num_v_win;\n");
 strcat( write_buf, " proc_update_details *prop_buffer;\n");


  strcat( write_buf, "void win_attach_meta_props() {\n");
   strcat( write_buf, "int i;\n");

    strcat( write_buf, "for(i=0; i<_num_process; i++) {\n");
      //attaches property
     strcat( write_buf, "  create_win_");
     strcat( write_buf, prop_types[0]);
     strcat(write_buf,"(prop_buffer[i].prop, prop_buffer[i].prop_max_size, _meta_propwin[i]);\n");
      //attaches v_to_be_updated
      strcat( write_buf, " create_win_int(prop_buffer[i].v_to_be_updated, prop_buffer[i].prop_max_size, _meta_v_win[i]);\n");
      // attaches index_of_v
      strcat( write_buf, " create_win_int(&prop_buffer[i].index_of_v, 1, _meta_num_v_win[i]);\n");
    strcat( write_buf, " }\n");


  strcat( write_buf, " }\n");


  strcat( write_buf, "void initialize(gm_graph &G) {\n");
  strcat( write_buf, "_my_rank = G.get_rank();\n");
 strcat( write_buf, "_num_process = G.get_num_processes();\n");
    strcat( write_buf, "_block_size = G.get_block_size();\n");
   strcat( write_buf, " num_nodes = G.num_nodes();\n");

   strcat( write_buf, " _meta_propwin = new MPI_Win[_num_process];\n");
   strcat( write_buf, " _meta_v_win = new MPI_Win[_num_process]; \n");
   strcat( write_buf, " _meta_num_v_win = new MPI_Win[_num_process];\n");


   strcat( write_buf, " int start_node = 0;\n");
    strcat( write_buf, "int next_to_end_node = _block_size;\n");
   strcat( write_buf, " if (_my_rank == _num_process - 1 ) {\n");
    strcat( write_buf, "  next_to_end_node =  num_nodes-(_my_rank * _block_size);\n");
   strcat( write_buf, " }\n");


  strcat( write_buf, "  max_neighbors = G.local_begin[next_to_end_node] - G.local_begin[start_node];\n");
  strcat( write_buf, "  prop_buffer = new proc_update_details[_num_process];\n");


  strcat( write_buf, "}\n");
  Body.insert_at(MPI_GEN.beg_fp, MPI_GEN.beg_cur_int, write_buf);
}
#endif



void gm_cpp_gen::do_generate_end() {
	Header.NL();
	Header.pushln("#endif");
}

void gm_cpp_gen::generate_proc(ast_procdef* proc) {
	//-------------------------------
	// declare function name 
	//-------------------------------
   
	generate_proc_decl(proc, false);  // declare in header file
  MPI_GEN.local_access_nodes.clear();
  MPI_GEN.local_access_edges.clear();

	//-------------------------------
	// BFS definitions
	//-------------------------------
	if (proc->find_info_bool(CPPBE_INFO_HAS_BFS)) {
		ast_extra_info_list* L = (ast_extra_info_list*) proc->find_info(CPPBE_INFO_BFS_LIST);
		assert(L != NULL);
		std::list<void*>::iterator I;
		Body.NL();
		Body.pushln("// BFS/DFS definitions for the procedure");
		for (I = L->get_list().begin(); I != L->get_list().end(); I++) {
			ast_bfs* bfs = (ast_bfs*) *I;
			generate_bfs_def(bfs);
		}
	}

	//-------------------------------
	// function definition
	//-------------------------------
	generate_proc_decl(proc, true);   // declare in body file

	MPI_GEN.proc = proc;

	generate_sent(proc->get_body());
	Body.NL();

	return;
}

void gm_cpp_gen::generate_proc_decl(ast_procdef* proc, bool is_body_file) {
	// declare in the header or body
	gm_code_writer& Out = is_body_file ? Body : Header;

	if (!is_body_file && proc->is_local()) return;

	if(!is_body_file)
	{






		Out.pushln("#define MASTER 0");

		Out.push("#define lock_win_for_node(_node)\\");
		Out.NL();
		Out.push("{\\");
		Out.NL();
		Out.push("int _err_val;\\");
		Out.NL();
		Out.push("int _rank;\\");
		Out.NL();
		Out.push("_rank = G.get_rank_node(_node);\\");
		Out.NL();
		Out.push("_err_val=MPI_Win_lock(MPI_LOCK_EXCLUSIVE,_rank, 0, win);\\");
		Out.NL();
		Out.push("if(_err_val != MPI_SUCCESS)\\");
		Out.NL();
		Out.push("fprintf(stderr,\"error occured for lock with val%d\",_err_val);\\");
		Out.NL();
		Out.push("}");
		Out.NL();
		Out.NL();

		Out.push("#define unlock_win_for_node(_node)\\");
		Out.NL();
		Out.push("{\\");
		Out.NL();
		Out.push("int _err_val;\\");
		Out.NL();
		Out.push("int _rank;\\");
		Out.NL();
		Out.push("_rank = G.get_rank_node(_node);\\");
		Out.NL();
		Out.push("_err_val=MPI_Win_unlock(_rank, win);\\");
		Out.NL();
		Out.push("if(_err_val != MPI_SUCCESS)\\");
		Out.NL();
		Out.push("fprintf(stderr,\"error occured for lock with val%d\",_err_val);\\");
		Out.NL();
		Out.push("}");
		Out.NL();
		Out.NL();


		Out.push("#define lock_and_get_node_property_int(_result, _ba_index, _rank, _prop_index)\\");
		Out.NL();
		Out.push("{\\");
		Out.NL();
		Out.push("int _err_val;\\");
		Out.NL();
		Out.push("_err_val=MPI_Win_lock(MPI_LOCK_EXCLUSIVE,_rank, 0, win);\\");
		Out.NL();
		Out.push("if(_err_val != MPI_SUCCESS)\\");
		Out.NL();
		Out.push("fprintf(stderr,\"error occured for lock with val%d\",_err_val);\\");
		Out.NL();
		Out.push("MPI_Get(&_result, 1, MPI_INT, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_prop_index*sizeof(int), 1, MPI_INT, win);\\");
		Out.NL();
		Out.push("MPI_Win_unlock(_rank, win);\\");
		Out.NL();
		Out.push("}");
		Out.NL();
		Out.NL();


		Out.push("#define lock_and_get_edge_property_int(_result, _ba_index, _rank, _prop_index)\\");
		Out.NL();
		Out.push("{\\");
		Out.NL();
		Out.push("int _err_val;\\");
		Out.NL();
		Out.push("_err_val=MPI_Win_lock(MPI_LOCK_EXCLUSIVE,_rank, 0, win);\\");
		Out.NL();
		Out.push("if(_err_val != MPI_SUCCESS)\\");
		Out.NL();
		Out.push("fprintf(stderr,\"error occured for lock with val%d\",_err_val);\\");
		Out.NL();
		Out.push("MPI_Get(&_result, 1, MPI_INT, _rank, base_address[G.get_num_processes()*_ba_index+_rank] + _prop_index*sizeof(int), 1, MPI_INT, win);\\");
		Out.NL();
		Out.push("MPI_Win_unlock(_rank, win);\\");
		Out.NL();
		Out.push("}");
		Out.NL();
		Out.NL();



		Out.push("#define lock_and_put_node_property_int(_result, _ba_index, _rank, _prop_index)\\");
		Out.NL();
		Out.push("{\\");
		Out.NL();
		Out.push("int _err_val;\\");
		Out.NL();
		Out.push("_err_val=MPI_Win_lock(MPI_LOCK_EXCLUSIVE,_rank, 0, win);\\");
		Out.NL();
		Out.push("if(_err_val != MPI_SUCCESS)\\");
		Out.NL();
		Out.push("fprintf(stderr,\"error occured for lock with val%d\",_err_val);\\");
		Out.NL();
		Out.push("MPI_Put(&_result, 1, MPI_INT, _rank, base_address[G.get_num_processes()*_ba_index +_rank] + _prop_index*sizeof(int), 1, MPI_INT, win);\\");
		Out.NL();
		Out.push("MPI_Win_unlock(_rank, win);\\");
		Out.NL();
		Out.push("}");
		Out.NL();
		Out.NL();



		Out.push("#define lock_and_put_edge_property_int(_result, _ba_index, _rank, _prop_index)\\");
		Out.NL();
		Out.push("{\\");
		Out.NL();
		Out.push("int _err_val;\\");
		Out.NL();
		Out.push("_err_val=MPI_Win_lock(MPI_LOCK_EXCLUSIVE,_rank, 0, win);\\");
		Out.NL();
		Out.push("if(_err_val != MPI_SUCCESS)\\");
		Out.NL();
		Out.push("fprintf(stderr,\"error occured for lock with val%d\",_err_val);\\");
		Out.NL();
		Out.push("MPI_Put(&_result, 1, MPI_INT, _rank, base_address[G.get_num_processes()*_ba_index + _rank]+_prop_index*sizeof(int), 1, MPI_INT, win);\\");
		Out.NL();
		Out.push("MPI_Win_unlock(_rank, win);\\");
		Out.NL();
		Out.push("}");
		Out.NL();
		Out.NL();



		Out.push("#define get_node_property_int(_result, _ba_index, _rank, _prop_index)\\");
		Out.NL();
		Out.push("{\\");
		Out.NL();
		Out.push("MPI_Get(&_result, 1, MPI_INT, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_prop_index*sizeof(int), 1, MPI_INT, win);\\");
		Out.NL();
		Out.push("}");
		Out.NL();
		Out.NL();


		Out.push("#define get_edge_property_int(_result, _ba_index, _rank, _prop_index)\\");
		Out.NL();
		Out.push("{\\");
		Out.NL();
		Out.push("MPI_Get(&_result, 1, MPI_INT, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_prop_index*sizeof(int), 1, MPI_INT, win);\\");
		Out.NL();
		Out.push("}");
		Out.NL();
		Out.NL();



		Out.push("#define put_node_property_int(_result, _ba_index, _rank, _prop_index)\\");
		Out.NL();
		Out.push("{\\");
		Out.NL();
		Out.push("MPI_Put(&_result, 1, MPI_INT, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_prop_index*sizeof(int), 1, MPI_INT, win);\\");
		Out.NL();
		Out.push("}");
		Out.NL();
		Out.NL();



		Out.push("#define put_edge_property_int(_result, _ba_index, _rank, _prop_index)\\");
		Out.NL();
		Out.push("{\\");
		Out.NL();
		Out.push("MPI_Put(&_result, 1, MPI_INT, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_prop_index*sizeof(int), 1, MPI_INT, win);\\");
		Out.NL();
		Out.push("}");
		Out.NL();
		Out.NL();


		Out.push("#define lock_and_get_node_property_bool(_result, _ba_index, _rank, _prop_index)\\");
		Out.NL();
		Out.push("{\\");
		Out.NL();
		Out.push("int _err_val;\\");
		Out.NL();
		Out.push("_err_val=MPI_Win_lock(MPI_LOCK_EXCLUSIVE,_rank, 0, win);\\");
		Out.NL();
		Out.push("if(_err_val != MPI_SUCCESS)\\");
		Out.NL();
		Out.push("fprintf(stderr,\"error occured for lock with val%d\",_err_val);\\");
		Out.NL();
		Out.push("MPI_Get(&_result, 1, MPI_C_BOOL, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_prop_index*sizeof(bool), 1, MPI_C_BOOL, win);\\");
		Out.NL();
		Out.push("MPI_Win_unlock(_rank, win);\\");
		Out.NL();
		Out.push("}");
		Out.NL();
		Out.NL();


		Out.push("#define lock_and_put_node_property_bool(_result, _ba_index, _rank, _prop_index)\\");
		Out.NL();
		Out.push("{\\");
		Out.NL();
		Out.push("int _err_val;\\");
		Out.NL();
		Out.push("_err_val=MPI_Win_lock(MPI_LOCK_EXCLUSIVE,_rank, 0, win);\\");
		Out.NL();
		Out.push("if(_err_val != MPI_SUCCESS)\\");
		Out.NL();
		Out.push("fprintf(stderr,\"error occured for lock with val%d\",_err_val);\\");
		Out.NL();
		Out.push("MPI_Put(&_result, 1, MPI_C_BOOL, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_prop_index*sizeof(bool), 1, MPI_C_BOOL, win);\\");
		Out.NL();
		Out.push("MPI_Win_unlock(_rank, win);\\");
		Out.NL();
		Out.push("}");
		Out.NL();
		Out.NL();

		Out.push("#define get_node_property_bool(_result, _ba_index, _rank, _prop_index)\\");
		Out.NL();
		Out.push("{\\");
		Out.NL();
		Out.push("MPI_Get(&_result, 1, MPI_C_BOOL, _rank, base_address[G.get_num_processes()*_ba_index+_rankj + _prop_index*sizeof(bool), 1, MPI_C_BOOL, win);\\");
		Out.NL();
		Out.push("}");
		Out.NL();
		Out.NL();

		Out.push("#define put_node_property_bool(_result, _ba_index, _rank, _prop_index)\\");
		Out.NL();
		Out.push("{\\");
		Out.NL();
		Out.push("MPI_Put(&_result, 1, MPI_C_BOOL, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_prop_index*sizeof(bool), 1, MPI_C_BOOL, win);\\");
		Out.NL();
		Out.push("}");
		Out.NL();
		Out.NL();

		Out.push("#define lock_and_put_node_property_float(_result, _ba_index, _rank, _prop_index)\\");
		Out.NL();
		Out.push("{\\");
		Out.NL();
		Out.push("int _err_val;\\");
		Out.NL();
		Out.push("_err_val=MPI_Win_lock(MPI_LOCK_EXCLUSIVE,_rank, 0, win);\\");
		Out.NL();
		Out.push("if(_err_val != MPI_SUCCESS)\\");
		Out.NL();
		Out.push("fprintf(stderr,\"error occured for lock with val%d\",_err_val);\\");
		Out.NL();
		Out.push("MPI_Put(&_result, 1, MPI_FLOAT, _rank, base_address[G.get_num_processes()*_ba_index +_rank] + _prop_index*sizeof(float), 1, MPI_FLOAT, win);\\");
		Out.NL();
		Out.push("MPI_Win_unlock(_rank, win);\\");
		Out.NL();
		Out.push("}");
		Out.NL();
		Out.NL();


		Out.push("#define lock_and_get_node_property_float(_result, _ba_index, _rank, _prop_index)\\");
		Out.NL();
		Out.push("{\\");
		Out.NL();
		Out.push("int _err_val;\\");
		Out.NL();
		Out.push("_err_val=MPI_Win_lock(MPI_LOCK_EXCLUSIVE,_rank, 0, win);\\");
		Out.NL();
		Out.push("if(_err_val != MPI_SUCCESS)\\");
		Out.NL();
		Out.push("fprintf(stderr,\"error occured for lock with val%d\",_err_val);\\");
		Out.NL();
		Out.push("MPI_Get(&_result, 1, MPI_FLOAT, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_prop_index*sizeof(float), 1, MPI_FLOAT, win);\\");
		Out.NL();
		Out.push("MPI_Win_unlock(_rank, win);\\");
		Out.NL();
		Out.push("}");
		Out.NL();
		Out.NL();

		Out.push("#define lock_and_get_node_property_double(_result, _ba_index, _rank, _prop_index)\\");
		Out.NL();
		Out.push("{\\");
		Out.NL();
		Out.push("int _err_val;\\");
		Out.NL();
		Out.push("_err_val=MPI_Win_lock(MPI_LOCK_EXCLUSIVE,_rank, 0, win);\\");
		Out.NL();
		Out.push("if(_err_val != MPI_SUCCESS)\\");
		Out.NL();
		Out.push("fprintf(stderr,\"error occured for lock with val%d\",_err_val);\\");
		Out.NL();
		Out.push("MPI_Get(&_result, 1, MPI_DOUBLE, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_prop_index*sizeof(double), 1, MPI_DOUBLE, win);\\");
		Out.NL();
		Out.push("MPI_Win_unlock(_rank, win);\\");
		Out.NL();
		Out.push("}");
		Out.NL();
		Out.NL();

		Out.push("#define lock_and_get_edge_property_double(_result, _ba_index, _rank, _prop_index)\\");
		Out.NL();
		Out.push("{\\");
		Out.NL();
		Out.push("int _err_val;\\");
		Out.NL();
		Out.push("_err_val=MPI_Win_lock(MPI_LOCK_EXCLUSIVE,_rank, 0, win);\\");
		Out.NL();
		Out.push("if(_err_val != MPI_SUCCESS)\\");
		Out.NL();
		Out.push("fprintf(stderr,\"error occured for lock with val%d\",_err_val);\\");
		Out.NL();
		Out.push("MPI_Get(&_result, 1, MPI_DOUBLE, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_prop_index*sizeof(double), 1, MPI_DOUBLE, win);\\");
		Out.NL();
		Out.push("MPI_Win_unlock(_rank, win);\\");
		Out.NL();
		Out.push("}");
		Out.NL();
		Out.NL();

		Out.push("#define lock_and_put_node_property_double(_result, _ba_index, _rank, _prop_index)\\");
		Out.NL();
		Out.push("{\\");
		Out.NL();
		Out.push("int _err_val;\\");
		Out.NL();
		Out.push("_err_val=MPI_Win_lock(MPI_LOCK_EXCLUSIVE,_rank, 0, win);\\");
		Out.NL();
		Out.push("if(_err_val != MPI_SUCCESS)\\");
		Out.NL();
		Out.push("fprintf(stderr,\"error occured for lock with val%d\",_err_val);\\");
		Out.NL();
		Out.push("MPI_Put(&_result, 1, MPI_DOUBLE, _rank, base_address[G.get_num_processes()*_ba_index +_rank] + _prop_index*sizeof(double), 1, MPI_DOUBLE, win);\\");
		Out.NL();
		Out.push("MPI_Win_unlock(_rank, win);\\");
		Out.NL();
		Out.push("}");
		Out.NL();
		Out.NL();

		Out.push("#define get_edge_property_double(_result, _ba_index, _rank, _prop_index)\\");
		Out.NL();
		Out.push("{\\");
		Out.NL();
		Out.push("MPI_Get(&_result, 1, MPI_DOUBLE, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_prop_index*sizeof(double), 1, MPI_DOUBLE, win);\\");
		Out.NL();
		Out.push("}");
		Out.NL();
		Out.NL();

		Out.push("#define get_node_property_double(_result, _ba_index, _rank, _prop_index)\\");
		Out.NL();
		Out.push("{\\");
		Out.NL();
		Out.push("MPI_Get(&_result, 1, MPI_DOUBLE, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_prop_index*sizeof(double), 1, MPI_DOUBLE, win);\\");
		Out.NL();
		Out.push("}");
		Out.NL();
		Out.NL();


		Out.push("#define put_node_property_double(_result, _ba_index, _rank, _prop_index)\\");
		Out.NL();
		Out.push("{\\");
		Out.NL();
		Out.push("MPI_Put(&_result, 1, MPI_DOUBLE, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_prop_index*sizeof(double), 1, MPI_DOUBLE, win);\\");
		Out.NL();
		Out.push("}");
		Out.NL();
		Out.NL();


		Out.push("#define put_edge_property_double(_result, _ba_index, _rank, _prop_index)\\");
		Out.NL();
		Out.push("{\\");
		Out.NL();
		Out.push("MPI_Put(&_result, 1, MPI_DOUBLE, _rank, base_address[G.get_num_processes()*_ba_index+_rank]+_prop_index*sizeof(double), 1, MPI_DOUBLE, win);\\");
		Out.NL();
		Out.push("}");
		Out.NL();
		Out.NL();


		Out.push("#define create_win_int(_base_address, _size, _win)\\");
		Out.NL();
		Out.push("{\\");
		Out.NL();
		Out.push("MPI_Win_create(_base_address, _size * sizeof(int), sizeof(int), MPI_INFO_NULL, MPI_COMM_WORLD, &_win);\\");
		Out.NL();
		Out.push("}");
		Out.NL();
		Out.NL();


		Out.push("#define create_win_bool(_base_address, _size, _win)\\");
		Out.NL();
		Out.push("{\\");
		Out.NL();
		Out.push("MPI_Win_create(_base_address, _size * sizeof(bool), sizeof(bool), MPI_INFO_NULL, MPI_COMM_WORLD, &_win);\\");
		Out.NL();
		Out.push("}");
		Out.NL();
		Out.NL();



		Out.push("#define create_win_double(_base_address, _size, _win)\\");
		Out.NL();
		Out.push("{\\");
		Out.NL();
		Out.push("MPI_Win_create(_base_address, _size * sizeof(double), sizeof(double), MPI_INFO_NULL, MPI_COMM_WORLD, &_win);\\");
		Out.NL();
		Out.push("}");
		Out.NL();
		Out.NL();


	}

	if (proc->is_local()) Out.push("static ");

	// return type
	Out.push_spc(get_type_string(proc->get_return_type()));
	Out.push(proc->get_procname()->get_genname());
	Out.push('(');

	int max_arg_per_line = 2;
	int arg_curr = 0;
	int remain_args = proc->get_in_args().size() + proc->get_out_args().size();
	{
		std::list<ast_argdecl*>& lst = proc->get_in_args();
		std::list<ast_argdecl*>::iterator i;
		for (i = lst.begin(); i != lst.end(); i++) {
			remain_args--;
			arg_curr++;

			ast_typedecl* T = (*i)->get_type();
			Out.push(get_type_string(T));
			if (T->is_primitive() || T->is_property())
				Out.push(" ");
			else
				Out.push("& ");

			assert((*i)->get_idlist()->get_length() == 1);
			Out.push((*i)->get_idlist()->get_item(0)->get_genname());
			if (remain_args > 0) {
				Out.push(", ");
			}

			if ((arg_curr == max_arg_per_line) && (remain_args > 0)) {
				Out.NL();
				arg_curr = 0;
			}
		}
	}
	{
		std::list<ast_argdecl*>& lst = proc->get_out_args();
		std::list<ast_argdecl*>::iterator i;
		for (i = lst.begin(); i != lst.end(); i++) {
			remain_args--;
			arg_curr++;

			Out.push(get_type_string((*i)->get_type()));
			ast_typedecl* T = (*i)->get_type();
			if (!T->is_property()) Out.push_spc("& ");

			Out.push((*i)->get_idlist()->get_item(0)->get_genname());

			if (remain_args > 0) {
				Out.push(", ");
			}
			if ((arg_curr == max_arg_per_line) && (remain_args > 0)) {
				Out.NL();
				arg_curr = 0;
			}
		}
	}
	// here macros need to be defined.
	Out.push(')');
	if (!is_body_file) { 
		Out.push(';');
		Out.NL();
		// Following is commented because we have moved these declartions
		// to gm_graph.h from the header of the application.
		Out.push("extern MPI_Win win;");
		Out.NL();
		Out.push("extern MPI_Aint *base_address;");
		Out.NL();
		Out.push("extern int base_address_index;");
	}
	Out.NL();
	return;
}

void gm_cpp_gen::generate_idlist(ast_idlist* idl) {
	int z = idl->get_length();
	for (int i = 0; i < z; i++) {
		ast_id* id = idl->get_item(i);
		generate_lhs_id(id);
		if (i < z - 1) Body.push_spc(',');
	}
}

void gm_cpp_gen::generate_idlist_primitive(ast_idlist* idList) {
	int length = idList->get_length();
	for (int i = 0; i < length; i++) {
    //insert to label for id here
		ast_id* id = idList->get_item(i);
    if(MPI_GEN.is_in_parallel_loop())
    {
		  id->add_info(LABEL_DECLARED_IN_PAR, new ast_extra_info(true));
    }
    else
    {
		  id->add_info(LABEL_DECLARED_IN_PAR, new ast_extra_info(false));
    }
    generate_lhs_id(id);
		generate_lhs_default(id->getTypeSummary());
		if (i < length - 1) Body.push_spc(',');
	}
}

void gm_cpp_gen::generate_lhs_default(int type) {
	switch (type) {
	case GMTYPE_BYTE:
	case GMTYPE_SHORT:
	case GMTYPE_INT:
	case GMTYPE_LONG:
		Body.push_spc(" = 0");
		break;
	case GMTYPE_FLOAT:
	case GMTYPE_DOUBLE:
		Body.push_spc(" = 0.0");
		break;
	case GMTYPE_BOOL:
		Body.push_spc(" = false");
		break;
	default:
		assert(false);
		return;
	}
}

void gm_cpp_gen::generate_lhs_id(ast_id* id) {

	Body.push(id->get_genname());
}
void gm_cpp_gen::generate_rhs_id(ast_id* id) {

	if (id->getTypeInfo()->is_edge_compatible()) {
		// reverse edge
		if (id->find_info_bool(CPPBE_INFO_IS_REVERSE_EDGE)) {
			Body.push(get_lib()->fw_edge_index(id));
		}
		//else if (id->getTypeInfo()->is_edge_iterator()) {
		// original edge index if semi-sorted
		//sprintf(temp, "%s.%s(%s)",
		//id->getTypeInfo()->get_target_graph_id()->get_genname(),
		//GET_ORG_IDX, 
		//id->get_genname()
		//);
		//Body.push(temp);
		//}
		else
		{
			if(MPI_GEN.is_present_in_local_access_nodes(id->get_genname()))
			{
        if(!MPI_GEN.is_assign_lhs_scalar()){
          Body.push("G.get_global_node_num( ");
          Body.push(id->get_genname());
          Body.push(")");
          MPI_GEN.reset_lhs_local_node();
          MPI_GEN.reset_lhs_local_edge();
        } else {
          Body.push(id->get_genname());
          MPI_GEN.set_lhs_local_node();
          MPI_GEN.reset_lhs_local_edge();
          
        }
			}
      else if(MPI_GEN.is_present_in_local_access_edges(id->get_genname())){
        if(!MPI_GEN.is_assign_lhs_scalar()){
          Body.push("G.get_global_edge_num( ");
          Body.push(id->get_genname());
          Body.push(")");
          MPI_GEN.reset_lhs_local_node();
          MPI_GEN.reset_lhs_local_edge();
        } else{
          Body.push(id->get_genname());
          MPI_GEN.reset_lhs_local_node();
          MPI_GEN.set_lhs_local_edge();
        }
      }
			else{
				generate_lhs_id(id);
        MPI_GEN.reset_lhs_local_node();
        MPI_GEN.reset_lhs_local_edge();
      }
		}
	}
	else {
			if(MPI_GEN.is_present_in_local_access_nodes(id->get_genname()))
			{
        if(!MPI_GEN.is_assign_lhs_scalar()){
          Body.push("G.get_global_node_num( ");
          Body.push(id->get_genname());
          Body.push(")");
          MPI_GEN.reset_lhs_local_node();
          MPI_GEN.reset_lhs_local_edge();
        } else {
          Body.push(id->get_genname());
          MPI_GEN.set_lhs_local_node();
          MPI_GEN.reset_lhs_local_edge();
          
        }
			}
      else if(MPI_GEN.is_present_in_local_access_edges(id->get_genname())){
        if(!MPI_GEN.is_assign_lhs_scalar()){
          Body.push("G.get_global_edge_num( ");
          Body.push(id->get_genname());
          Body.push(")");
          MPI_GEN.reset_lhs_local_node();
          MPI_GEN.reset_lhs_local_edge();
        } else{
          Body.push(id->get_genname());
          MPI_GEN.reset_lhs_local_node();
          MPI_GEN.set_lhs_local_edge();
        }
      }
			else{
				generate_lhs_id(id);
        MPI_GEN.reset_lhs_local_node();
        MPI_GEN.reset_lhs_local_edge();
      }
	}
}

void gm_cpp_gen::get_tempvar_name_and_type(const char **tmp_type,
		const char **tmp_name, ast_field *f) {
	//Get the type;
	ast_typedecl *tmp_type_decl = f->get_second()->getTargetTypeInfo();
	*tmp_type = get_type_string(tmp_type_decl);
	*tmp_name = f->get_second()->get_genname();
	return;
}

void gm_cpp_gen::generate_lhs_field(ast_field* f) {
	//Declare the temporary variable
	bool non_local_access = false;
	if(f->getTypeInfo()->is_node_property())
	{
		if(MPI_GEN.is_present_in_local_access_nodes(get_lib()->node_index(f->get_first()))) {
			Body.push(f->get_second()->get_genname());
			Body.push('[');
			Body.push(get_lib()->node_index(f->get_first()));
			Body.push(']');
		} else {
			non_local_access = true;
		}

	}
	else if(f->getTypeInfo()->is_edge_property())
	{

		//iterate and check if alias_name is present in local_access_edge
		//if so, emit
		//Body.push(f->get_second()->get_genname());
		//Body.push('[');


		const char * index_name;

		if (f->is_rarrow()) { // [XXX] this feature is still not finished, should be re-visited
			const char* alias_name = f->get_first()->getSymInfo()->find_info_string(CPPBE_INFO_NEIGHBOR_ITERATOR);
			assert(alias_name != NULL);
			assert(strlen(alias_name) > 0);
			index_name = alias_name;
		}
		//check if the edge is a back-edge
		else if (f->get_first()->find_info_bool(CPPBE_INFO_IS_REVERSE_EDGE)) {
			index_name = get_lib()->fw_edge_index(f->get_first());
		}
		else {
			//			original edge index if semi-sorted
			index_name = get_lib()->edge_index(f->get_first());
		}

		if(MPI_GEN.is_present_in_local_access_edges(index_name)) {
			Body.push(f->get_second()->get_genname());
			Body.push('[');
			Body.push(index_name);
			Body.push(']');
		} else {
			non_local_access = true;
		}
	}
	if(non_local_access)
	{
		const char *tmp_type = NULL;
		const char *tmp_name = NULL;
		get_tempvar_name_and_type(&tmp_type, &tmp_name, f);

		//	tmpvar_count++;
		//	sprintf(tempvar_countstr, "%d", tmpvar_count);
		//	Body.push(tmp_type);
		//	Body.push(" ");

		//Body.push(tmp_name);
		//Body.push("_tmp");
		//Body.push(tempvar_countstr);
		//Body.push(";");
		//Body.NL();

		MPI_GEN.tmpvar_count++;
		sprintf(MPI_GEN.tempvar_countstr, "%d", MPI_GEN.tmpvar_count);



		Body.push(tmp_type);
		Body.push(" ");

		Body.push(tmp_name);
		Body.push("_tmp");
		Body.push(MPI_GEN.tempvar_countstr);
		Body.push(";");
		Body.NL();



		Body.push(tmp_name);
		Body.push("_tmp");
		Body.push(MPI_GEN.tempvar_countstr);
	}
	/*Body.push('[');
	if (f->getTypeInfo()->is_node_property()) {
		Body.push(get_lib()->node_index(f->get_first()));
	} else if (f->getTypeInfo()->is_edge_property()) {

		if (f->is_rarrow()) { // [XXX] this feature is still not finished, should be re-visited
			const char* alias_name = f->get_first()->getSymInfo()->find_info_string(CPPBE_INFO_NEIGHBOR_ITERATOR);
			assert(alias_name != NULL);
			assert(strlen(alias_name) > 0);
			Body.push(alias_name);
		}
		// check if the edge is a back-edge
		else if (f->get_first()->find_info_bool(CPPBE_INFO_IS_REVERSE_EDGE)) {
			Body.push(get_lib()->fw_edge_index(f->get_first()));
		}
		else {
			// original edge index if semi-sorted
			//sprintf(temp, "%s.%s(%s)",
			//f->get_first()->getTypeInfo()->get_target_graph_id()->get_genname(),
			//GET_ORG_IDX, 
			//f->get_first()->get_genname()
			//);
			//Body.push(temp);
			Body.push(get_lib()->edge_index(f->get_first()));
		}
	}
	else {
		assert(false);
	}
	Body.push(']');*/
	return;
}

void gm_cpp_gen::generate_rhs_field(ast_field* f) {


	bool non_local_access = false;
	if(f->getTypeInfo()->is_node_property())
	{
		//to check if it is a local access
		if(MPI_GEN.is_present_in_local_access_nodes(get_lib()->node_index(f->get_first()))) {
			Body.push(f->get_second()->get_genname());
			Body.push('[');
			Body.push(get_lib()->node_index(f->get_first()));
			Body.push(']');
		} else {
			non_local_access = true;
#ifdef __USE_BSP_OPT__
      //TODO: The condtions coded for setting inside bsp is not complete
  if(!MPI_GEN.is_inside_red()){
    MPI_GEN.reset_bsp_canonical();
    MPI_GEN.reset_bsp_nested_1();
    MPI_GEN.reset_bsp_nested_2();
    Body.push("/*reset inside rhs field*/");
  }
#endif
		}

	}
	else if(f->getTypeInfo()->is_edge_property())
	{

		//iterate and check if alias_name is present in local_access_edge
		//if so, emit
		//Body.push(f->get_second()->get_genname());
		//Body.push('[');


		const char * index_name;

		if (f->is_rarrow()) { // [XXX] this feature is still not finished, should be re-visited
			const char* alias_name = f->get_first()->getSymInfo()->find_info_string(CPPBE_INFO_NEIGHBOR_ITERATOR);
			assert(alias_name != NULL);
			assert(strlen(alias_name) > 0);
			index_name = alias_name;
		}
		//check if the edge is a back-edge
		else if (f->get_first()->find_info_bool(CPPBE_INFO_IS_REVERSE_EDGE)) {
			index_name = get_lib()->fw_edge_index(f->get_first());
		}
		else {
			//			original edge index if semi-sorted
			index_name = get_lib()->edge_index(f->get_first());
		}

		if(MPI_GEN.is_present_in_local_access_edges(index_name)) {
			Body.push(f->get_second()->get_genname());
			Body.push('[');
			Body.push(index_name);
			Body.push(']');
		} else {
			non_local_access = true;
#ifdef __USE_BSP_OPT__
  if(!MPI_GEN.is_inside_red()){
    MPI_GEN.reset_bsp_canonical();
    MPI_GEN.reset_bsp_nested_1();
    MPI_GEN.reset_bsp_nested_2();
  }
#endif
		}
	}

	//Body.push("//check");
	//Body.push(f->get_second()->get_genname());
	//Body.NL();

	//mpi
	// generate_lhs_field(f);
	// For mpi implementation we need to handle lhs and rhs in different
	// ways. So the body of generate_lhs_field is inlined here.
	// mpi
	if (non_local_access) {
		MPI_GEN.tmpvar_count++;
		char *tmpcountstr = new char[32];
		sprintf(tmpcountstr, "%d", MPI_GEN.tmpvar_count);
		gm_mpi_get_info *GET_INFO = new gm_mpi_get_info(f, tmpcountstr);
		MPI_GEN.mpi_get_details.push_back(GET_INFO);

		Body.push(f->get_second()->get_genname());
		Body.push("_tmp");
		Body.push(tmpcountstr);
#ifdef __USE_BSP_OPT__
    if(MPI_GEN.is_bsp_canonical())
      Body.push("/* cp1: true */");
    else
      Body.push("/* cp1: false */");
#endif
	}
	/*Body.push('[');
	if (f->getTypeInfo()->is_node_property()) {
		Body.push(get_lib()->node_index(f->get_first()));
	} else if (f->getTypeInfo()->is_edge_property()) {

		if (f->is_rarrow()) { // [XXX] this feature is still not finished, should be re-visited
			const char* alias_name = f->get_first()->getSymInfo()->find_info_string(CPPBE_INFO_NEIGHBOR_ITERATOR);
			assert(alias_name != NULL);
			assert(strlen(alias_name) > 0);
			Body.push(alias_name);
		}
		// check if the edge is a back-edge
		else if (f->get_first()->find_info_bool(CPPBE_INFO_IS_REVERSE_EDGE)) {
			Body.push(get_lib()->fw_edge_index(f->get_first()));
		}
		else {
			// original edge index if semi-sorted
			//sprintf(temp, "%s.%s(%s)",
			//f->get_first()->getTypeInfo()->get_target_graph_id()->get_genname(),
			//GET_ORG_IDX,
			//f->get_first()->get_genname()
			//);
			//Body.push(temp);
			Body.push(get_lib()->edge_index(f->get_first()));
		}
	}
	else {
		assert(false);
	}
	Body.push(']');*/
	return;

}

const char* gm_cpp_gen::get_type_string(int type_id) {

	if (gm_is_prim_type(type_id)) {
		switch (type_id) {
		case GMTYPE_BYTE:
			return "int8_t";
		case GMTYPE_SHORT:
			return "int16_t";
		case GMTYPE_INT:
			return "int32_t";
		case GMTYPE_LONG:
			return "int64_t";
		case GMTYPE_FLOAT:
			return "float";
		case GMTYPE_DOUBLE:
			return "double";
		case GMTYPE_BOOL:
			return "bool";
		default:
			assert(false);
			return "??";
		}
	} else {
		return get_lib()->get_type_string(type_id);
	}
}

const char* gm_cpp_gen::get_type_string(ast_typedecl* t) {
	if ((t == NULL) || (t->is_void())) {
		return "void";
	}

	if (t->is_primitive()) {
		return get_type_string(t->get_typeid());
	} else if (t->is_property()) {
		ast_typedecl* t2 = t->get_target_type();
		assert(t2 != NULL);
		if (t2->is_primitive()) {
			switch (t2->get_typeid()) {
			case GMTYPE_BYTE:
				return "char_t*";
			case GMTYPE_SHORT:
				return "int16_t*";
			case GMTYPE_INT:
				return "int32_t*";
			case GMTYPE_LONG:
				return "int64_t*";
			case GMTYPE_FLOAT:
				return "float*";
			case GMTYPE_DOUBLE:
				return "double*";
			case GMTYPE_BOOL:
				return "bool*";
			default:
				assert(false);
				break;
			}
		} else if (t2->is_nodeedge()) {
			char temp[128];
			sprintf(temp, "%s*", get_lib()->get_type_string(t2));
			return gm_strdup(temp);
		} else if (t2->is_collection()) {
			char temp[128];
			sprintf(temp, "%s<%s>&", PROP_OF_COL, get_lib()->get_type_string(t2));
			return gm_strdup(temp);
		} else {
			assert(false);
		}
	} else if (t->is_map()) {
		char temp[256];
		ast_maptypedecl* mapType = (ast_maptypedecl*) t;
		const char* keyType = get_type_string(mapType->get_key_type());
		const char* valueType = get_type_string(mapType->get_value_type());
		sprintf(temp, "gm_map<%s, %s>", keyType, valueType);
		return gm_strdup(temp);
	} else
		return get_lib()->get_type_string(t);

	return "ERROR";
}

extern bool gm_cpp_should_be_dynamic_scheduling(ast_foreach* f);
#ifdef __USE_BSP_OPT__
void gm_cpp_gen::emit_post_opt_body(GM_REDUCE_T r_type){
  if(MPI_GEN.is_bsp_canonical()) {
    //TODO: this should not be called here. If there  are multiple bsp canonical loops, it is not dealt.
    generate_opt_inits();
    Body.push(MPI_GEN.prop_types[0]);
    Body.push(" *new_prop = new ");
    Body.push(MPI_GEN.prop_types[0]);
    
    Body.pushln("[G.get_num_of_local_reverse_edges()];");
    Body.pushln("int32_t *v_to_update = new int32_t[G.get_num_of_local_reverse_edges()];");
    Body.pushln("int num_v_to_update;");
    //fprintf(stdout, "rank : %d cp: %d.1\n", _my_rank, 3);
     Body.pushln("for(int proc_id = 0; proc_id < _num_process; proc_id++) {");
      //get num_v_to_update
      Body.pushln(" MPI_Win_lock(MPI_LOCK_SHARED, proc_id, 0, _meta_num_v_win[_my_rank]);");
      Body.pushln(" MPI_Get(&num_v_to_update, 1, MPI_INT, proc_id, 0, 1, MPI_INT, _meta_num_v_win[_my_rank]);");
       Body.pushln("MPI_Win_unlock(proc_id, _meta_num_v_win[_my_rank]);");

      //get new_prop
      Body.pushln(" MPI_Win_lock(MPI_LOCK_SHARED, proc_id, 0, _meta_propwin[_my_rank]);");
      Body.push(" MPI_Get(new_prop, num_v_to_update, ");
      if(strcmp(MPI_GEN.prop_types[0],"int")==0){
        Body.push("MPI_INT");
      }else if(strcmp(MPI_GEN.prop_types[0],"double")==0){
        Body.push("MPI_DOUBLE");
      }//TODO: all types not done
      Body.pushln(", proc_id, 0, num_v_to_update, MPI_INT, _meta_propwin[_my_rank]);");
      Body.pushln(" MPI_Win_unlock(proc_id, _meta_propwin[_my_rank]);");

      //get v_to_update
      Body.pushln(" MPI_Win_lock(MPI_LOCK_SHARED, proc_id, 0, _meta_v_win[_my_rank]);");
      Body.pushln(" MPI_Get(v_to_update, num_v_to_update, MPI_INT, proc_id, 0, num_v_to_update, MPI_INT, _meta_v_win[_my_rank]);");
      Body.pushln(" MPI_Win_unlock(proc_id, _meta_v_win[_my_rank]);");

      Body.pushln(" for(int32_t i=0; i<num_v_to_update; i++) {");

        Body.pushln(" int v = v_to_update[i];");
        Body.push(MPI_GEN.prop_types[0]);
        Body.pushln(" prop = new_prop[i];");
        
        ast_field *f = MPI_GEN.updated_props[0];
        const char *name = f->get_second()->get_genname();
        switch(r_type){
        case GMREDUCE_PLUS:

          Body.push(name);
          Body.pushln(" [v] = ");
          Body.push(name);
          Body.pushln(" [v] + prop;");
          break;
        default:
          Body.push(" if(");
          Body.push(name);
          Body.pushln("[v] > prop) {");
          Body.push(name);
          Body.pushln(" [v] = prop;");
          Body.pushln(" G_updated_nxt[v] = true;");
          Body.pushln(" }");
        }
      Body.pushln(" }");

    Body.pushln(" }");
   Body.pushln(" MPI_Barrier(MPI_COMM_WORLD);");
	Body.pushln("for(int proc_id=0; proc_id<_num_process; proc_id++) {");
		Body.pushln("prop_buffer[proc_id].reset();");
	Body.pushln("}");


  }
}
#endif

void gm_cpp_gen::generate_sent_foreach(ast_foreach* f) {

#ifdef __USE_VAR_REUSE_OPT__
  if (MPI_GEN.is_in_parallel_loop()) {
    MPI_GEN.inc_nesting_level();
  }
#endif

	int ptr;
	bool need_init_before = get_lib()->need_up_initializer(f);

	if (need_init_before) {
		assert(f->get_parent()->get_nodetype() == AST_SENTBLOCK);
		get_lib()->generate_up_initializer(f, Body);
	}

	if (f->is_parallel()) {
		Body.NL();
		prepare_parallel_for(gm_cpp_should_be_dynamic_scheduling(f));
    MPI_GEN.set_in_parallel_loop();
	}

	get_lib()->generate_foreach_header(f, Body);

	if (get_lib()->need_down_initializer(f)) {
		Body.pushln("{");
		get_lib()->generate_down_initializer(f, Body);

		if (f->get_body()->get_nodetype() != AST_SENTBLOCK) {
      if(f->is_parallel()){

#ifdef __USE_BSP_OPT__
        Body.reset_ready_to_push(); 
        MPI_GEN.set_in_bsp_prepass();
			  generate_sent(f->get_body());
        MPI_GEN.reset_in_bsp_prepass();
        Body.set_ready_to_push();
#endif
        generate_sent(f->get_body());
      }
      else
			  generate_sent(f->get_body());
		} else {
			// '{' '} already handled
			generate_sent_block((ast_sentblock*) f->get_body(), false);
		}

		int type = f->get_iter_type();
		if (gm_is_simple_collection_iteration(type) || gm_is_collection_of_collection_iteration(type))
		{
			Body.pushln("if(G.get_rank() == MASTER)");
			Body.pushln("{");
			Body.push(f->get_source()->get_genname());
			Body.push("_has_next = ");
			Body.push(f->find_info_string(CPPBE_INFO_COLLECTION_ITERATOR));
			Body.push(".has_next();");
			Body.NL();
			Body.pushln("}");
			Body.push("MPI_Bcast(&");
			Body.push(f->get_source()->get_genname());
			Body.push("_has_next, 1, MPI_INT, MASTER, MPI_COMM_WORLD);");
			Body.NL();

			//Body.pushln("}");
		}
		Body.pushln("}");

	} else if (f->get_body()->get_nodetype() == AST_SENTBLOCK) {
		generate_sent(f->get_body());
	} else {
		Body.push_indent();
		generate_sent(f->get_body());
		Body.pop_indent();
		Body.NL();
	}
	if(f->is_parallel())
	{
    MPI_GEN.reset_in_parallel_loop();
    Body.push("MPI_Barrier(MPI_COMM_WORLD);");
    Body.NL();
#ifdef __USE_BSP_OPT__
    emit_post_opt_body(MPI_GEN.r_type);
#endif
	}

	// FIXME: code for poping the parallel indices need to be added.

#ifdef __USE_VAR_REUSE_OPT__
  if (MPI_GEN.is_in_parallel_loop()) {
    MPI_GEN.clear_vars_in_curr_scope();
    MPI_GEN.dec_nesting_level();
  }
#endif
#ifdef __USE_BSP_OPT__
  Body.push("/*BSP : ");
  if(MPI_GEN.is_bsp_canonical()){
    Body.push("true");
  }else{
    Body.push("false");
  }
  Body.push("*/");
#endif

}

void gm_cpp_gen::generate_sent_call(ast_call* c) {
	assert(c->is_builtin_call());
	generate_expr_builtin(c->get_builtin());
	Body.pushln(";");
}

// t: type (node_prop)
// id: field name
void gm_cpp_gen::declare_prop_def(ast_typedecl* t, ast_id * id) {
	ast_typedecl* t2 = t->get_target_type();
	assert(t2 != NULL);

	Body.push(" = ");
	switch (t2->getTypeSummary()) {
	case GMTYPE_INT:
		Body.push("new int[");
		//Body.push(ALLOCATE_INT);
		break;
	case GMTYPE_LONG:
		Body.push("new long[");
		Body.push(ALLOCATE_LONG);
		break;
	case GMTYPE_BOOL:
		Body.push("new bool[");
		//Body.push(ALLOCATE_BOOL);
		break;
	case GMTYPE_DOUBLE:
		Body.push("new double[");
		//Body.push(ALLOCATE_DOUBLE);
		break;
	case GMTYPE_FLOAT:
		Body.push("new float[");
		break;
	case GMTYPE_NODE:
		Body.push("new node_t[");
		//Body.push(ALLOCATE_NODE);
		break;
	case GMTYPE_EDGE:
		Body.push("new edge_t[");
		//Body.push(ALLOCATE_EDGE);
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

	//Body.push('(');
	if (t->is_node_property()) {
		Body.push("G.get_num_of_local_nodes()];");
		//Body.push(get_lib()->max_node_index(t->get_target_graph_id()));
	} else if (t->is_edge_property()) {
		Body.push("G.get_num_of_local_forward_edges()];");
		Body.push(get_lib()->max_edge_index(t->get_target_graph_id()));
	}
	//Body.push(',');
	//Body.push(THREAD_ID);
	//Body.pushln("());");
	Body.NL();
	Body.push("int ");
	Body.push(id->get_genname());
	Body.push("_index = base_address_index;");
	Body.NL();

	switch (t2->getTypeSummary()) {
	case GMTYPE_INT:
		Body.push("G.win_attach_int");
		//Body.push(ALLOCATE_INT);
		break;
	case GMTYPE_LONG:
		Body.push("G.win_attach_long");
		//Body.push(ALLOCATE_LONG);
		break;
	case GMTYPE_BOOL:
		Body.push("G.win_attach_bool");
		//Body.push(ALLOCATE_BOOL);
		break;
	case GMTYPE_DOUBLE:
		Body.push("G.win_attach_double");
		//Body.push(ALLOCATE_DOUBLE);
		break;
	case GMTYPE_FLOAT:
		Body.push("G.win_attach_float");
		break;
	case GMTYPE_NODE:
		Body.push("G.win_attach_int");
		//Body.push(ALLOCATE_NODE);
		break;
	case GMTYPE_EDGE:
		Body.push("G.win_attach_int");
		//Body.push(ALLOCATE_NODE);
		//Body.push(ALLOCATE_EDGE);
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

	if (t->is_node_property()) {
		Body.push("(");
		Body.push(id->get_genname());
		Body.push(", G.get_num_of_local_nodes());");
		//Body.push(get_lib()->max_node_index(t->get_target_graph_id()));
	} else if (t->is_edge_property()) {
		Body.push("(");
		Body.push(id->get_genname());
		Body.push(", G.get_num_of_local_forward_edges());");
		Body.push(get_lib()->max_edge_index(t->get_target_graph_id()));
	}
	Body.NL();
	/*
	 */

	// register to memory controller
}

void gm_cpp_gen::generate_sent_vardecl(ast_vardecl* v) {
	ast_typedecl* t = v->get_type();

	if (t->is_collection_of_collection()) {
		Body.push(get_type_string(t));
		ast_typedecl* targetType = t->get_target_type();
		Body.push("<");
		Body.push(get_type_string(t->getTargetTypeSummary()));
		Body.push("> ");
		ast_idlist* idl = v->get_idlist();
		assert(idl->get_length() == 1);
		generate_lhs_id(idl->get_item(0));
//e->add_info(LABEL_PAR_SCOPE, new ast_extra_info(true));
		get_lib()->add_collection_def(idl->get_item(0));
		return;
	}

	if (t->is_map()) {
		ast_maptypedecl* map = (ast_maptypedecl*) t;
		ast_idlist* idl = v->get_idlist();
		assert(idl->get_length() == 1);
		get_lib()->add_mpi_map_def(map, idl->get_item(0));
		return;
	}

	if (t->is_sequence_collection()) {
		//for sequence-list-vector optimization
		ast_id* id = v->get_idlist()->get_item(0);
		const char* type_string;
		if(get_lib()->has_optimized_type_name(id->getSymInfo())) {
			type_string = get_lib()->get_optimized_type_name(id->getSymInfo());
		} else {
			type_string = get_type_string(t);
		}
		Body.push_spc(type_string);
	} else {
		Body.push_spc(get_type_string(t));
	}

	if (t->is_property()) {

		//mpi
		// window attachments should be included here
		//mpi

		ast_idlist* idl = v->get_idlist();
		assert(idl->get_length() == 1);
		generate_lhs_id(idl->get_item(0));
		declare_prop_def(t, idl->get_item(0));
	} else if (t->is_collection()) {
		ast_idlist* idl = v->get_idlist();
		assert(idl->get_length() == 1);
		generate_lhs_id(idl->get_item(0));
		get_lib()->add_collection_def(idl->get_item(0));
	} else if (t->is_primitive()) {
		generate_idlist_primitive(v->get_idlist());
		Body.pushln(";");
	} else {
		generate_idlist(v->get_idlist());
		Body.pushln(";");
	}
}

const char* gm_cpp_gen::get_function_name_map_reduce_assign(int reduceType) {

	switch (reduceType) {
	case GMREDUCE_PLUS:
		return "changeValueAtomicAdd";
	default:
		assert(false);
		return "ERROR";
	}
}

void gm_cpp_gen::generate_sent_map_assign(ast_assign_mapentry* a) {
	ast_mapaccess* mapAccess = a->to_assign_mapentry()->get_lhs_mapaccess();
	ast_id* map = mapAccess->get_map_id();

	int current_indent = 0;
	int f_ptr = _Body.get_write_ptr(current_indent);
	MPI_GEN.mpi_get_details.clear();
	char buffer[256];
	if (a->is_under_parallel_execution()) {
		if (a->is_reduce_assign() && a->get_reduce_type() == GMREDUCE_PLUS) {
			sprintf(buffer, "%s.%s(", map->get_genname(), get_function_name_map_reduce_assign(a->get_reduce_type()));
		} else {
			sprintf(buffer, "%s.setValue_par(", map->get_genname());
		}
	} else {
		if (a->is_reduce_assign() && a->get_reduce_type() == GMREDUCE_PLUS) {
			//TODO do this without CAS overhead
			sprintf(buffer, "%s.%s(", map->get_genname(), get_function_name_map_reduce_assign(a->get_reduce_type()));
		} else {
			sprintf(buffer, "%s.setValue_seq(", map->get_genname());
		}
	}
	Body.push(buffer);

	ast_expr* key = mapAccess->get_key_expr();
	generate_expr(key);
	Body.push(", ");
	generate_expr(a->get_rhs());
	Body.pushln(");");
	if(MPI_GEN.mpi_get_details.size() != 0)
		MPI_GEN.generate_mpi_get_block(f_ptr, current_indent, Body);
}




void gm_cpp_gen::generate_sent_assign(ast_assign* a) {
	// Save the current write position for future use
	// when the rhs of assign sent have to be remotely accessed
	// using MPI_Get.
	int current_indent = 0;
	int f_ptr = _Body.get_write_ptr(current_indent);

	MPI_GEN.mpi_get_details.clear();
	if (a->is_target_scalar()) {
		ast_id* leftHandSide = a->get_lhs_scala();
		if (leftHandSide->is_instantly_assigned()) { //we have to add the variable declaration here
			_Body.push(get_lib()->get_type_string(leftHandSide->getTypeSummary()));
			if (a->is_reference()) {
				_Body.push("& ");
			} else {
				_Body.push(" ");
			}
		}
		generate_lhs_id(a->get_lhs_scala());

#ifdef __USE_VAR_REUSE_OPT__
    if (MPI_GEN.is_in_parallel_loop()) {
      if (MPI_GEN.check_in_vars_in_scope((a->get_lhs_scala())->get_genname())) {
        MPI_GEN.remove_var((a->get_lhs_scala())->get_genname());
      }
    }
#endif

    if(strcmp((a->get_lhs_scala())->get_genname(), "dist") == 0)
      printf("found dist\n");
	} else if (a->is_target_map_entry()) {
		generate_sent_map_assign(a->to_assign_mapentry());
		return;
	} else {

		generate_lhs_field(a->get_lhs_field());
	}

	_Body.push(" = ");

	// TODO: find out the exact type of rhs. Currently none in
	// a->get_rhs() matches.
/*
	if (a->get_rhs()->is_mapaccess()) {
		Body.push("//is mapaccess");
		Body.NL();
	}

	if (a->get_rhs()->is_biop()) {
		Body.push("//is biop");
		Body.NL();
	}

	if (a->get_rhs()->is_id()) {
		Body.push("//is an id");
		Body.NL();
		ast_id *rhs_id = a->get_rhs()->get_id();
		const char *rhs_name = rhs_id->get_genname();
		if(MPI_GEN.is_present_in_local_access_edges(rhs_name)) {
			Body.push("//is in local access edges");
			Body.NL();
			Body.push("G.get_global_edge_num(");
			Body.push(rhs_name); Body.push(");");
			Body.NL();
		} else {
			generate_expr(a->get_rhs());
		}
	} else {*/
  // the only statement inside which assign_lhs_scalar will be true is inside an assignment statement.
    if(a->is_target_scalar()) {
      MPI_GEN.set_assign_lhs_scalar();      
    }else{
      MPI_GEN.reset_assign_lhs_scalar();
    }
		generate_expr(a->get_rhs());
    if(MPI_GEN.is_lhs_local_node()){
      MPI_GEN.local_access_nodes.push_back((a->get_lhs_scala())->get_genname());
    }else if(MPI_GEN.is_lhs_local_edge()){
      MPI_GEN.local_access_edges.push_back((a->get_lhs_scala())->get_genname());
    }
    MPI_GEN.reset_lhs_local_node();
    MPI_GEN.reset_lhs_local_edge();
    // ouside assignment statement make the flag always false.
    MPI_GEN.reset_assign_lhs_scalar();
	//}

	_Body.pushln(" ;");



	if (!(a->is_target_scalar()) && !(a->is_target_map_entry())) {

		bool local_access_node = false;
		bool local_access_edge = false;

		ast_field* f = a->get_lhs_field();
		assert(f);
		const char * index_name;
		if(f->getTypeInfo()->is_node_property()){
			index_name = get_lib()->node_index(f->get_first());
			local_access_node = MPI_GEN.is_present_in_local_access_nodes(index_name);
		}
		else if(f->getTypeInfo()->is_edge_property())
		{
			if (f->is_rarrow()) { // [XXX] this feature is still not finished, should be re-visited
				const char* alias_name = f->get_first()->getSymInfo()->find_info_string(CPPBE_INFO_NEIGHBOR_ITERATOR);
				assert(alias_name != NULL);
				assert(strlen(alias_name) > 0);
				index_name = alias_name;
			}
			//check if the edge is a back-edge
			else if (f->get_first()->find_info_bool(CPPBE_INFO_IS_REVERSE_EDGE)) {
				index_name = get_lib()->fw_edge_index(f->get_first());
			}
			else {
				//			original edge index if semi-sorted
				index_name = get_lib()->edge_index(f->get_first());
			}
			local_access_edge = MPI_GEN.is_present_in_local_access_edges(index_name);
		}

		if(!(local_access_node||local_access_edge))
			MPI_GEN.generate_mpi_put_block(f, Body);

	}
	int cur_ind = 0;
	printf("before calling generate_mpi_get_block: file_ptr is %d\n", _Body.get_write_ptr(cur_ind));
	// If the assignment is on a node or edge property we have to
	// send it via MPI_put.
	if(MPI_GEN.mpi_get_details.size() != 0)
		MPI_GEN.generate_mpi_get_block(f_ptr, current_indent, Body);
}

void gm_cpp_gen::generate_sent_block(ast_sentblock* sb) {
	generate_sent_block(sb, true);
}

void gm_cpp_gen::generate_sent_block_enter(ast_sentblock* sb) {
	if (sb->find_info_bool(CPPBE_INFO_IS_PROC_ENTRY) && !FE.get_current_proc()->is_local()) {
		Body.pushln("//Initializations");

		sprintf(temp, "%s();", RT_INIT);
		Body.pushln(temp);

		//----------------------------------------------------
		// freeze graph instances
		//----------------------------------------------------
		ast_procdef* proc = FE.get_current_proc();
		gm_symtab* vars = proc->get_symtab_var();
		gm_symtab* fields = proc->get_symtab_field();
		//std::vector<gm_symtab_entry*>& E = vars-> get_entries();
		//for(int i=0;i<E.size();i++) {
		// gm_symtab_entry* e = E[i];
		std::set<gm_symtab_entry*>& E = vars->get_entries();
		std::set<gm_symtab_entry*>::iterator I;
		for (I = E.begin(); I != E.end(); I++) {
			gm_symtab_entry* e = *I;
			if (e->getType()->is_graph()) {
				sprintf(temp, "%s.%s();", e->getId()->get_genname(), FREEZE);
				Body.pushln(temp);

				// currrently every graph is an argument
				if (e->find_info_bool(CPPBE_INFO_USE_REVERSE_EDGE)) {
					sprintf(temp, "%s.%s();", e->getId()->get_genname(), MAKE_REVERSE);
					Body.pushln(temp);
				}
				if (e->find_info_bool(CPPBE_INFO_NEED_SEMI_SORT)) {
					bool has_edge_prop = false;
#if 0
					// Semi-sorting must be done before edge-property creation
					//std::vector<gm_symtab_entry*>& F = fields-> get_entries();
					//for(int j=0;j<F.size();j++) {
					// gm_symtab_entry* f = F[j];
					std::set<gm_symtab_entry*>& F = fields->get_entries();
					std::set<gm_symtab_entry*>::iterator J;
					for (J = F.begin(); J != F.end(); J++) {
						gm_symtab_entry* f = *J;
						if ((f->getType()->get_target_graph_sym() == e) && (f->getType()->is_edge_property())) has_edge_prop = true;
					}

					if (has_edge_prop) {
						Body.pushln("//[xxx] edge property must be created before semi-sorting");
						sprintf(temp, "assert(%s.%s());", e->getId()->get_genname(), IS_SEMI_SORTED);
						Body.pushln(temp);
					} else {
						sprintf(temp, "%s.%s();", e->getId()->get_genname(), SEMI_SORT);
						Body.pushln(temp);
					}
#else
					sprintf(temp, "%s.%s();", e->getId()->get_genname(), SEMI_SORT);
					Body.pushln(temp);
#endif
				}

				if (e->find_info_bool(CPPBE_INFO_NEED_FROM_INFO)) {
					sprintf(temp, "%s.%s();", e->getId()->get_genname(), PREPARE_FROM_INFO);
					Body.pushln(temp);
				}
			}
		}

		Body.NL();
#ifdef __USE_BSP_OPT__

	MPI_GEN.st_blk_cur_int = 0;
	MPI_GEN.st_blk_fp = Body.get_write_ptr(MPI_GEN.st_blk_cur_int);
  //generate_opt_inits();
#endif
		Body.NL();
		MPI_GEN.generate_win_attach_in_param_props(Body);
		Body.NL();
		MPI_GEN.generate_win_attach_out_param_props(Body);

	}


}

void gm_cpp_gen::generate_sent_block(ast_sentblock* sb, bool need_br) {
	if (is_target_omp()) {
		bool is_par_scope = sb->find_info_bool(LABEL_PAR_SCOPE);
		if (is_par_scope) {
			assert(is_under_parallel_sentblock() == false);
			set_under_parallel_sentblock(true);
			need_br = true;
			//Body.pushln("#pragma omp parallel");
		}
	}

	if (need_br) Body.pushln("{");

	// sentblock exit
	generate_sent_block_enter(sb);

	std::list<ast_sent*>& sents = sb->get_sents();
	std::list<ast_sent*>::iterator it;
	bool vardecl_started = false;
	bool other_started = false;
	for (it = sents.begin(); it != sents.end(); it++) {
		// insert newline after end of VARDECL
		ast_sent* s = *it;
		if (!vardecl_started) {
			if (s->get_nodetype() == AST_VARDECL) vardecl_started = true;
		} else {
			if (other_started == false) {
				if (s->get_nodetype() != AST_VARDECL) {
					Body.NL();
					other_started = true;
				}
			}
		}
		generate_sent(*it);
	}

	// sentblock exit
	generate_sent_block_exit(sb);

	if (need_br) Body.pushln("}");

	if (is_under_parallel_sentblock()) set_under_parallel_sentblock(false);

	return;
}

void gm_cpp_gen::generate_sent_block_exit(ast_sentblock* sb) {
	bool has_prop_decl = sb->find_info_bool(CPPBE_INFO_HAS_PROPDECL);
	bool is_proc_entry = sb->find_info_bool(CPPBE_INFO_IS_PROC_ENTRY);
	assert(sb->get_nodetype() == AST_SENTBLOCK);
	bool has_return_ahead = gm_check_if_end_with_return(sb);

	if (has_prop_decl && !has_return_ahead) {
		if (is_proc_entry) {
			Body.NL();
			sprintf(temp, "%s();", CLEANUP_PTR);
			Body.pushln(temp);
		} else {
			Body.NL();
			gm_symtab* tab = sb->get_symtab_field();
			//std::vector<gm_symtab_entry*>& entries = tab->get_entries();
			//std::vector<gm_symtab_entry*>::iterator I;
			std::set<gm_symtab_entry*>& entries = tab->get_entries();
			std::set<gm_symtab_entry*>::iterator I;
			for (I = entries.begin(); I != entries.end(); I++) {
				gm_symtab_entry *e = *I;
				sprintf(temp, "%s(%s,%s());", DEALLOCATE, e->getId()->get_genname(), THREAD_ID);
				Body.pushln(temp);
			}
		}
	}

}

void gm_cpp_gen::generate_sent_reduce_assign(ast_assign *a) {
#ifdef __USE_BSP_OPT__
  MPI_GEN.set_inside_red();
#endif
  if (a->is_argminmax_assign()) {
    generate_sent_reduce_argmin_assign(a);
#ifdef __USE_BSP_OPT__
  MPI_GEN.reset_inside_red();
#endif
    return;
  }

  GM_REDUCE_T r_type = (GM_REDUCE_T) a->get_reduce_type();
#ifdef __USE_BSP_OPT__
  MPI_GEN.r_type = r_type;
#endif
  const char* method_name = get_lib()->get_reduction_function_name(r_type);
  bool is_scalar = (a->get_lhs_type() == GMASSIGN_LHS_SCALA);

  ast_typedecl* lhs_target_type;
  if (a->get_lhs_type() == GMASSIGN_LHS_SCALA) {
    lhs_target_type = a->get_lhs_scala()->getTypeInfo();
  } else {
    lhs_target_type = a->get_lhs_field()->getTypeInfo()->get_target_type();
  }

  char templateParameter[32];
  if (r_type != GMREDUCE_OR && r_type != GMREDUCE_AND) {
    sprintf(templateParameter, "<%s>", get_type_string(lhs_target_type));
  } else {
    sprintf(templateParameter, "");
  }

  //mpi
  int current_indent = 0;
  int f_ptr = _Body.get_write_ptr(current_indent);

  MPI_GEN.mpi_get_details.clear();

#ifdef __USE_BSP_OPT__
  char *rhstemp = new char[100];
  rhstemp[0] = '\0'; 
#endif

  if(!is_scalar){
    ast_field *f = a->get_lhs_field();
    const char *tmp_type = NULL;
    const char *tmp_name = NULL;
    get_tempvar_name_and_type(&tmp_type, &tmp_name, f);

    MPI_GEN.tmpvar_count++;
    sprintf(MPI_GEN.tempvar_countstr, "%d", MPI_GEN.tmpvar_count);


    Body.push(tmp_type);
    Body.push(" ");

    Body.push(tmp_name);
    Body.push("_tmp");
    Body.push(MPI_GEN.tempvar_countstr);
    Body.push(";");
    Body.NL();

#ifdef __USE_BSP_OPT__
    strcat(rhstemp, tmp_name);
    strcat(rhstemp, "_tmp");
    strcat(rhstemp, MPI_GEN.tempvar_countstr);
#endif

    Body.push(tmp_name);
    Body.push("_tmp");
    Body.push(MPI_GEN.tempvar_countstr);
    Body.push(" = ");

  }

  // a boolean variable to check whether mpi reduction is required or not.
  bool require_mpi_red = false;
  if(is_scalar)
  {
    ast_id * lhs_id = a->get_lhs_scala();
    if(!(lhs_id->find_info_bool(LABEL_DECLARED_IN_PAR))) 
      // only if the scalar variable is declared in a serial region we need to do mpi_red
    {
      require_mpi_red = true;
    }
  }

  if(require_mpi_red)
  {

    //mpi
    Body.push(method_name);
    Body.push(templateParameter);
    Body.push("(&");
    if (is_scalar)
      generate_rhs_id(a->get_lhs_scala());
    Body.push(", ");
    generate_expr(a->get_rhs());
    Body.push(");\n");
  }
  else
  {
    if (is_scalar)
    {
      generate_rhs_id(a->get_lhs_scala());
      Body.push(" = ");
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

        #ifndef __USE_BSP_OPT__
          generate_rhs_field(a->get_lhs_field()); Body.push("+"); 
        #endif
          generate_expr(a->get_rhs()); Body.push(";");
          break;    
        case GMREDUCE_MULT:
        #ifndef __USE_BSP_OPT__
           generate_rhs_field(a->get_lhs_field()); Body.push("*"); 
        #endif
           generate_expr(a->get_rhs()); Body.push(";");
          break;    
        case GMREDUCE_AND:
        #ifndef __USE_BSP_OPT__
          generate_rhs_field(a->get_lhs_field());  Body.push("&"); 
        #endif
          Body.push("(");generate_expr(a->get_rhs()); Body.push(");");
          break;    
        case GMREDUCE_OR:
        #ifndef __USE_BSP_OPT__
           generate_rhs_field(a->get_lhs_field()); Body.push("|"); 
        #endif
           Body.push("(");generate_expr(a->get_rhs()); Body.push(");");
          break;    
        case GMREDUCE_MIN:
        #ifndef __USE_BSP_OPT__
          Body.push("std::min("); generate_rhs_field(a->get_lhs_field()); Body.push(",");
        #endif
          generate_expr(a->get_rhs()); 
        #ifndef __USE_BSP_OPT__
          Body.push(");");
        #endif
          break;    
        case GMREDUCE_MAX:
        #ifndef __USE_BSP_OPT__
          Body.push("std::max("); generate_rhs_field(a->get_lhs_field()); Body.push(",");
        #endif
          generate_expr(a->get_rhs()); 
        #ifndef __USE_BSP_OPT__
          Body.push(");");
        #endif
          break;    
        default:
          assert(false);
      }
    }
  }
  //mpi
  MPI_GEN.generate_mpi_get_block(f_ptr, current_indent, Body);
  //mpi
  if(!is_scalar){
    ast_field *f = a->get_lhs_field();
#ifdef __USE_BSP_OPT__
    if(!MPI_GEN.is_in_bsp_prepass()){
      if(MPI_GEN.is_bsp_canonical()){
        generate_opt_body(a, rhstemp);
      }else{
#endif
         MPI_GEN.generate_mpi_put_block(f ,Body);
#ifdef __USE_BSP_OPT__
      }
    }
#endif
  }
#ifdef __USE_BSP_OPT__
  MPI_GEN.reset_inside_red();
#endif
}

#ifdef __USE_BSP_OPT__
void gm_cpp_gen::generate_opt_body(ast_assign *a, const char * rhs_temp) {
      ast_field * f = a->get_lhs_field();
			const char * ind = get_lib()->node_index(f->get_first());
     
      MPI_GEN.updated_props.push_back(f); 
      Body.push("/*inserted*/");
      Body.push("int ");
      Body.push(ind);
      Body.push("_rank = G.get_rank_node(");
      Body.push(ind);
      Body.pushln(");");
      
      Body.push("int ");
      Body.push(ind);
      Body.push("_ln = G.get_local_node_num(");
      Body.push(ind);
      Body.pushln(");");
      
      Body.push("prop_buffer[");
      Body.push(ind);
      Body.push("_rank].set_prop(");
      Body.push(ind);
      Body.push("_ln, ");
      Body.push(rhs_temp);
      Body.pushln(");");
}
#endif

void gm_cpp_gen::generate_sent_reduce_argmin_assign(ast_assign *a) {
	assert(a->is_argminmax_assign());
	MPI_GEN.mpi_get_details.clear();


	//-----------------------------------------------
	// <LHS; L1,...> min= <RHS; R1,...>;
	//
	// {
	//    RHS_temp = RHS;
	//    if (LHS > RHS_temp) {
	//        <type> L1_temp = R1;
	//        ...
	//        LOCK(LHS) // if LHS is scalar,
	//                  // if LHS is field, lock the node
	//            if (LHS > RHS_temp) {
	//               LHS = RHS_temp;
	//               L1 = L1_temp;  // no need to lock for L1.
	//               ...
	//            }
	//        UNLOCK(LHS)
	//    }
	// }
	//-----------------------------------------------
	Body.pushln("{ ");

	int curr_indent = 0;
	int fp = Body.get_write_ptr(curr_indent);

	const char* rhs_temp;
	int t;
	if (a->is_target_scalar()) {
		t = a->get_lhs_scala()->getTypeSummary();
		rhs_temp = (const char*) FE.voca_temp_name_and_add(a->get_lhs_scala()->get_genname(), "_new");
	} else {
		t = a->get_lhs_field()->get_second()->getTargetTypeSummary();
		rhs_temp = (const char*) FE.voca_temp_name_and_add(a->get_lhs_field()->get_second()->get_genname(), "_new");
	}
	Body.push(get_type_string(t));
	Body.SPC();
	Body.push(rhs_temp);
	Body.push(" = ");
	generate_expr(a->get_rhs());
	Body.pushln(";");

  bool require_mpi_red = false;
	if (a->is_target_scalar()) {
    ast_id * lhs_id = a->get_lhs_scala();
    if(!(lhs_id->find_info_bool(LABEL_DECLARED_IN_PAR))) 
      // only if the scalar variable is declared in a serial region we need to do mpi_red
    {
      require_mpi_red = true;
    }
  }
  if(require_mpi_red)
  {
    const char* new_temp;
		t = a->get_lhs_scala()->getTypeSummary();
		new_temp = (const char*) FE.voca_temp_name_and_add(a->get_lhs_scala()->get_genname(), "_red");
    Body.push(get_type_string(t));
    Body.SPC();
    Body.push(new_temp);
    Body.pushln(";");

    if (a->get_reduce_type() == GMREDUCE_MIN) {
      Body.push("MPI_Allreduce( &");
      Body.push(rhs_temp);
      Body.push(", &");
      Body.push(new_temp);
      Body.pushln(", 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);");

    } else {
      Body.push("MPI_Allreduce( &");
      Body.push(rhs_temp);
      Body.push(", &");
      Body.push(new_temp);
      Body.pushln(", 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);");
    }

    rhs_temp = new_temp;
  }


	MPI_GEN.generate_mpi_get_block(fp, curr_indent, Body);

  //all rhs values are copied to temporaries at this point.
#ifdef __USE_BSP_OPT__
  //if not in bsp prepass
  if(!MPI_GEN.is_in_bsp_prepass()) {
    if(MPI_GEN.is_bsp_canonical()) {
      generate_opt_body(a, rhs_temp);
      Body.pushln("}"); // end of outer if
      return;
    }
  }
#endif

	MPI_GEN.mpi_get_details.clear();

	int curr_indent1 = 0;
	int fp1 = Body.get_write_ptr(curr_indent1);
	// Did for making fp1 < file_ptr
	Body.NL();

	Body.push("if (");
	if (a->is_target_scalar()) {
		generate_rhs_id(a->get_lhs_scala());
	} else {
		generate_rhs_field(a->get_lhs_field());


		//declare variable
		//declare get
	}
	if (a->get_reduce_type() == GMREDUCE_MIN) {
		Body.push(">");
	} else {
		Body.push("<");
	}
	Body.push(rhs_temp);
	Body.pushln(") {");


	if(!a->is_target_scalar()){
		MPI_GEN.generate_mpi_get_block(fp1, curr_indent1, Body);
		MPI_GEN.mpi_get_details.clear();

	}




	std::list<ast_node*> L = a->get_lhs_list();
	std::list<ast_expr*> R = a->get_rhs_list();
	std::list<ast_node*>::iterator I;
	std::list<ast_expr*>::iterator J;
	char** names = new char*[L.size()];
	int i = 0;
	J = R.begin();
	for (I = L.begin(); I != L.end(); I++, J++, i++) {
		ast_node* n = *I;
		ast_id* id;
		int type;
		if (n->get_nodetype() == AST_ID) {
			id = (ast_id*) n;
			type = id->getTypeSummary();
		} else {
			assert(n->get_nodetype() == AST_FIELD);
			ast_field* f = (ast_field*) n;
			id = f->get_second();
			type = id->getTargetTypeSummary();
		}
		names[i] = (char*) FE.voca_temp_name_and_add(id->get_genname(), "_arg");
		Body.push(get_type_string(type));
		Body.SPC();
		Body.push(names[i]);
		Body.push(" = ");
		generate_expr(*J);
		Body.pushln(";");
	}

	// LOCK, UNLOCK
	const char* LOCK_FN_NAME;
	const char* UNLOCK_FN_NAME;
	if (a->is_target_scalar()) {
		LOCK_FN_NAME = "";
		UNLOCK_FN_NAME = "";
	} else {
		ast_id* drv = a->get_lhs_field()->get_first();
		if (drv->getTypeInfo()->is_node_compatible()) {
			LOCK_FN_NAME = "lock_win_for_node";
			UNLOCK_FN_NAME = "unlock_win_for_node";
		} else if (drv->getTypeInfo()->is_edge_compatible()) {
			LOCK_FN_NAME = "lock_win_for_edge";
			UNLOCK_FN_NAME = "unlock_win_for_edge";
		} else {
			assert(false);
		}
	}

	Body.push(LOCK_FN_NAME);
	Body.push("(");
	if (a->is_target_scalar()) {
		Body.push("&");
		generate_rhs_id(a->get_lhs_scala());
	} else {
		generate_rhs_id(a->get_lhs_field()->get_first());
	}
	Body.pushln(");");

	int curr_indent2 = 0;
	int fp2 = Body.get_write_ptr(curr_indent2);


	Body.push("if (");
	if (a->is_target_scalar()) {
		generate_rhs_id(a->get_lhs_scala());
	} else {
		generate_rhs_field(a->get_lhs_field());
	}
	if (a->get_reduce_type() == GMREDUCE_MIN) {
		Body.push(">");
	} else {
		Body.push("<");
	}
	Body.push(rhs_temp);
	Body.pushln(") {");

	if(!(a->is_target_scalar()))
	{
		MPI_GEN.generate_mpi_get_block_without_lock(fp2, curr_indent2, Body, true);
		MPI_GEN.mpi_get_details.clear();

	}
	// lhs = rhs_temp
#ifdef __USE_BSP_OPT__
  bool lhs_local = false;
#endif

  if (a->is_target_scalar()) {
		generate_lhs_id(a->get_lhs_scala());
	} else {

		generate_lhs_field(a->get_lhs_field());
	  
#ifdef __USE_BSP_OPT__

    ast_field * f1 = a->get_lhs_field();
    if(f1->getTypeInfo()->is_node_property())
    {
      //it means lhs is accessing local data=>
      if(MPI_GEN.is_present_in_local_access_nodes(get_lib()->node_index(f1->get_first()))) {
           MPI_GEN.reset_bsp_canonical();
           MPI_GEN.reset_bsp_nested_1();
           MPI_GEN.reset_bsp_nested_2(); 
    Body.push("/*reset inside red*/");
      }

    }
#endif

	}


	Body.push(" = ");
	Body.push(rhs_temp);
	Body.pushln(";");


	if(!(a->is_target_scalar()))
	{
		MPI_GEN.generate_mpi_put_block_without_lock(a->get_lhs_field(), Body);

	}


	i = 0;
	for (I = L.begin(); I != L.end(); I++, i++) {
		ast_node* n = *I;
		if (n->get_nodetype() == AST_ID) {
			generate_lhs_id((ast_id*) n);
			ast_id *ne = (ast_id *)n;
		} else {
			generate_lhs_field((ast_field*) n);
			ast_field *ne = (ast_field *)n;
		}
		Body.push(" = ");
		Body.push(names[i]);
		Body.pushln(";");
		if (!(n->get_nodetype() == AST_ID)) {
			MPI_GEN.generate_mpi_put_block_without_lock((ast_field*) n, Body);
		}
	}

	Body.pushln("}"); // end of inner if

	Body.push(UNLOCK_FN_NAME);
	Body.push("(");
	if (a->is_target_scalar()) {
		Body.push("&");
		generate_rhs_id(a->get_lhs_scala());
	} else {
		generate_rhs_id(a->get_lhs_field()->get_first());
	}
	Body.pushln(");");

	Body.pushln("}"); // end of outer if

	Body.pushln("}"); // end of reduction
	// clean-up
	for (i = 0; i < (int) L.size(); i++)
		delete[] names[i];
	delete[] names;
	delete[] rhs_temp;
}

void gm_cpp_gen::generate_sent_return(ast_return *r) {
	if (FE.get_current_proc()->find_info_bool(CPPBE_INFO_HAS_PROPDECL)) {
		Body.push(CLEANUP_PTR);
		Body.pushln("();");
	}


	int current_indent = 0;
	int f_ptr = _Body.get_write_ptr(current_indent);
	MPI_GEN.mpi_get_details.clear();
	Body.push("/*here*/return");
	if (r->get_expr() != NULL) {
		Body.SPC();
		generate_expr(r->get_expr());
	}
	Body.pushln("; ");
	if(MPI_GEN.mpi_get_details.size() != 0)
		MPI_GEN.generate_mpi_get_block(f_ptr, current_indent, Body);
}

void gm_cpp_gen::generate_sent_nop(ast_nop* n) {
	switch (n->get_subtype()) {
	case NOP_REDUCE_SCALAR:
		((nop_reduce_scalar*) n)->generate(this);
		break;
		/* otherwise ask library to hande it */
	default: {
		get_lib()->generate_sent_nop(n);
		break;
	}
	}

	return;
}

void gm_cpp_gen::generate_expr_inf(ast_expr *e) {
	char* temp = temp_str;
	assert(e->get_opclass() == GMEXPR_INF);
	int t = e->get_type_summary();
	switch (t) {
	case GMTYPE_INF:
	case GMTYPE_INF_INT:
		sprintf(temp, "%s", e->is_plus_inf() ? "INT_MAX" : "INT_MIN");
		break;
	case GMTYPE_INF_LONG:
		sprintf(temp, "%s", e->is_plus_inf() ? "LLONG_MAX" : "LLONG_MIN");
		break;
	case GMTYPE_INF_FLOAT:
		sprintf(temp, "%s", e->is_plus_inf() ? "FLT_MAX" : "FLT_MIN");
		break;
	case GMTYPE_INF_DOUBLE:
		sprintf(temp, "%s", e->is_plus_inf() ? "DBL_MAX" : "DBL_MIN");
		break;
	default:
		printf("what type is it? %d", t);
		assert(false);
		sprintf(temp, "%s", e->is_plus_inf() ? "INT_MAX" : "INT_MIN");
		break;
	}
	Body.push(temp);
	return;
}

void gm_cpp_gen::generate_expr_abs(ast_expr *e) {
	Body.push(" std::abs(");
	generate_expr(e->get_left_op());
	Body.push(") ");
}
void gm_cpp_gen::generate_expr_minmax(ast_expr *e) {
	if (e->get_optype() == GMOP_MIN) {
		Body.push(" std::min(");
	} else {
		Body.push(" std::max(");
	}
	generate_expr(e->get_left_op());
	Body.push(",");
	generate_expr(e->get_right_op());
	Body.push(") ");
}

void gm_cpp_gen::generate_expr_nil(ast_expr* ee) {
	get_lib()->generate_expr_nil(ee, Body);
}

const char* gm_cpp_gen::get_function_name(int methodId, bool& addThreadId) {
	switch (methodId) {
	case GM_BLTIN_TOP_DRAND:
		addThreadId = true;
		return "gm_rt_uniform";
	case GM_BLTIN_TOP_IRAND:
		addThreadId = true;
		return "gm_rt_rand";
	case GM_BLTIN_TOP_LOG:
		return "log";
	case GM_BLTIN_TOP_EXP:
		return "exp";
	case GM_BLTIN_TOP_POW:
		return "pow";
	default:
		assert(false);
		return "ERROR";
	}
}

void gm_cpp_gen::generate_expr_builtin(ast_expr* ee) {
	ast_expr_builtin* e = (ast_expr_builtin*) ee;

	gm_builtin_def* def = e->get_builtin_def();

	ast_id* driver;
	if (e->driver_is_field())
	{
		//Body.push("/*dif*/");
		driver = ((ast_expr_builtin_field*) e)->get_field_driver()->get_second();
	}
	else
	{

		driver = e->get_driver();
	}

	assert(def != NULL);
	int method_id = def->get_method_id();
	if (driver == NULL) {
		//Body.push("/*DIN*/");
		bool add_thread_id = false;
		const char* func_name = get_function_name(method_id, add_thread_id);
		Body.push(func_name);
		Body.push('(');
		generate_expr_list(e->get_args());
		if (add_thread_id) {
			if (e->get_args().size() > 0)
				Body.push(", G.get_rank()");
			else
				Body.push("G.get_rank()");
		}
		Body.push(")");
	} else {
		//Body.push("/*DINN*/");
		get_lib()->generate_expr_builtin((ast_expr_builtin*) e, Body);
	}
}

void gm_cpp_gen::prepare_parallel_for(bool need_dynamic) {
	//changing for mpi
	//if (is_under_parallel_sentblock())
	//   Body.push("#pragma omp for nowait"); // already under parallel region.
	//else
	//  Body.push("#pragma omp parallel for");

	//if (need_dynamic) {
	//  Body.push(" schedule(dynamic,128)");

	//}
	Body.push("//mpi for");

	Body.NL();
}

