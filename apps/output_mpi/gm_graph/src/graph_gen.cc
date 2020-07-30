#include <bits/stdc++.h>
#include <stdlib.h>
#include <set>
#include <math.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>
#include <sys/time.h>
#include <fstream>

#include "graph_gen.h"
#include "gm_rand.h"

using namespace std;

gm_graph* create_uniform_random_graph(node_t N, edge_t M, long seed, bool use_xorshift_rng) {
    
    gm_rand xorshift_rng(seed);
    if (!use_xorshift_rng) {
        srand(seed);
    }

    gm_graph* g = new gm_graph();
    g->prepare_external_creation(N, M);

    node_t* src = new node_t[M];
    node_t* dest = new node_t[M];
    edge_t* degree = new edge_t[N];
    memset(degree, 0, sizeof(edge_t) * N);

    for (edge_t i = 0; i < M; i++) {
        node_t r;
        if (use_xorshift_rng) r = (edge_t)xorshift_rng.rand();
        else r = rand(); //TODO 64-bit ?
        src[i] = r % N;  
        if (use_xorshift_rng) r = (edge_t)xorshift_rng.rand();
        else r = rand(); //TODO 64-bit ?
        dest[i] = r % N; 

        degree[src[i]]++;
    }

    g->begin[0] = 0;
    for (node_t i = 1; i <= N; i++) {
        g->begin[i] = g->begin[i - 1] + degree[i - 1];
    }

    for (edge_t i = 0; i < M; i++) {
        node_t u = src[i];
        node_t v = dest[i];

        edge_t pos = degree[u]--;
        assert(pos > 0);
        g->node_idx[g->begin[u] + pos - 1] = v;  // set end node of this edge
    }

    delete[] src;
    delete[] dest;
    delete[] degree;

    return g;
}

//-----------------------------------------------------------------------------
// Note: this method is 20x slower than the above method
//-----------------------------------------------------------------------------
gm_graph* create_uniform_random_graph2(node_t N, edge_t M, long seed) {
    srand(seed);

    gm_graph *G = new gm_graph();
    for (node_t i = 0; i < N; i++) {
        G->add_node();
    }
    for (edge_t i = 0; i < M; i++) {
        node_t from = rand() % N;
        node_t to = rand() % N;
        G->add_edge(from, to);
    }

    struct timeval T1, T2;
    gettimeofday(&T1, NULL);
    G->freeze();
    gettimeofday(&T2, NULL);
    printf("time for freeze (ms) = %lf\n", ((T2.tv_sec) - (T1.tv_sec)) * 1000 - (T2.tv_usec - T1.tv_usec) * 0.001);

    return G;
}

//-----------------------------------------------------------------------------
// Note: Based on create_uniform_random_graph2
//-----------------------------------------------------------------------------
gm_graph* create_uniform_random_nonmulti_graph(node_t N, edge_t M, long seed) {
    srand(seed);

    gm_graph *G = new gm_graph();
    for (node_t i = 0; i < N; i++) {
        G->add_node();
    }
    for (edge_t i = 0; i < M; i++) {
        node_t from = rand() % N;
        node_t to = rand() % N;
        if (!G->has_edge(from, to))
          G->add_edge(from, to);
        else
          i--;
    }

    G->freeze();

    return G;
}

/** 
 Create RMAT graph
 a, b, c : params
 */
gm_graph* create_RMAT_graph(node_t N, edge_t M, long rseed, double a, double b, double c, bool permute) {
    double d;
    assert(a + b + c < 1);
    d = 1 - (a + b + c);

    // my favorite random seed
    srand48(rseed);

    gm_graph* g = new gm_graph();
    g->prepare_external_creation(N, M);

    //----------------------------------------------
    // generate edges
    //----------------------------------------------
    // 0. init
    node_t* src = new node_t[M];
    node_t* dest = new node_t[M];
    edge_t* degree = new edge_t[N];
    memset(degree, 0, sizeof(edge_t) * N);

    node_t SCALE = (node_t) log2((double) N);

    // 1. edge-gen
    for (edge_t i = 0; i < M; i++) {
        node_t u = 1;
        node_t v = 1;
        node_t step = N / 2;
        double av = a;
        double bv = b;
        double cv = c;
        double dv = d;

        double p = drand48();
        if (p < av) { // do nothing
        } else if (p < (av + bv)) {
            v += step;
        } else if (p < (av + bv + cv)) {
            u += step;
        } else {
            v += step;
            u += step;
        }
        for (node_t j = 1; j < SCALE; j++) {
            step = step / 2;
            double var = 0.1;
            av *= 0.95 + var * drand48();		// vary abcd by 10%
            bv *= 0.95 + var * drand48();
            cv *= 0.95 + var * drand48();
            dv *= 0.95 + var * drand48();

            double S = av + bv + cv + dv;
            av = av / S;
            bv = bv / S;
            cv = cv / S;
            dv = dv / S;

            // choose partition
            p = drand48();
            if (p < av) { // do nothing
            } else if (p < (av + bv)) {
                v += step;
            } else if (p < (av + bv + cv)) {
                u += step;
            } else {
                v += step;
                u += step;
            }
        }

        src[i] = u - 1;
        dest[i] = v - 1;

        // avoid self edges
        if (src[i] == dest[i]) {
            i = i - 1;
            continue;
        }
    }

    // 2. permutate vertice
    // so that, one can't know what are the high-degree edges from node_id
    if (permute) {
        node_t* P = new node_t[N];
        for (node_t i = 0; i < N; i++)
            P[i] = i;

        for (node_t i = 0; i < N; i++) {
            node_t j = (node_t) (N * drand48());
            node_t temp = P[j];
            P[j] = P[i];
            P[i] = temp;
        }

        for (edge_t i = 0; i < M; i++) {
            src[i] = P[src[i]];
            dest[i] = P[dest[i]];
        }

        delete[] P;
    }

    // 3. count degree
    for (edge_t i = 0; i < M; i++) {
        degree[src[i]]++;
    }

    // (It is possible this routine creates multi-edges between a node-pair)
    // (Should be check later, or be ignored now)

    // 4. Now setup G's data structures
    g->begin[0] = 0;
    for (node_t i = 1; i <= N; i++) {
        g->begin[i] = g->begin[i - 1] + degree[i - 1];
    }

    for (edge_t i = 0; i < M; i++) {
        node_t u = src[i];
        node_t v = dest[i];
        edge_t pos = degree[u]--;

        g->node_idx[g->begin[u] + pos - 1] = v;  // set end node of this edge
    }

    delete[] src;
    delete[] dest;
    delete[] degree;

    return g;
}


/*gm_graph* create_RMAT_graph(node_t N, edge_t M, long rseed, double a, double b, double c, bool permute) {
    double d;
    assert(a + b + c < 1);
    d = 1 - (a + b + c);

    // my favorite random seed
    srand48(rseed);

    gm_graph* g = new gm_graph();
    g->prepare_external_creation(N, M);

    //----------------------------------------------
    // generate edges
    //----------------------------------------------
    // 0. init
    node_t* src = new node_t[M];
    node_t* dest = new node_t[M];
    edge_t* degree = new edge_t[N];
    memset(degree, 0, sizeof(edge_t) * N);

    node_t SCALE = (node_t) log2((double) N);

    // 1. edge-gen
    for (edge_t i = 0; i < M; i++) {
        node_t u = 1;
        node_t v = 1;
        node_t step = N / 2;
        double av = a;
        double bv = b;
        double cv = c;
        double dv = d;

        double p = drand48();
        if (p < av) { // do nothing
        } else if (p < (av + bv)) {
            v += step;
        } else if (p < (av + bv + cv)) {
            u += step;
        } else {
            v += step;
            u += step;
        }
        for (node_t j = 1; j < SCALE; j++) {
            step = step / 2;
            double var = 0.1;
            av *= 0.95 + var * drand48();		// vary abcd by 10%
            bv *= 0.95 + var * drand48();
            cv *= 0.95 + var * drand48();
            dv *= 0.95 + var * drand48();

            double S = av + bv + cv + dv;
            av = av / S;
            bv = bv / S;
            cv = cv / S;
            dv = dv / S;

            // choose partition
            p = drand48();
            if (p < av) { // do nothing
            } else if (p < (av + bv)) {
                v += step;
            } else if (p < (av + bv + cv)) {
                u += step;
            } else {
                v += step;
                u += step;
            }
        }

        src[i] = u - 1;
        dest[i] = v - 1;

        // avoid self edges
        if (src[i] == dest[i]) {
            i = i - 1;
            continue;
        }
    }

    // 2. permutate vertice
    // so that, one can't know what are the high-degree edges from node_id
    if (permute) {
        node_t* P = new node_t[N];
        for (node_t i = 0; i < N; i++)
            P[i] = i;

        for (node_t i = 0; i < N; i++) {
            node_t j = (node_t) (N * drand48());
            node_t temp = P[j];
            P[j] = P[i];
            P[i] = temp;
        }

        for (edge_t i = 0; i < M; i++) {
            src[i] = P[src[i]];
            dest[i] = P[dest[i]];
        }

        delete[] P;
    }

    // 3. count degree
    for (edge_t i = 0; i < M; i++) {
        degree[src[i]]++;
    }

    // (It is possible this routine creates multi-edges between a node-pair)
    // (Should be check later, or be ignored now)

    // 4. Now setup G's data structures
    g->begin[0] = 0;
    for (node_t i = 1; i <= N; i++) {
        g->begin[i] = g->begin[i - 1] + degree[i - 1];
    }

    for (edge_t i = 0; i < M; i++) {
        node_t u = src[i];
        node_t v = dest[i];
        edge_t pos = degree[u]--;

        g->node_idx[g->begin[u] + pos - 1] = v;  // set end node of this edge
    }

    delete[] src;
    delete[] dest;
    delete[] degree;

    return g;
}*/

/* Creates graphs in CSR format from gr files. 
 * The weights are stored in the file with name that is passed as second argument
 */

gm_graph* create_graph_from_gr(char* in_graph_filename, char* out_weights_filename) {

    fstream Gfile; 
    ofstream Wfile;
    Gfile.open(in_graph_filename); 
    Wfile.open(out_weights_filename); 
    
    gm_graph *G = new gm_graph();

    int N, M;
    string word;
    
    while (Gfile >> word) 
    { 
        if(word == "p"){
          Gfile >> word;
          Gfile >> N;
          Gfile >> M;  
          for (node_t i = 0; i < N; i++) {
            G->add_node();
          }
        } else if(word == "a") {
          int src;
          int dest;
          int wt;
          Gfile >> src;
          Gfile >> dest;
          Gfile >> wt;
          G->add_edge(--src, --dest);
          Wfile << wt;
          Wfile << endl;
        } 
    } 
    
    G->freeze();

    Wfile.close();
    Gfile.close();
    return G;
}



gm_graph* create_graph_for_twt(char* in_graph_filename, char* out_weights_filename, int N, char* out_dh_falcon_file) {

    fstream Gfile; 
    Gfile.open(in_graph_filename); 
    
    gm_graph *G = new gm_graph();

    int M;
    string word;
    int edge_no = 0;
    while (Gfile >> word) 
    { 
        if (word == "%") {
          Gfile >> word;
          Gfile >> word;
          Gfile >> word;
          Gfile >> M;
          printf("Number of edges is %d\n", M);
          for (node_t i = 0; i < N; i++) {
            G->add_node();
          }
          printf("Added all the nodes\n");
        }else {
          int src;
          int dest;
          
          src = stoi(word);
          Gfile >> dest;
          G->add_edge(--src, --dest);
          printf("reading edge %d\n", edge_no++);
          
        }
    } 
    
    printf("Finishing graph reads\n");
    G->freeze();

    Gfile.close();
    return G;
}


