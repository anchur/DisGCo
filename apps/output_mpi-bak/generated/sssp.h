#ifndef GM_GENERATED_CPP_SSSP_H
#define GM_GENERATED_CPP_SSSP_H

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <float.h>
#include <limits.h>
#include <cmath>
#include <algorithm>
#include <omp.h>
#include "gm.h"

void sssp(gm_graph& G, int32_t* G_dist, 
    int32_t* G_len, node_t& root);

#endif
