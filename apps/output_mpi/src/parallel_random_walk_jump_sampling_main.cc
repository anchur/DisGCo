#include "common_main.h"
#include "parallel_random_walk_jump_sampling.h"

class my_main: public main_t
{
public:

    virtual bool prepare() {
        return true;
    }

    virtual bool run() {
        int start = rand() % G.num_nodes();
        gm_node_set set(G.num_nodes());
				bool selected = true;
        parallel_random_walk_jump_sampling(G, start, 0.15, 1, &selected);
        return true;
    }

    virtual bool post_process() {
        return true;
    }
};

int main(int argc, char** argv) {
    my_main M;
    M.main(argc, argv);
}
