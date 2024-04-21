#include "bandwidth_config.h"
#include "common.h"
#include "master.h"
#include "online_learning.h"
#include "worker.h"
XBT_LOG_NEW_DEFAULT_CATEGORY(s4u_app_masterworker, "Messages specific for this example");

// init bandwidth config
auto bw_config = std::make_shared<BandWidthConfigModule>("./config/bandwidth.config");
auto master_worker_state = std::make_shared<MasterWorkerState>();

/**
 * 
 *         / node0:1
 *         - node0:2 
 * node0:0 - node0:3
 *         - node0:4
 *         \ node0:5
 * 
*/
static void my_master_manager(std::vector<std::string> args) {
    // init job_queue
    auto job_queue = std::make_shared<ConcurrencyQueue>("./config/test_input");

    // init master_manager
    const std::string my_host_name_prefix = args[1];
    int master_num = std::stoi(args[2]);
    int worker_host_num = std::stoi(args[3]);
    std::vector<std::string> worker_host_name_prefixs;
    for (int i = 4; i < args.size(); i++) {
        worker_host_name_prefixs.emplace_back(args[i]);
    }
    master_worker_state->mutex.lock();
    for (int i = 1; i <= master_num; i++) {
        master_worker_state->master_state[i] = State::Free;
    }
    for (int i = 1; i <= worker_host_num; i++) {
        for (int j = 1; j <= master_num; j++) {
            master_worker_state->worker_state[i][j] = State::Free;
        }
    }
    master_worker_state->mutex.unlock();


    auto master_manager = std::make_shared<MasterManager>(master_num, worker_host_num, my_host_name_prefix,
                                                          worker_host_name_prefixs, bw_config, master_worker_state);

    for (int i = 0; i < 5; i++) {
        JobText job = job_queue->Pop();
        master_manager->Run(job);
    }
    auto mailbox = simgrid::s4u::Mailbox::by_name("unreachable");
    mailbox->get<char>();
    // online_learning_thd.join();
}

/**
 *             / node[1-6]:1
 *             - node[1-6]:2
 * node[1-6]:0 - node[1-6]:3
 *             - node[1-6]:4
 *             \ node[1-6]:5
*/
static void my_worker_manager(std::vector<std::string> args) {
    const std::string my_host_name_prefix = args[1];
    const std::string master_host_name_prefix = args[2];
    int worker_num = std::stoi(args[3]);
    int id = std::stoi(args[4]);
    std::vector<std::string> worker_host_names;
    for (int i = 5; i < args.size(); i++) {
        worker_host_names.emplace_back(args[i]);
    }
    auto worker_manager =
        std::make_shared<WorkerManager>(my_host_name_prefix, master_host_name_prefix, id, worker_num,
                                        worker_host_names.size(), worker_host_names, bw_config, master_worker_state);
    worker_manager->Run();
}

static void my_master(std::vector<std::string> args) {
    int id = std::stoi(args[1]);
    const std::string my_host_name_prefix = args[2];
    int worker_host_num = std::stoi(args[3]);
    std::vector<std::string> worker_host_name_prefixs;
    for (int i = 4; i < args.size(); i++) {
        worker_host_name_prefixs.emplace_back(args[i]);
    }
    auto master = std::make_shared<Master>(id, my_host_name_prefix, worker_host_num, worker_host_name_prefixs,
                                           bw_config, master_worker_state);
    master->Run();
}

static void my_worker(std::vector<std::string> args) {
    int host_id = std::stoi(args[1]);
    int id = std::stoi(args[2]);
    const std::string my_host_name_prefix = args[3];
    const std::string master_host_name_prefix = args[4];
    int worker_host_num = std::stoi(args[5]);
    std::vector<std::string> worker_host_name_prefixs;
    for (int i = 6; i < args.size(); i++) {
        worker_host_name_prefixs.emplace_back(args[i]);
    }
    auto worker = std::make_shared<Worker>(my_host_name_prefix, master_host_name_prefix, worker_host_num,
                                           worker_host_name_prefixs, id, host_id, bw_config, master_worker_state);
    worker->Run();
}

// static void my_master(std::vector<std::string> args) {
//     assert(args.size() >= 4);

//     // init job_queue
//     auto job_queue = std::make_shared<ConcurrencyQueue>("./config/test_input");

//     // init master_manager
//     const std::string my_host_name_prefix = args[1];
//     int master_num = std::stoi(args[2]);
//     int worker_host_num = std::stoi(args[3]);
//     std::vector<std::string> worker_host_names;
//     for (int i = 4; i < args.size(); i++) {
//         worker_host_names.emplace_back(args[i]);
//     }
//     auto master_manager =
//         std::make_shared<MasterManager>(master_num, worker_host_num, my_host_name_prefix, worker_host_names, bw_config);

//     // start OnlineLearningModule
//     auto online_learning_module =
//         std::make_shared<OnlineLearningModule>(master_manager, job_queue, 1, worker_host_num - 1, 1.0);
//     online_learning_module->DoWork();

//     // // do job now
//     // for(int i = 0; i < 4; i++) {
//     //     JobText job_text = job_queue->Pop();
//     //     master_manager->Run(job_text);
//     // }
//     while (true) {
//         sleep(1);
//     }

//     // online_learning_thd.join();
// }

// static void my_worker(std::vector<std::string> args) {
//     assert(args.size() >= 5);

//     simgrid::s4u::Host* my_host = simgrid::s4u::this_actor::get_host();
//     const std::string my_host_name_prefix = args[1];
//     std::string master_host_name_prefix = args[2];
//     int worker_num = std::stoi(args[3]);
//     int id = std::stoi(args[4]);
//     std::vector<std::string> worker_host_names;
//     for (int i = 5; i < args.size(); i++) {
//         worker_host_names.emplace_back(args[i]);
//     }
//     auto worker_manager = std::make_shared<WorkerManager>(my_host_name_prefix, master_host_name_prefix, id, worker_num,
//                                                           worker_host_names.size(), worker_host_names, bw_config);
//     worker_manager->Run();
// }
int main(int argc, char* argv[]) {
    simgrid::s4u::Engine e(&argc, argv);

    /* Register the functions representing the actors */
    e.register_function("my_master_manager", &my_master_manager);
    e.register_function("my_worker_manager", &my_worker_manager);
    e.register_function("my_master", &my_master);
    e.register_function("my_worker", &my_worker);

    /* Load the platform description and then deploy the application */
    e.load_platform(argv[1]);
    e.load_deployment(argv[2]);

    /* Run the simulation */
    e.run();

    LOG_INFO("Simulation is over");

    return 0;
}
