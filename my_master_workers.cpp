#include "bandwidth_config.h"
#include "common.h"
#include "master.h"
#include "online_learning.h"
#include "worker.h"
XBT_LOG_NEW_DEFAULT_CATEGORY(s4u_app_masterworker, "Messages specific for this example");

// init bandwidth config
auto bw_config = std::make_shared<BandWidthConfigModule>("./config/bandwidth.config");

static void my_master(std::vector<std::string> args) {
    assert(args.size() >= 4);

    // init job_queue
    auto job_queue = std::make_shared<ConcurrencyQueue>("./config/test_input");

    // init master_manager
    simgrid::s4u::Host* my_host = simgrid::s4u::this_actor::get_host();
    std::string my_host_name = my_host->get_name();
    int master_num = std::stoi(args[1]);
    int worker_host_num = std::stoi(args[2]);
    std::vector<std::string> worker_host_names;
    for (int i = 3; i < args.size(); i++) {
        worker_host_names.emplace_back(args[i]);
    }
    auto master_manager =
        std::make_shared<MasterManager>(master_num, worker_host_num, my_host_name, worker_host_names, bw_config);

    // start OnlineLearningModule
    // auto online_learning_module =
    //     std::make_shared<OnlineLearningModule>(master_manager, job_queue, 1, worker_host_num - 1, 1.0);
    // online_learning_module->DoWork();

    // // do job now
    for(int i = 0; i < 1; i++) {
        JobText job_text = job_queue->Pop();
        master_manager->Run(job_text);
    }
    while(true) {
        sleep(1);
    }

    // online_learning_thd.join();
}

static void my_worker(std::vector<std::string> args) {
    assert(args.size() >= 5);

    simgrid::s4u::Host* my_host = simgrid::s4u::this_actor::get_host();
    std::string my_host_name = my_host->get_name();
    std::string master_host_name = args[1];
    int worker_num = std::stoi(args[2]);
    int id = std::stoi(args[3]);
    std::vector<std::string> worker_host_names;
    for (int i = 4; i < args.size(); i++) {
        worker_host_names.emplace_back(args[i]);
    }
    auto worker_manager = std::make_shared<WorkerManager>(my_host_name, master_host_name, id, worker_num,
                                                          worker_host_names.size(), worker_host_names, bw_config);
    worker_manager->Run();
}
int main(int argc, char* argv[]) {
    simgrid::s4u::Engine e(&argc, argv);

    /* Register the functions representing the actors */
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
