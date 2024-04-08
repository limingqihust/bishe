#pragma once
#include <thread>
#include "bandwidth_config.h"
#include "common.h"
#include "tera_sort/CodedConfiguration.h"
#include "tera_sort/Configuration.h"
#include "tera_sort/PartitionSampling.h"
#include "job_text.h"
enum class MasterState { Free, Mapping, Reducing, Done };

struct UtilityInfo {
    double time;
    double network_load;
    double computation_load;
    std::string PrintInfo() {
        return "time: " + std::to_string(time) + " network_load: " + std::to_string(network_load) + " computation_load: " + std::to_string(computation_load); 
    }
};

/**
 * 负责一个job的调度
*/
class Master {
public:
    Master(int id, std::string my_host_name, int worker_host_num, std::vector<std::string> worker_host_names,
           std::shared_ptr<BandWidthConfigModule> bw_config)
        : state_(MasterState::Free),
          id_(id),
          my_host_name_(my_host_name),
          worker_host_num_(worker_host_num),
          worker_host_names_(worker_host_names),
          bw_config_(bw_config) {
        mailbox_ = simgrid::s4u::Mailbox::by_name(my_host_name + ":" + std::to_string(id));
        barrier_mailbox_ = simgrid::s4u::Mailbox::by_name(my_host_name + ":" + std::to_string(id) + ":barrier");
    }

    // 初始化该master需要执行的job
    void SetJobText(JobText job_text) { job_text_ = job_text; };

    // 被MasterManager调用，接收一个Job
    void DoJob(const std::vector<int>& worker_ids);

    UtilityInfo DoJobTryR(std::vector<int> worker_ids, int r);

    MasterState GetMasterState() const { return state_; }

    void SetMasterState(MasterState state) { state_ = state; }

    void SetR(int r) { r_ = r; }

private:
    UtilityInfo TeraSort();
    UtilityInfo CodedTeraSort();

    MasterState state_;
    int id_;
    JobText job_text_;
    std::string my_host_name_;  // name of master node
    int worker_host_num_;
    std::vector<std::string> worker_host_names_;               // record all worker host name
    std::vector<simgrid::s4u::Mailbox*> worker_mailboxs_;      // send info to worker managers
    simgrid::s4u::Mailbox* mailbox_;                           // receive data
    simgrid::s4u::Mailbox* barrier_mailbox_;                   // receive barrier info from worker 
    std::vector<simgrid::s4u::Mailbox*> worker_job_mailboxs_;  // worker mailbox responsible for this job
    Configuration conf;
    CodedConfiguration coded_conf;
    std::atomic<int> r_;

    std::shared_ptr<BandWidthConfigModule> bw_config_;
};

class MasterManager {
public:
    MasterManager(int master_num, int worker_host_num, std::string my_host_name,
                  std::vector<std::string> worker_host_names, std::shared_ptr<BandWidthConfigModule> bw_config)
        : master_num_(master_num),
          worker_host_num_(worker_host_num),
          my_host_name_(my_host_name),
          worker_host_names_(worker_host_names),
          bw_config_(bw_config) {
        for (int i = 0; i < master_num; i++) {
            masters_.emplace_back(
                std::make_shared<Master>(i, my_host_name, worker_host_num, worker_host_names, bw_config));
            master_thds_.emplace_back(std::thread());
        }
        mailbox_ = simgrid::s4u::Mailbox::by_name(my_host_name_);
        for (auto worker_host_name : worker_host_names_) {
            worker_host_mailboxs_.push_back(simgrid::s4u::Mailbox::by_name(worker_host_name));
        }
    }

    // receive a job, alloc a master to execute this job
    void Run(JobText& job_text);
    void SetR(int r);
    UtilityInfo RunTryR(JobText& job, int r);

private:
    int FindFreeMaster();
    std::vector<int> RequestWorkerIds(int master_id, const JobText& job_text);
    void SplitInput(const JobText& job_text);

    int master_num_;                                            // num of master in this master host
    int worker_host_num_;                                       // num of worker host
    std::string my_host_name_;                                  // name of master host
    std::vector<std::string> worker_host_names_;                // name of worker hosts
    simgrid::s4u::Mailbox* mailbox_;                            // receive message from worker manager
    std::vector<simgrid::s4u::Mailbox*> worker_host_mailboxs_;  // communicate with worker managers
    std::mutex mutex_;                                          // lock before modify master state
    std::vector<std::shared_ptr<Master>> masters_;
    std::vector<std::thread> master_thds_;

    std::shared_ptr<BandWidthConfigModule> bw_config_;
};