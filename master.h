#pragma once
#include <thread>
#include "bandwidth_config.h"
#include "common.h"
#include "job_text.h"
#include "tera_sort/CodedConfiguration.h"
#include "tera_sort/Configuration.h"
#include "tera_sort/PartitionSampling.h"
enum class MasterState { Free, Mapping, Reducing, Done };

struct UtilityInfo {
    double time;
    double network_load;
    double computation_load;
    std::string PrintInfo() {
        return "time: " + std::to_string(time) + " network_load: " + std::to_string(network_load) +
               " computation_load: " + std::to_string(computation_load);
    }
};

/**
 * 负责一个job的调度
*/
class Master {
public:
    Master(int id, std::string my_host_name_prefix, int worker_host_num,
           std::vector<std::string> worker_host_name_prefixs, std::shared_ptr<BandWidthConfigModule> bw_config, std::shared_ptr<MasterWorkerState> master_worker_state)
        : state_(MasterState::Free),
          id_(id),
          my_host_name_prefix_(my_host_name_prefix),
          worker_host_num_(worker_host_num),
          worker_host_name_prefixs_(worker_host_name_prefixs),
          bw_config_(bw_config),
          master_worker_state_(master_worker_state) {
        mailbox_ = simgrid::s4u::Mailbox::by_name(my_host_name_prefix + ":" + std::to_string(id));
        barrier_mailbox_ = simgrid::s4u::Mailbox::by_name(my_host_name_prefix + ":" + std::to_string(id) + ":barrier");
    }

    // 初始化该master需要执行的job
    void SetJobText(JobText job_text) { job_text_ = job_text; };

    // 被MasterManager调用，接收一个Job
    void DoJob(const std::vector<int>& worker_ids);
    // receive job from master manager
    void Run();

    UtilityInfo DoJobTryR(std::vector<int> worker_ids, int r);

    MasterState GetMasterState() const { return state_; }

    void SetMasterState(MasterState state) { state_ = state; }

    void SetR(int r) { r_ = r; }

private:
    UtilityInfo TeraSort();
    UtilityInfo CodedTeraSort();

    MasterState state_;
    int id_;  // id of master, from 1 to master_num
    JobText job_text_;
    std::string my_host_name_prefix_;  // name of master node
    int worker_host_num_;
    std::vector<std::string> worker_host_name_prefixs_;        // record all worker host name
    std::vector<simgrid::s4u::Mailbox*> worker_mailboxs_;      // send info to worker managers
    simgrid::s4u::Mailbox* mailbox_;                           // receive data
    simgrid::s4u::Mailbox* barrier_mailbox_;                   // receive barrier info from worker
    std::vector<simgrid::s4u::Mailbox*> worker_job_mailboxs_;  // worker mailbox responsible for this job
    Configuration conf;
    CodedConfiguration coded_conf;
    std::atomic<int> r_;

    std::shared_ptr<BandWidthConfigModule> bw_config_;
    std::shared_ptr<MasterWorkerState> master_worker_state_;
};

class MasterManager {
public:
    MasterManager(int master_num, int worker_host_num, std::string my_host_name_prefix,
                  std::vector<std::string> worker_host_name_prefixs, std::shared_ptr<BandWidthConfigModule> bw_config,
                  std::shared_ptr<MasterWorkerState> master_worker_state)
        : master_num_(master_num),
          worker_host_num_(worker_host_num),
          my_host_name_prefix_(my_host_name_prefix),
          worker_host_name_prefixs_(worker_host_name_prefixs),
          bw_config_(bw_config),
          master_worker_state_(master_worker_state) {
        for (int i = 1; i <= master_num; i++) {
            masters_state_[i] = MasterState::Free;
        }
        mailbox_ = simgrid::s4u::Mailbox::by_name(my_host_name_prefix + ":0");
        for (auto worker_host_name_prefix : worker_host_name_prefixs_) {
            worker_host_mailboxs_.push_back(simgrid::s4u::Mailbox::by_name(worker_host_name_prefix + ":0"));
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
    std::string my_host_name_prefix_;                           // name of master host
    std::vector<std::string> worker_host_name_prefixs_;         // name of worker hosts
    simgrid::s4u::Mailbox* mailbox_;                            // receive message from worker manager
    std::vector<simgrid::s4u::Mailbox*> worker_host_mailboxs_;  // communicate with worker managers
    std::mutex mutex_;                                          // lock before modify master state
    std::unordered_map<int, MasterState> masters_state_;
    std::shared_ptr<BandWidthConfigModule> bw_config_;
    std::shared_ptr<MasterWorkerState> master_worker_state_;
};