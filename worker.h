#pragma once
#include "bandwidth_config.h"
#include "common.h"
#include "tera_sort/CodeGeneration.h"
#include "tera_sort/CodedConfiguration.h"
#include "tera_sort/Configuration.h"
#include "tera_sort/PartitionSampling.h"
#include "tera_sort/Trie.h"
enum class WorkerState { Free, Mapping, Reducing };

/**
 * 被分配给一个job
 * 负责处理一个job中被分配给自己的Map Task和Reduce Task
 */
class Worker {
public:
    Worker(std::string my_host_name, std::string master_host_name, int worker_host_num,
           std::vector<std::string> worker_host_names, int id, int host_id,
           std::shared_ptr<BandWidthConfigModule> bw_config)
        : state_(WorkerState::Free),
          id_(id),
          my_host_name_(my_host_name),
          master_host_name_(master_host_name),
          worker_host_num_(worker_host_num),
          worker_host_names_(worker_host_names),
          host_id_(host_id),
          bw_config_(bw_config) {
        mailbox_ = simgrid::s4u::Mailbox::by_name(my_host_name_ + ":" + std::to_string(id));
        barrier_mailbox_ = simgrid::s4u::Mailbox::by_name(my_host_name + ":" + std::to_string(id) + ":barrier");
    }

    void DoJob();

    WorkerState GetWorkerState() const { return state_; }

    void SetWorkerState(WorkerState state) { state_ = state; }

    void SetMasterMailbox(int master_id) {
        master_mailbox_ = simgrid::s4u::Mailbox::by_name(master_host_name_ + ":" + std::to_string(master_id));
    }

    NodeSetDataPartMap encodePreData;
    NodeSetDataPartMap decodePreData;

private:
    void TeraSort();
    void ExecMap();
    void ExecReduce();
    TrieNode* buildTrie(PartitionList* partitionList, int lower, int upper, unsigned char* prefix, int prefixSize,
                        int maxDepth);
    void PrintLocalList();
    void PrintPartitionCollection();
    void PrintPartitionTxData();
    void PrintInputPartitionCollection();

    void CodedTeraSort();
    void GenMulticastGroup();
    void ExecCodedMap();
    void ExecCodedEncoding();
    void ExecCodedShuffle();
    void ExecCodedDecoding();
    void ExecCodedReduce();
    void SendEncodeData(EnData& endata, std::vector<int> dst_ids);
    void RecvEncodeData(SubsetSId nsid);

    WorkerState state_;
    int id_;
    int host_id_;
    std::string my_host_name_;
    std::string master_host_name_;
    int worker_host_num_;
    std::vector<std::string> worker_host_names_;
    std::vector<int> worker_partener_ids_;
    simgrid::s4u::Mailbox* mailbox_;          // receive message from master
    simgrid::s4u::Mailbox* master_mailbox_;   // send message to master
    simgrid::s4u::Mailbox* barrier_mailbox_;  // for barrier

    std::shared_ptr<BandWidthConfigModule> bw_config_;

    /* used by tera_sort */
    Configuration* conf;
    CodedConfiguration* coded_conf;
    CodeGeneration* cg;
    InputPartitionCollection inputPartitionCollection;
    NodeSetEnDataMap encodeDataSend;
    NodeSetVecEnDataMap encodeDataRecv;
    MulticastGroupMap multicastGroupMap;
    NodeSet localLoadSet;

    PartitionList partitionList;
    PartitionCollection partitionCollection;
    PartitionPackData partitionTxData;
    PartitionPackData partitionRxData;
    LineList localList;
    TrieNode* trie;
};

/**
 * WorkerManager负责管理本节点下的多个Worker
 */
class WorkerManager {
public:
    WorkerManager(std::string my_host_name, std::string master_host_name, int id, int worker_num, int worker_host_num,
                  std::vector<std::string> worker_host_names, std::shared_ptr<BandWidthConfigModule> bw_config)
        : my_host_name_(my_host_name),
          master_host_name_(master_host_name),
          worker_num_(worker_num),
          worker_host_names_(worker_host_names),
          id_(id),
          bw_config_(bw_config) {
        assert(worker_host_num == worker_host_names_.size());
        for (int i = 0; i < worker_num; i++) {
            workers_.emplace_back(std::make_shared<Worker>(my_host_name, master_host_name, worker_host_num,
                                                           worker_host_names, i, id, bw_config));
        }

        mailbox_ = simgrid::s4u::Mailbox::by_name(my_host_name_);
        master_host_mailbox_ = simgrid::s4u::Mailbox::by_name(master_host_name_);
    }

    void Run();

private:
    int FindFreeWorker();
    std::string my_host_name_;      // name of this worker node
    std::string master_host_name_;  // name of master node
    int worker_num_;
    std::vector<std::string> worker_host_names_;
    int id_;
    std::mutex mutex_;
    std::vector<std::shared_ptr<Worker>> workers_;
    simgrid::s4u::Mailbox* mailbox_;              // receive message from master manager
    simgrid::s4u::Mailbox* master_host_mailbox_;  // send message to master manager

    std::shared_ptr<BandWidthConfigModule> bw_config_;
};
