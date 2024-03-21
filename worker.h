#pragma once
#include "common.h"

enum class WorkerState { Free, Mapping, Reducing };

/**
 * 被分配给一个job
 * 负责处理一个job中被分配给自己的Map Task和Reduce Task
 */
class Worker {
public:
    Worker(std::string my_host_name, std::string master_host_name, int id)
        : state_(WorkerState::Free), id_(id), my_host_name_(my_host_name), master_host_name_(master_host_name) {
        mailbox_ = simgrid::s4u::Mailbox::by_name(my_host_name_ + ":" + std::to_string(id));
    }

    void DoJob();

    WorkerState GetWorkerState() const { return state_; }

    void SetWorkerState(WorkerState state) { state_ = state; }

    void SetMasterMailbox(int master_id) {
        master_mailbox_ = simgrid::s4u::Mailbox::by_name(master_host_name_ + ":" + std::to_string(master_id));
    }

private:
    void TeraSort();

    WorkerState state_;
    int id_;
    std::string my_host_name_;
    std::string master_host_name_;
    simgrid::s4u::Mailbox* mailbox_;         // receive message from master
    simgrid::s4u::Mailbox* master_mailbox_;  // send message to master
};

/**
 * WorkerManager负责管理本节点下的多个Worker
 */
class WorkerManager {
public:
    WorkerManager(std::string my_host_name, std::string master_host_name, int id, int worker_num)
        : my_host_name_(my_host_name),
          master_host_name_(master_host_name),
          worker_num_(worker_num),
          id_(id) {
        for (int i = 0; i < worker_num; i++) {
            workers_.emplace_back(std::make_shared<Worker>(my_host_name, master_host_name, i));
        }

        mailbox_ = simgrid::s4u::Mailbox::by_name(my_host_name_);
    }

    void Run();

private:
    int FindFreeWorker();
    std::string my_host_name_;         // name of this worker node
    std::string master_host_name_;  // name of master node
    int worker_num_;
    int id_;
    std::mutex mutex_;
    std::vector<std::shared_ptr<Worker>> workers_;
    simgrid::s4u::Mailbox* mailbox_;  // receive message from master
};
