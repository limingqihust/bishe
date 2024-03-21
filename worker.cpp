#include "worker.h"

/**
 * receive request from master, find a free worker to deal with map task and reduce task of this job
*/
void WorkerManager::Run() {
    while(true) {  
        // 1. receive a request from master 
        LOG_INFO("[worker] id: %d, wait for request", id_);
        int* master_id = mailbox_->get<int>();
        LOG_INFO("[worker] id: %d, receive master_id: %d request for worker", id_, *master_id);
        // 2. find a free worker responsible for this job
        int worker_id = FindFreeWorker();
        LOG_INFO("[worker] id: %d choose worker %d to do job", id_, worker_id);
        workers_[worker_id]->SetMasterMailbox(*master_id);
        // 3. notify worker id to master
        auto master_mailbox = simgrid::s4u::Mailbox::by_name(master_host_name_ + ":" + std::to_string(*master_id));
        master_mailbox->put(new int(worker_id), 4);
        LOG_INFO("[worker] id: %d notify master %s", id_, master_mailbox->get_cname());
        delete master_id;

        // 4. let this worker to do job(including map task, shuffle task and reduce task)
        workers_[worker_id]->DoJob();
        return ;
    }
}


int WorkerManager::FindFreeWorker() {
    while(true) {
        mutex_.lock();
        for(int i = 0; i < worker_num_; i++) {
            if(workers_[i]->GetWorkerState() == WorkerState::Free) {
                workers_[i]->SetWorkerState(WorkerState::Mapping);
                mutex_.unlock();
                return i;
            }
        }
        mutex_.unlock();
        sleep(5);
    }
}

/**
 * 1. return id of this worker to master for record
 * 2. do job
*/
void Worker::DoJob() {
    TeraSort();
}


