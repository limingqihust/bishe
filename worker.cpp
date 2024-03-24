#include "worker.h"

/**
 * receive request from master, find a free worker to deal with map task and reduce task of this job
*/
void WorkerManager::Run() {
    while (true) {
        // 1. receive a request from master
        int* master_id = mailbox_->get<int>();
        LOG_INFO("[worker manager] id: %d, receive master_id: %d request for worker", id_, *master_id);
        // 2. find a free worker responsible for this job
        int worker_id = FindFreeWorker();
        workers_[worker_id]->SetMasterMailbox(*master_id);
        // 3. notify worker id to master
        auto master_mailbox = simgrid::s4u::Mailbox::by_name(master_host_name_);
        master_mailbox->put(new int(worker_id), 4);
        LOG_INFO("[worker manager] choose worker id: %d, notify master manager", id_);
        delete master_id;

        // 4. let this worker to do job(including map task, shuffle task and reduce task)
        workers_[worker_id]->DoJob();
        return;
    }
}

int WorkerManager::FindFreeWorker() {
    while (true) {
        mutex_.lock();
        for (int i = 0; i < worker_num_; i++) {
            if (workers_[i]->GetWorkerState() == WorkerState::Free) {
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
    // receive worker_partner_ids
    int* worker_partener_ids_temp = mailbox_->get<int>();
    for (int i = 0; i < worker_host_num_; i++) {
        worker_partener_ids_.push_back(worker_partener_ids_temp[i]);
    }
    delete[] worker_partener_ids_temp;
    // TeraSort();
    CodedTeraSort();
}


