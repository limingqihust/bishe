#include "worker.h"

/**
 * receive request from master, find a free worker to deal with map task and reduce task of this job
*/
void WorkerManager::Run() {
    while (true) {
        // 1. receive a request from master
        LOG_INFO("[worker manager] my_host_name: %s wait for request from mailbox: %s", my_host_name_.c_str(), mailbox_->get_cname());
        char* temp = mailbox_->get<char>();
        LOG_INFO("[worker manager] my_host_name: %s receive request from master in mailbox:%s", my_host_name_.c_str(), mailbox_->get_cname());
        std::string info(temp);
        delete [] temp;
        int master_id;
        JobText job_text = StringToJobText(master_id, info);


        // 2. find a free worker responsible for this job
        int worker_id = FindFreeWorker();
        // workers_[worker_id] = std::make_shared<Worker>(my_host_name_, master_host_name_, worker_host_names_.size(),
        //                                                    worker_host_names_, worker_id, id_, bw_config_);
        workers_[worker_id]->SetMasterMailbox(master_id);
        workers_[worker_id]->SetJobText(job_text);
        // 3. notify worker id to master
        auto master_mailbox = simgrid::s4u::Mailbox::by_name(master_host_name_);
        LOG_INFO("[worker manager] host_name: %s choose worker id: %d, notify to mailbox: %s", my_host_name_.c_str(), worker_id, master_mailbox->get_cname());
        master_mailbox->put(new int(worker_id), 4);

        // 4. let this worker to do job(including map task, shuffle task and reduce task)
        workers_[worker_id]->DoJob();

        // 5. reset this worker
        mutex_.lock();
        workers_[worker_id]->SetWorkerState(WorkerState::DONE);
        mutex_.unlock();
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
            } else if(workers_[i]->GetWorkerState() == WorkerState::DONE) {
                // free it
                // worker_thds_[i].join();
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
    worker_partener_ids_.clear();
    for (int i = 0; i < worker_host_num_; i++) {
        worker_partener_ids_.push_back(worker_partener_ids_temp[i]);
    }
    delete[] worker_partener_ids_temp;

    switch(job_text_.type) {
        case JobType::TeraSort:
            TeraSort();
            break;
        case JobType::CodedTeraSort:
            CodedTeraSort();
            break;
    }
}

void Worker::Clear() {
    // clean
    // LOG_INFO("encodePreData.size: %ld", encodePreData.size());
    for(auto it0 : encodePreData) {
        for(auto it1 : it0.second) {
            for(auto it2 : it1.second) {
                delete [] it2.data;
            }
        }
    }
    encodePreData.clear();
    decodePreData.clear();

    delete coded_conf;
    delete cg;
    inputPartitionCollection.clear();
    encodeDataSend.clear();
    encodeDataRecv.clear();
    multicastGroupMap.clear();
    localLoadSet.clear();
    // LOG_INFO("partitionList size: %ld", partitionList.size());
    for(auto p : partitionList) {
        delete [] p;
    }
    partitionList.clear();
    partitionCollection.clear();
    partitionTxData.clear();
    partitionRxData.clear();
    // LOG_INFO("localList size: %ld", localList.size());
    for(auto p : localList) {
        delete [] p;
    }
    localList.clear();
    delete trie;
}


