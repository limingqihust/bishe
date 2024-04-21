#include "worker.h"

/**
 * receive request from master, find a free worker to deal with map task and reduce task of this job
*/
void WorkerManager::Run() {
    while (true) {
        // 1. receive a request from master
        LOG_INFO("[worker manager] my_host_name_prefix: %s wait for request from master manager", my_host_name_prefix_.c_str());
        char* temp = mailbox_->get<char>();
        LOG_INFO("[worker manager] my_host_name_prefix: %s receive request from master manager", my_host_name_prefix_.c_str());
        std::string info(temp);


        // 2. find a free worker responsible for this job
        int worker_id = FindFreeWorker();

        // 3. notify worker id to master manager
        Send(master_host_mailbox_, bw_config_->GetBW(BWType::MAX), new int(worker_id), sizeof(int));
        LOG_INFO("[worker manager] host_name: %s choose worker id: %d, notify to master manager", my_host_name_prefix_.c_str(), worker_id);
        
        // 4. let this worker to do job(including map task, shuffle task and reduce task)
        //    assign master_id and job_text to worker
        auto mailbox = simgrid::s4u::Mailbox::by_name(my_host_name_prefix_ + ":" + std::to_string(worker_id));
        Send(mailbox, bw_config_->GetBW(BWType::MAX), temp, info.size());  
    }
}

int WorkerManager::FindFreeWorker() {
    while (true) {
        master_worker_state_->mutex.lock();
        for (int i = 1; i <= worker_num_; i++) {
            assert(master_worker_state_->worker_state.find(id_) != master_worker_state_->worker_state.end());
            assert(master_worker_state_->worker_state[id_].find(i) != master_worker_state_->worker_state[id_].end());
            if (master_worker_state_->worker_state[id_][i] == State::Free) {
                master_worker_state_->worker_state[id_][i] = State::Busy;
                master_worker_state_->mutex.unlock();
                return i;
            }
        }
        master_worker_state_->mutex.unlock();
        sleep(5);
    }
}

void Worker::Run() {
    while(true) {
        char* temp = mailbox_->get<char>();
        std::string info(temp);
        LOG_INFO("[worker] my_host_name_prefix: %s, id: %d, receive job: %s", my_host_name_prefix_.c_str(), id_, info.c_str());
        delete [] temp;
        int master_id;
        JobText job_text = StringToJobText(master_id, info);
        SetMasterMailbox(master_id);
        SetJobText(job_text);
        
        // receive worker_partner_ids
        LOG_INFO("[worker] my_host_name_prefix: %s, id: %d, wait for worker partner ids", my_host_name_prefix_.c_str(), id_);
        int* worker_partener_ids_temp = mailbox_->get<int>();
        worker_partener_ids_.clear();
        for (int i = 0; i < worker_host_num_; i++) {
            worker_partener_ids_.push_back(worker_partener_ids_temp[i]);
            LOG_INFO("[worker] my_host_name_prefix: %s, id: %d recieve worker partner id: %d", my_host_name_prefix_.c_str(), id_, worker_partener_ids_temp[i]);
        }
        delete[] worker_partener_ids_temp;

        switch(job_text_.type) {
        case JobType::TeraSort:
            TeraSort();
            break;
        case JobType::CodedTeraSort:
            CodedTeraSort();
            break;

        // reset worker
        master_worker_state_->mutex.lock();
        assert(master_worker_state_->worker_state.find(host_id_) != master_worker_state_->worker_state.end());
        assert(master_worker_state_->worker_state[host_id_].find(id_) != master_worker_state_->worker_state[host_id_].end());
        master_worker_state_->worker_state[host_id_][id_] = State::Free;
        master_worker_state_->mutex.unlock();
    }

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


