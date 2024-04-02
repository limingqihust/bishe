#include "master.h"

/**
 * called when master node receive a job
 */
void MasterManager::Run(MasterJobText job_text) {
    // 1. find a free master, set its state to busy
    int master_id = FindFreeMaster();
    LOG_INFO("[master manager] choose id: %d to do job", master_id);
    // 2. assign job to this master
    masters_[master_id]->SetJobText(job_text);

    // 3. let master to this job(return directly, free resources when job done automatically)
    masters_[master_id]->DoJob(RequestWorkerIds(master_id));
}

/**
 * 当收到一个作业时被调用
 * 寻找一个空闲的Master负责这个作业的调度工作
*/
int MasterManager::FindFreeMaster() {
    while (true) {
        mutex_.lock();
        for (int i = 0; i < master_num_; i++) {
            if (masters_[i]->GetMasterState() == MasterState::Free) {
                masters_[i]->SetMasterState(MasterState::Mapping);
                mutex_.unlock();
                return i;
            }
        }
        mutex_.unlock();
        sleep(5);
    }
}

std::vector<int> MasterManager::RequestWorkerIds(int master_id) {
    std::vector<int> worker_ids;
    for(int i = 0; i < worker_host_num_; i++) {
        auto mailbox = worker_host_mailboxs_[i];
        // send master_id to worker manager, meaning a request to worker manager and notify master_id to it
        mailbox->put(new int(master_id), 4);
        
        // record worker_id
        int* worker_id_info = mailbox_->get<int>();
        worker_ids.push_back(*worker_id_info);
        delete worker_id_info;
    }
    return worker_ids;
}

/**
 * modify parameter r of coded-terasort
*/
void MasterManager::SetR(int r) {
    for(int i = 0; i < master_num_; i++) {
        masters_[i]->SetR(r);
    }
}

/**
 * do micro experient with parameter r, return UtilityInfo
*/
UtilityInfo MasterManager::RunTryR(MasterJobText job, int r) {
    // 1. find a free master, set its state to busy
    int master_id = FindFreeMaster();
    LOG_INFO("[master manager] choose id: %d to do job", master_id);
    // 2. assign job to this master
    masters_[master_id]->SetJobText(job);

    // 3. let master to this job(return directly, free resources when job done automatically)
    return masters_[master_id]->DoJobTryR(RequestWorkerIds(master_id), r);

    return {-1, -1, -1};
}


/**
 * 被MasterManager调用
 * 1. 接收一个Job
 * 2. 向WorkerManager请求Worker
 * 3. 处理Worker的Task请求
*/
void Master::DoJob(std::vector<int> worker_ids) {
    assert(worker_ids.size() == worker_host_num_);
    for(int i = 0; i < worker_host_num_; i++) {
        worker_mailboxs_.push_back(simgrid::s4u::Mailbox::by_name(worker_host_names_[i] + ":" + std::to_string(worker_ids[i])));
    }

    // notify worker_ids to all worker
    for(auto mailbox : worker_mailboxs_) {
        int* worker_ids_temp = new int [worker_host_num_];
        for(int i = 0; i < worker_host_num_; i++) {
            worker_ids_temp[i] = worker_ids[i];
        }
        mailbox->put(worker_ids_temp, worker_host_num_ * 4);
    }

    // 2. do job
    // TeraSort();
    CodedTeraSort();
}

/**
 * exec job with parameter r, return utility_info
*/
UtilityInfo Master::DoJobTryR(std::vector<int> worker_ids, int r) {
    for(int i = 0; i < worker_host_num_; i++) {
        worker_mailboxs_.push_back(simgrid::s4u::Mailbox::by_name(worker_host_names_[i] + ":" + std::to_string(worker_ids[i])));
    }

    // 1. notify worker_ids to all worker
    for(auto mailbox : worker_mailboxs_) {
        int* worker_ids_temp = new int [worker_host_num_];
        for(int i = 0; i < worker_host_num_; i++) {
            worker_ids_temp[i] = worker_ids[i];
        }
        mailbox->put(worker_ids_temp, worker_host_num_ * 4);
    }

    // 2. do job
    // TeraSort();
    CodedTeraSort();

    return {-1, -1, -1};
}