#include "master.h"

/**
 * called when master node receive a job
 */
void MasterManager::Run(MasterJobText job_text) {
    // 1. find a free master, set its state to busy
    int master_id = FindFreeMaster();

    // 2. assign job to this master
    masters_[master_id]->SetJobText(job_text);

    // 3. let master to this job(return directly, free resources when job done automatically)
    masters_[master_id]->DoJob();
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
                LOG_INFO("[master] master manager choose master %d to do this job", i);
                return i;
            }
        }
        mutex_.unlock();
        sleep(5);
    }
}

/**
 * 被MasterManager调用
 * 1. 接收一个Job
 * 2. 向WorkerManager请求Worker
 * 3. 处理Worker的Task请求
*/
void Master::DoJob() {
    // 1. request workers, notify master_id to it, collect worker_id
    for(int i = 0; i < worker_host_num_; i++) {
        auto mailbox = worker_mailboxs_[i];
        // send master_id to worker manager, meaning a request to worker manager and notify master_id to it
        mailbox->put(new int(id_), 4);
        
        // record worker_id
        LOG_INFO("[master] id: %d wait worker_id_info from %s", id_, mailbox_->get_cname());
        int* worker_id_info = mailbox_->get<int>();
        LOG_INFO("[master] master %d receive worker_info %s", id_, (worker_host_names_[i] + ":" + std::to_string(*worker_id_info)).c_str());
        worker_mailboxs_.push_back(simgrid::s4u::Mailbox::by_name(worker_host_names_[i] + ":" + std::to_string(*worker_id_info)));
        delete worker_id_info;
    }
    // 2. do job
    TeraSort();
}