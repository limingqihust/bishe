#include "master.h"
#include "./tera_sort/InputSplitter.h"

/**
 * called when master node receive a job
 */
void MasterManager::Run(JobText& job_text) {
    assert(1 <= job_text.r && job_text.r <= worker_host_num_ - 1);
    // 1. find a free master, set its state to busy
    int master_id = FindFreeMaster();
    // LOG_INFO("[master manager] choose id: %d to do job: %s", master_id, JobTextToString(master_id, job_text).c_str());
    // 2. assign job to this master
    job_text.input_file_num = GetJobTextInputFileNum(job_text);
    masters_[master_id]->SetJobText(job_text);

    // 3. split file, generate input files
    SplitInput(job_text);

    // 4. let master to this job(return directly, free resources when job done automatically)
    std::vector<int> master_ids = RequestWorkerIds(master_id, job_text);

    masters_[master_id]->DoJob(master_ids);

    // 5. reset this worker
    mutex_.lock();
    masters_[master_id]->SetMasterState(MasterState::Done);
    mutex_.unlock();
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
            } else if (masters_[i]->GetMasterState() == MasterState::Done) {
                // master_thds_[i].join();
                masters_[i]->SetMasterState(MasterState::Mapping);
                mutex_.unlock();
                return i;
            }
        }
        mutex_.unlock();
        sleep(5);
    }
}

std::vector<int> MasterManager::RequestWorkerIds(int master_id, const JobText& job_text) {
    std::vector<int> worker_ids;
    for (int i = 0; i < worker_host_num_; i++) {
        auto mailbox = worker_host_mailboxs_[i];
        // send master_id and job_text to worker manager, meaning a request to worker manager and notify master_id, job_text to it
        const std::string info = JobTextToString(master_id, job_text);
        char* temp = new char[info.size()];
        strcpy(temp, info.c_str());
        // LOG_INFO("[master manager] send job_text to mailbox: %s", mailbox->get_cname());
        Send(mailbox, bw_config_->GetBW(BWType::MAX), temp, info.size());

        // record worker_id
        int* worker_id_info = mailbox_->get<int>();
        worker_ids.push_back(*worker_id_info);
        // LOG_INFO("[master] receive worker_id: %d from mailbox: %s", *worker_id_info, mailbox_->get_cname());
        delete worker_id_info;
    }
    return worker_ids;
}

void MasterManager::SplitInput(const JobText& job_text) {
    Configuration* conf;
    switch (job_text.type) {
        case JobType::TeraSort:
            conf = new Configuration(job_text.reducer_num, job_text.reducer_num, job_text.r,
                                     job_text.input_file_prefix);
            break;
        case JobType::CodedTeraSort:
            conf = new CodedConfiguration(job_text.input_file_num, job_text.reducer_num, job_text.r,
                                          job_text.input_file_prefix);
            break;
    }
    InputSplitter input_splitter;
    input_splitter.setConfiguration(conf);
    input_splitter.splitInputFile();
    // LOG_INFO(
    //     "[master manager] split input file, input_file_num: %d, reducer_num: %d, r: %d, input_file_prefix: %s done",
    //     job_text.input_file_num, job_text.reducer_num, job_text.r, job_text.input_file_prefix.c_str());

    delete conf;
}

/**
 * modify parameter r of coded-terasort
*/
void MasterManager::SetR(int r) {
    for (int i = 0; i < master_num_; i++) {
        masters_[i]->SetR(r);
    }
}

/**
 * do micro experient with parameter r, return UtilityInfo
*/
UtilityInfo MasterManager::RunTryR(JobText& job, int r) {
    assert(1 <= r && r <= worker_host_num_ - 1);
    // 1. find a free master, set its state to busy
    int master_id = FindFreeMaster();
    // LOG_INFO("[master manager] choose id: %d to do job", master_id);
    // 2. assign job to this master
    job.r = r;
    job.input_file_num = GetJobTextInputFileNum(job);
    masters_[master_id]->SetJobText(job);

    // 3. split file, generate input files
    SplitInput(job);

    // 4. let master to this job(return directly, free resources when job done automatically)
    UtilityInfo res = masters_[master_id]->DoJobTryR(RequestWorkerIds(master_id, job), r);

    // 5. reset this worker
    mutex_.lock();
    masters_[master_id]->SetMasterState(MasterState::Done);
    mutex_.unlock();

    return res;
}

/**
 * 被MasterManager调用
 * 1. 接收一个Job
 * 2. 向WorkerManager请求Worker
 * 3. 处理Worker的Task请求
*/
void Master::DoJob(const std::vector<int>& worker_ids) {
    // LOG_INFO("[master] id: %d do job: %s", id_, JobTextToString(id_, job_text_).c_str());
    assert(worker_ids.size() == worker_host_num_);
    worker_mailboxs_.clear();
    for (int i = 0; i < worker_host_num_; i++) {
        worker_mailboxs_.push_back(
            simgrid::s4u::Mailbox::by_name(worker_host_names_[i] + ":" + std::to_string(worker_ids[i])));
    }

    // notify worker_ids to all worker
    for (auto mailbox : worker_mailboxs_) {
        int* worker_ids_temp = new int[worker_host_num_];
        for (int i = 0; i < worker_host_num_; i++) {
            worker_ids_temp[i] = worker_ids[i];
        }
        // LOG_INFO("[master] id: %d send worker ids to mailbox: %s", id_, mailbox->get_cname());
        mailbox->put(worker_ids_temp, worker_host_num_ * sizeof(int));
    }

    // 2. do job
    switch (job_text_.type) {
        case JobType::TeraSort:
            TeraSort();
            break;
        case JobType::CodedTeraSort:
            CodedTeraSort();
            break;
    }
}

/**
 * exec job with parameter r, return utility_info
*/
UtilityInfo Master::DoJobTryR(std::vector<int> worker_ids, int r) {
    // LOG_INFO("[master] id: %d do job: %s try r: %d", id_, JobTextToString(id_, job_text_).c_str(), r);
    assert(worker_ids.size() == worker_host_num_);
    UtilityInfo res;
    worker_mailboxs_.clear();
    for (int i = 0; i < worker_host_num_; i++) {
        worker_mailboxs_.push_back(
            simgrid::s4u::Mailbox::by_name(worker_host_names_[i] + ":" + std::to_string(worker_ids[i])));
    }

    // notify worker_ids to all worker
    for (auto mailbox : worker_mailboxs_) {
        int* worker_ids_temp = new int[worker_host_num_];
        for (int i = 0; i < worker_host_num_; i++) {
            worker_ids_temp[i] = worker_ids[i];
        }
        // LOG_INFO("[master] id: %d send worker ids to mailbox: %s", id_, mailbox->get_cname());
        mailbox->put(worker_ids_temp, worker_host_num_ * sizeof(int));
    }

    // 2. do job
    switch (job_text_.type) {
        case JobType::TeraSort:
            res = TeraSort();
            break;
        case JobType::CodedTeraSort:
            res = CodedTeraSort();
            break;
    }
    return res;
}