#include "master.h"
#include "./tera_sort/InputSplitter.h"

/**
 * called when master node receive a job
 */
void MasterManager::Run(JobText& job_text) {
    assert(1 <= job_text.r && job_text.r <= worker_host_num_ - 1);
    // 1. find a free master, set its state to busy
    int master_id = FindFreeMaster();
    LOG_INFO("[master manager] choose id: %d to do job: %s", master_id, JobTextToString(master_id, job_text).c_str());

    // 2. Split file
    job_text.input_file_num = GetJobTextInputFileNum(job_text);
    SplitInput(job_text);

    // 3. send request to worker manager
    std::vector<int> worker_ids = RequestWorkerIds(master_id, job_text);

    // 4. assign job to master
    // TODO:
    const std::string info = JobTextToString(master_id, job_text);
    char* temp = new char[info.size()];
    strcpy(temp, info.c_str());
    auto mailbox = simgrid::s4u::Mailbox::by_name(my_host_name_prefix_ + ":" + std::to_string(master_id));
    Send(mailbox, bw_config_->GetBW(BWType::MAX), temp, info.size());
    int* worker_ids_temp = new int[worker_host_num_];
    for (int i = 0; i < worker_host_num_; i++) {
        worker_ids_temp[i] = worker_ids[i];
    }
    Send(mailbox, bw_config_->GetBW(BWType::MAX), worker_ids_temp, worker_host_num_ * sizeof(int));
    LOG_INFO("[master manager] route job: %s and worker ids to master id: %d", info.c_str(), master_id);

    // 4. reset this worker
}

/**
 * 当收到一个作业时被调用
 * 寻找一个空闲的Master负责这个作业的调度工作
*/
int MasterManager::FindFreeMaster() {
    while (true) {
        master_worker_state_->mutex.lock();
        for (int i = 1; i <= master_num_; i++) {
            assert(master_worker_state_->master_state.find(i) != master_worker_state_->master_state.end());
            if (master_worker_state_->master_state[i] == State::Free) {
                master_worker_state_->master_state[i] = State::Busy;
                master_worker_state_->mutex.unlock();
                return i;
            }
        }
        master_worker_state_->mutex.unlock();
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
        LOG_INFO("[master manager] send job_text to worker manager: %s", mailbox->get_cname());
        Send(mailbox, bw_config_->GetBW(BWType::MAX), temp, info.size());

        // record worker_id
        LOG_INFO("[master manager] wait for worker id from worker manager: %s by mailbox: %s", mailbox->get_cname(),
                 mailbox_->get_cname());
        int* worker_id_info = mailbox_->get<int>();
        worker_ids.push_back(*worker_id_info);
        LOG_INFO("[master manager] receive worker_id: %d from worker manager", *worker_id_info, mailbox->get_cname());
        delete worker_id_info;
    }
    return worker_ids;
}

void MasterManager::SplitInput(const JobText& job_text) {
    Configuration* conf;
    switch (job_text.type) {
        case JobType::TeraSort:
            conf =
                new Configuration(job_text.reducer_num, job_text.reducer_num, job_text.r, job_text.input_file_prefix);
            break;
        case JobType::CodedTeraSort:
            conf = new CodedConfiguration(job_text.input_file_num, job_text.reducer_num, job_text.r,
                                          job_text.input_file_prefix);
            break;
    }
    InputSplitter input_splitter;
    input_splitter.setConfiguration(conf);
    input_splitter.splitInputFile();
    LOG_INFO(
        "[master manager] split input file, input_file_num: %d, reducer_num: %d, r: %d, input_file_prefix: %s done",
        job_text.input_file_num, job_text.reducer_num, job_text.r, job_text.input_file_prefix.c_str());

    delete conf;
}

/**
 * modify parameter r of coded-terasort
*/
void MasterManager::SetR(int r) {
    // for (int i = 0; i < master_num_; i++) {
    //     masters_[i]->SetR(r);
    // }
}

/**
 * do micro experient with parameter r, return UtilityInfo
*/
UtilityInfo MasterManager::RunTryR(JobText& job, int r) {
    // assert(1 <= r && r <= worker_host_num_ - 1);
    // // 1. find a free master, set its state to busy
    // int master_id = FindFreeMaster();
    // LOG_INFO("[master manager] choose id: %d to do job", master_id);
    // // 2. assign job to this master
    // job.r = r;
    // job.input_file_num = GetJobTextInputFileNum(job);
    // // masters_[master_id]->SetJobText(job);

    // // 3. split file, generate input files
    // SplitInput(job);

    // // 4. let master to this job(return directly, free resources when job done automatically)
    // UtilityInfo res = masters_[master_id]->DoJobTryR(RequestWorkerIds(master_id, job), r);

    // // 5. reset this worker
    // mutex_.lock();
    // masters_[master_id]->SetMasterState(MasterState::Done);
    // mutex_.unlock();

    // return res;
}

void Master::Run() {
    while (true) {
        // receive job text from master manager
        char* temp = mailbox_->get<char>();
        std::string info(temp);
        LOG_INFO("[master] id: %d, receive job: %s", id_, info.c_str());
        delete[] temp;
        int master_id;
        JobText job_text = StringToJobText(master_id, info);
        SetJobText(job_text);

        // receive worker ids from master manager
        std::vector<int> worker_ids;
        int* worker_partener_ids_temp = mailbox_->get<int>();
        for (int i = 0; i < worker_host_num_; i++) {
            worker_ids.push_back(worker_partener_ids_temp[i]);
        }
        delete[] worker_partener_ids_temp;

        // init worker mailbox
        assert(worker_ids.size() == worker_host_num_);
        worker_mailboxs_.clear();
        for (int i = 0; i < worker_host_num_; i++) {
            worker_mailboxs_.push_back(
                simgrid::s4u::Mailbox::by_name(worker_host_name_prefixs_[i] + ":" + std::to_string(worker_ids[i])));
        }

        // notify worker_ids to all worker
        for (auto mailbox : worker_mailboxs_) {
            int* worker_ids_temp = new int[worker_host_num_];
            for (int i = 0; i < worker_host_num_; i++) {
                worker_ids_temp[i] = worker_ids[i];
            }
            LOG_INFO("[master] id: %d send worker ids to mailbox: %s", id_, mailbox->get_cname());
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

        // reset master
        master_worker_state_->mutex.lock();
        assert(master_worker_state_->master_state.find(id_) != master_worker_state_->master_state.end());
        master_worker_state_->master_state[id_] = State::Free;
        master_worker_state_->mutex.unlock();
        LOG_INFO("[master] id: %d reset", id_);
    }
}

/**
 * 被MasterManager调用
 * 1. 接收一个Job
 * 2. 向WorkerManager请求Worker
 * 3. 处理Worker的Task请求
*/
void Master::DoJob(const std::vector<int>& worker_ids) {
    LOG_INFO("[master] id: %d do job: %s", id_, JobTextToString(id_, job_text_).c_str());
    assert(worker_ids.size() == worker_host_num_);
    worker_mailboxs_.clear();
    for (int i = 0; i < worker_host_num_; i++) {
        worker_mailboxs_.push_back(
            simgrid::s4u::Mailbox::by_name(worker_host_name_prefixs_[i] + ":" + std::to_string(worker_ids[i])));
    }

    // notify worker_ids to all worker
    for (auto mailbox : worker_mailboxs_) {
        int* worker_ids_temp = new int[worker_host_num_];
        for (int i = 0; i < worker_host_num_; i++) {
            worker_ids_temp[i] = worker_ids[i];
        }
        LOG_INFO("[master] id: %d send worker ids to mailbox: %s", id_, mailbox->get_cname());
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
    LOG_INFO("[master] id: %d do job: %s try r: %d", id_, JobTextToString(id_, job_text_).c_str(), r);
    assert(worker_ids.size() == worker_host_num_);
    UtilityInfo res;
    worker_mailboxs_.clear();
    for (int i = 0; i < worker_host_num_; i++) {
        worker_mailboxs_.push_back(
            simgrid::s4u::Mailbox::by_name(worker_host_name_prefixs_[i] + ":" + std::to_string(worker_ids[i])));
    }

    // notify worker_ids to all worker
    for (auto mailbox : worker_mailboxs_) {
        int* worker_ids_temp = new int[worker_host_num_];
        for (int i = 0; i < worker_host_num_; i++) {
            worker_ids_temp[i] = worker_ids[i];
        }
        LOG_INFO("[master] id: %d send worker ids to mailbox: %s", id_, mailbox->get_cname());
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