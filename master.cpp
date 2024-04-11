#include "master.h"
#include "./tera_sort/InputSplitter.h"
#include "tera_sort/Utility.h"

/**
 * called when master node receive a job
 */
void MasterManager::Run(JobText& job_text) {
    assert(1 <= job_text.r && job_text.r <= worker_host_num_ - 1);
    // 1. find a free master, set its state to busy
    int master_id = FindFreeMaster();
    LOG_INFO("[master manager] choose id: %d to do job: %s", master_id, JobTextToString(master_id, job_text).c_str());
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
        LOG_INFO("[master manager] send job_text to mailbox: %s", mailbox->get_cname());
        Send(mailbox, bw_config_->GetBW(BWType::MAX), temp, info.size());

        // record worker_id
        int* worker_id_info = mailbox_->get<int>();
        worker_ids.push_back(*worker_id_info);
        LOG_INFO("[master] receive worker_id: %d from mailbox: %s", *worker_id_info, mailbox_->get_cname());
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
    LOG_INFO(
        "[master manager] split input file, input_file_num: %d, reducer_num: %d, r: %d, input_file_prefix: %s done",
        job_text.input_file_num, job_text.reducer_num, job_text.r, job_text.input_file_prefix.c_str());

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
    LOG_INFO("[master manager] choose id: %d to do job", master_id);
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
    LOG_INFO("[master] id: %d do job: %s", id_, JobTextToString(id_, job_text_).c_str());
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
            simgrid::s4u::Mailbox::by_name(worker_host_names_[i] + ":" + std::to_string(worker_ids[i])));
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


/**
 * init tasks_info_
*/
void Master::InitTaskSchedule(const PartitionList* partition_list) {
    unsigned char* left = new unsigned char [conf.getKeySize() + 1];
    memset(left, 0, conf.getKeySize() + 1);
    assert(partition_list->size() == conf.getNumReducer() - 1);
    for(int i = 1; i <= conf.getNumReducer(); i++) {
        TaskInfo task_info;
        task_info.file_ids ={i - 1};
        std::pair<unsigned char*, unsigned char*> partition;
        partition.first = new unsigned char [conf.getKeySize() + 1];
        memset(partition.first, 0, conf.getKeySize() + 1);
        partition.second = new unsigned char [conf.getKeySize() + 1];
        memset(partition.second, 0, conf.getKeySize() + 1);
        memcpy(partition.first, left, conf.getKeySize() + 1);
        if(i == conf.getNumReducer()) {
            memset(partition.second, 0xff, conf.getKeySize());
        } else {
            memcpy(partition.second, (*partition_list)[i - 1], conf.getKeySize() + 1);
        }
        memcpy(left, partition.second, conf.getKeySize() + 1);
        task_info.partitions.emplace_back(partition);
        tasks_info_[i] = task_info;
    }
    delete [] left;

    for(auto it: tasks_info_) {
        std::cout << "worker host id: " << it.first << " task info: " << std::endl;
        std::cout << "file id: {";
        for(auto file_id: it.second.file_ids) {
            std::cout << file_id << " ";
        }
        std::cout << "}" << std::endl;
        std::cout << "partitions: ";
        for(auto partition: it.second.partitions) {
            std::cout << "{";
            printKey(partition.first, conf.getKeySize());
            std::cout << ", ";
            printKey(partition.second, conf.getKeySize());
            std::cout << "}";
        }
        std::cout << std::endl;
    }
}

/**
 * select a job, route from src_id to dst_id
 * update task_info_(used when shuffle)
*/
void Master::TaskSchedule() {
    // 1. select src_id, dst_id according node computation load and network load
    int src_id = 1;
    int dst_id = 2;
    // MasterState task_type = MasterState::Mapping;
    MasterState task_type = MasterState::Reducing;

    // 2. update tasks_info_
    assert(1 <= src_id && src_id <= worker_host_num_ && 1 <= dst_id && dst_id <= worker_host_num_);
    assert(task_type == MasterState::Mapping || task_type == MasterState::Reducing);
    std::vector<int> file_ids;
    std::vector<std::pair<unsigned char*, unsigned char*>> partitions;
    if (task_type == MasterState::Mapping) {
        // route file in src node to dst node
        // dst node should exec map with this files
        // partition remains
        file_ids = tasks_info_[src_id].file_ids;
        for(auto file_id : file_ids) {
            tasks_info_[dst_id].file_ids.emplace_back(file_id);
        }
        tasks_info_[src_id].file_ids.clear();
    } else if (task_type == MasterState::Reducing) {
        // dst node should exec reduce on parition on dst node
        schedule_info_.reduce_schedule_info = {src_id, dst_id};
    } 
}


/**
 * schedule map task to workers according to tasks_info
 * send map task to worker
 * if no map task need to send to a worker, send a empty command
*/
void Master::MapSchedule() {
    for(int i = 1; i <= conf.getNumReducer(); i++) {
        Command command;
        command.type = CommandType::Map;
        command.file_ids = tasks_info_[i].file_ids;
        int command_size;
        char* command_temp = SerializeCommand(command, command_size);
        LOG_INFO("[master] send map task to worker: %d, task info: %s", i, command_temp);
        Send(worker_mailboxs_[i - 1], bw_config_->GetBW(BWType::MAX), command_temp, command_size);
    }
}


/**
 * schedule shuffle
 * send command to worker to send data
 * if map task scheduled from src node to dst node,  ommit src node
 * If reduce task scheduled from src node to dst node, 
 * notify to all worker thatdata needed to be routed to src node should routed to dst node
*/
void Master::ShuffleSchedule() {
    std::pair<int, int> reduce_schedule_info = schedule_info_.reduce_schedule_info;
    for(int i = 1; i <= conf.getNumReducer(); i++) {            // node i send, other node receive
        if(tasks_info_[i].file_ids.empty()) {                   // node i map task is schedule to other node, nothing to send
            continue;
        }
        for(int j = 1; j <= conf.getNumReducer(); j++) {        // send command to worker j
            Command command;
            if(j == i) {                                        // worker i send data to other worker
                command.type = CommandType::Send;
                command.reduce_schedule_info = schedule_info_.reduce_schedule_info;
            } else if (j == schedule_info_.reduce_schedule_info.first) {   // omit
                command.type = CommandType::Omit;
            } else {                                            // receive data from node i
                command.type = CommandType::Receive;
                command.receive_id = i;
            }
            int command_size;
            char* command_temp = SerializeCommand(command, command_size);
            Send(worker_mailboxs_[j - 1], bw_config_->GetBW(BWType::MAX), command_temp, command_size);
        }
        // wait for all worker send/receive done
        for(int j = 1; j <= conf.getNumReducer(); j++) {
            delete mailbox_->get<unsigned char>();
        }
    }

    // end of shuffle, send end command to all workers
    for(int i = 1; i <= conf.getNumReducer(); i++) {
        Command command;
        command.type = CommandType::End;
        int command_size;
        char* command_temp = SerializeCommand(command, command_size);
        Send(worker_mailboxs_[i - 1], bw_config_->GetBW(BWType::MAX), command_temp, command_size);
    }
}

void Master::ReduceSchedule() {

}