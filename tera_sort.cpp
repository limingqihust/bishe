#include "master.h"
#include "worker.h"

UtilityInfo Master::TeraSort() {
    // LOG_INFO("[master] master_id: %d, TeraSort start", id_);
    UtilityInfo res;
    // GENERATE LIST OF PARTITIONS.
    PartitionSampling partitioner;
    partitioner.setConfiguration(&conf);
    PartitionList* partitionList = partitioner.createPartitions();
    unsigned long int i = 0;
    // BROADCAST CONFIGURATION TO WORKERS
    /* omit temporary*/

    // BROADCAST PARTITIONS TO WORKERS
    assert(partitionList->size() == conf.getNumReducer() - 1);
    for (auto it = partitionList->begin(); it != partitionList->end(); it++) {
        // unsigned char* partition = *it;
        // MPI_Bcast( partition, conf.getKeySize() + 1, MPI_UNSIGNED_CHAR, 0, MPI_COMM_WORLD );
        for (auto worker_mailbox : worker_mailboxs_) {
            unsigned char* partition = new unsigned char[conf.getKeySize() + 1];
            memcpy(partition, *it, conf.getKeySize() + 1);
            Send(worker_mailbox, bw_config_->GetBW(BWType::M_W), partition, conf.getKeySize() + 1);
        }
    }

    // COMPUTE MAP TIME
    int numWorker = conf.getNumReducer();
    double rTime = 0;
    double avgTime = 0;
    double maxTime = 0;
    for (int i = 1; i <= numWorker; i++) {
        double* time = mailbox_->get<double>();
        // LOG_INFO("[master] receive map time: %lf", *time);
        avgTime += *time;
        maxTime = max(maxTime, *time);
        delete time;
    }
    res.computation_load = avgTime / numWorker;
    // std::cout << "[master] MAP     | Avg = " << setw(10) << avgTime / numWorker << "   Max = " << setw(10) << maxTime
    //           << endl;
    std::cout << "MAP " << avgTime / numWorker << endl;
    assert(worker_mailboxs_.size() == numWorker);
    for (auto mailbox : worker_mailboxs_) {
        Send(mailbox, bw_config_->GetBW(BWType::MAX), new unsigned char, sizeof(unsigned char));
    }

    // COMPUTE PACKING TIME
    avgTime = 0;
    maxTime = 0;
    for (int i = 1; i <= numWorker; i++) {
        double* time = mailbox_->get<double>();
        // LOG_INFO("[master] receive pack time: %lf", *time);
        avgTime += *time;
        maxTime = max(maxTime, *time);
        delete time;
    }
    // std::cout << "[master] PACK    | Avg = " << setw(10) << avgTime / numWorker << "   Max = " << setw(10) << maxTime
    //           << endl;
    for (auto mailbox : worker_mailboxs_) {
        Send(mailbox, bw_config_->GetBW(BWType::MAX), new unsigned char, sizeof(unsigned char));
    }

    // COMPUTE SHUFFLE TIME
    avgTime = 0;
    maxTime = 0;
    for (auto mailbox : worker_mailboxs_) {
        // notify worker i to send data
        Send(mailbox, bw_config_->GetBW(BWType::MAX), new unsigned char, sizeof(unsigned char));
        // wait for all worker receive data done
        for (int i = 0; i < worker_host_num_ - 1; i++) {
            delete mailbox_->get<unsigned char>();
        }
    }
    avgTime = 0;
    maxTime = 0;
    for (int i = 1; i <= numWorker; i++) {
        double* time = mailbox_->get<double>();
        // LOG_INFO("[master] receive pack time: %lf", *time);
        avgTime += *time;
        maxTime = max(maxTime, *time);
        delete time;
    }
    res.network_load = avgTime / numWorker;
    // std::cout << "[master] SHUFFLE    | Avg = " << setw(10) << avgTime / numWorker << "   Max = " << setw(10) << maxTime
    //           << endl;
    std::cout << "SHUFFLE " << avgTime / numWorker << endl;

    for (auto mailbox : worker_mailboxs_) {
        Send(mailbox, bw_config_->GetBW(BWType::MAX), new unsigned char, sizeof(unsigned char));
    }

    // COMPUTE UNPACK TIME
    avgTime = 0;
    maxTime = 0;
    for (int i = 1; i <= numWorker; i++) {
        double* time = mailbox_->get<double>();
        // LOG_INFO("[master] receive unpack time: %lf", *time);
        avgTime += *time;
        maxTime = max(maxTime, *time);
        delete time;
    }
    // std::cout << "[master] UNPACK    | Avg = " << setw(10) << avgTime / numWorker << "   Max = " << setw(10) << maxTime
    //           << endl;
    for (auto mailbox : worker_mailboxs_) {
        Send(mailbox, bw_config_->GetBW(BWType::MAX), new unsigned char, sizeof(unsigned char));
    }

    // COMPUTE REDUCE TIME
    avgTime = 0;
    maxTime = 0;
    for (int i = 1; i <= numWorker; i++) {
        double* time = mailbox_->get<double>();
        // LOG_INFO("[master] receive reduce time: %lf", *time);
        avgTime += *time;
        maxTime = max(maxTime, *time);
        delete time;
    }
    // std::cout << "[master] REDUCE    | Avg = " << setw(10) << avgTime / numWorker << "   Max = " << setw(10) << maxTime
    //           << endl;
    for (auto mailbox : worker_mailboxs_) {
        Send(mailbox, bw_config_->GetBW(BWType::MAX), new unsigned char, sizeof(unsigned char));
    }

    // return UtilityInfo to master manager
    return res;
}

void Worker::TeraSort() {
    // LOG_INFO(
    //     "[worker] my_host_name: %s, id: %d, TeraSort start, input_file_num: %d, reducer_num: %d, r: %d, "
    //     "intpuf_file_prefix: %s",
    //     my_host_name_.c_str(), id_, job_text_.input_file_num, job_text_.reducer_num, job_text_.r,
    //     job_text_.input_file_prefix.c_str());  // RECEIVE CONFIGURATION FROM MASTER
    // conf = new Configuration;
    conf = new Configuration(job_text_.input_file_num, job_text_.reducer_num, job_text_.r, job_text_.input_file_prefix);
    // MPI_Bcast( (void*) conf, sizeof( Configuration ), MPI_CHAR, 0, MPI_COMM_WORLD );

    // RECEIVE PARTITIONS FROM MASTER
    for (unsigned int i = 1; i < conf->getNumReducer(); i++) {
        // unsigned char* buff = new unsigned char[ conf->getKeySize() + 1 ];
        // MPI_Bcast( buff, conf->getKeySize() + 1, MPI_UNSIGNED_CHAR, 0, MPI_COMM_WORLD );
        unsigned char* buff = mailbox_->get<unsigned char>();
        partitionList.push_back(buff);
    }
    ExecMap();

    // SHUFFLING PHASE
    unsigned int lineSize = conf->getLineSize();
    double s4u_start = simgrid::s4u::Engine::get_clock();
    auto shuffle_start = std::chrono::high_resolution_clock::now();
    for (unsigned int i = 1; i <= conf->getNumReducer(); i++) {
        if (i == host_id_) {  // should send to other worker
            // wait for master permission to send data
            delete mailbox_->get<unsigned char>();
            // Sending from node i
            for (unsigned int j = 1; j <= conf->getNumReducer(); j++) {
                if (j == i) {
                    continue;
                }
                TxData& txData = partitionTxData[j - 1];
                auto mailbox = simgrid::s4u::Mailbox::by_name(worker_host_names_[j - 1] + ":" +
                                                              std::to_string(worker_partener_ids_[j - 1]));
                Send(mailbox, bw_config_->GetBW(BWType::W_W), new int(txData.numLine), sizeof(int));
                unsigned char* data_temp = new unsigned char[txData.numLine * lineSize];
                memcpy(data_temp, txData.data, txData.numLine * lineSize);
                Send(mailbox, bw_config_->GetBW(BWType::W_W), data_temp, txData.numLine * lineSize);
                delete[] txData.data;
                // MPI_Send( &( txData.numLine ), 1, MPI_UNSIGNED_LONG_LONG, j, 0, MPI_COMM_WORLD );
                // MPI_Send( txData.data, txData.numLine * lineSize, MPI_UNSIGNED_CHAR, j, 0, MPI_COMM_WORLD );
            }

        } else {  // receive data from worker i
            TxData& rxData = partitionRxData[i - 1];
            int* len_temp = mailbox_->get<int>();
            rxData.numLine = *len_temp;
            delete len_temp;
            rxData.data = new unsigned char[rxData.numLine * lineSize];
            unsigned char* data_temp = mailbox_->get<unsigned char>();
            memcpy(rxData.data, data_temp, rxData.numLine * lineSize);
            delete[] data_temp;
            // receive data from worker i done, notify master that i am done
            Send(master_mailbox_, bw_config_->GetBW(BWType::MAX), new unsigned char, sizeof(unsigned char));
        }
    }
    double s4u_end = simgrid::s4u::Engine::get_clock();
    simgrid::s4u::this_actor::execute(s4u_end - s4u_start);
    auto shuffle_end = std::chrono::high_resolution_clock::now();
    // auto shuffle_time = std::chrono::duration_cast<std::chrono::duration<double>>(shuffle_end - shuffle_start).count();
    auto shuffle_time = s4u_end - s4u_start;
    Send(master_mailbox_, bw_config_->GetBW(BWType::M_W), new double(shuffle_time), sizeof(double));
    delete mailbox_->get<unsigned char>();

    // UNPACK PHASE
    auto unpack_start = std::chrono::high_resolution_clock::now();
    // append local partition to localList
    for (auto it = partitionCollection[host_id_ - 1]->begin(); it != partitionCollection[host_id_ - 1]->end(); ++it) {
        unsigned char* buff = new unsigned char[conf->getLineSize()];
        memcpy(buff, *it, conf->getLineSize());
        localList.push_back(buff);
    }

    // append data from other workers
    for (unsigned int i = 1; i <= conf->getNumReducer(); i++) {
        if (i == host_id_) {
            continue;
        }
        TxData& rxData = partitionRxData[i - 1];
        for (unsigned long long lc = 0; lc < rxData.numLine; lc++) {
            unsigned char* buff = new unsigned char[lineSize];
            memcpy(buff, rxData.data + lc * lineSize, lineSize);
            localList.push_back(buff);
        }
        delete[] rxData.data;
    }
    // PrintLocalList();
    auto unpack_end = std::chrono::high_resolution_clock::now();
    auto rTime = std::chrono::duration_cast<std::chrono::duration<double>>(unpack_end - unpack_start).count();
    // LOG_INFO("[worker] my_host_name: %s, id: %d send unpack time %lf to master", my_host_name_.c_str(), id_, rTime);

    Send(master_mailbox_, bw_config_->GetBW(BWType::M_W), new double(rTime), sizeof(double));
    delete mailbox_->get<unsigned char>();

    // REDUCE PHASE
    auto reduce_start = std::chrono::high_resolution_clock::now();
    ExecReduce();
    auto reduce_end = std::chrono::high_resolution_clock::now();
    rTime = std::chrono::duration_cast<std::chrono::duration<double>>(reduce_end - reduce_start).count();
    // LOG_INFO("[worker] my_host_name: %s, id: %d send reduce time %lf to master", my_host_name_.c_str(), id_, rTime);
    Send(master_mailbox_, bw_config_->GetBW(BWType::M_W), new double(rTime), sizeof(double));
    delete mailbox_->get<unsigned char>();

    // PrintLocalList();
    TeraSortClear();
}

void Worker::ExecMap() {
    auto map_start = std::chrono::high_resolution_clock::now();

    // READ INPUT FILE AND PARTITION DATA
    char filePath[MAX_FILE_PATH];
    sprintf(filePath, "%s_%d", conf->getInputPath(), host_id_ - 1);
    ifstream inputFile(filePath, ios::in | ios::binary | ios::ate);
    if (!inputFile.is_open()) {
        LOG_ERROR("[worker] my_host_name: %s, id: %d, cannot open file %s", my_host_name_.c_str(), id_,
                  conf->getInputPath());
        assert(false);
    }

    int fileSize = inputFile.tellg();
    unsigned long int lineSize = conf->getLineSize();
    unsigned long int numLine = fileSize / lineSize;
    inputFile.seekg(0, ios::beg);

    // Build trie
    unsigned char prefix[conf->getKeySize()];
    trie = buildTrie(&partitionList, 0, partitionList.size(), prefix, 0, 2);

    // Create lists of lines
    for (unsigned int i = 0; i < conf->getNumReducer(); i++) {
        partitionCollection.insert(pair<unsigned int, LineList*>(i, new LineList));
    }
    // MAP
    // Put each line to associated collection according to partition list
    for (unsigned long i = 0; i < numLine; i++) {
        unsigned char* buff = new unsigned char[lineSize];
        inputFile.read((char*)buff, lineSize);
        unsigned int wid = trie->findPartition(buff);
        partitionCollection.at(wid)->push_back(buff);
    }
    inputFile.close();
    auto map_end = std::chrono::high_resolution_clock::now();
    auto rTime = std::chrono::duration_cast<std::chrono::duration<double>>(map_end - map_start).count();
    // LOG_INFO("[worker] my_host_name: %s, id: %d send map time %lf to master", my_host_name_.c_str(), id_, rTime);
    // master_mailbox_->put(new double(rTime), 8);
    Send(master_mailbox_, bw_config_->GetBW(BWType::M_W), new double(rTime), sizeof(double));
    delete mailbox_->get<unsigned char>();

    // PACK
    auto pack_start = std::chrono::high_resolution_clock::now();
    // Packet partitioned data to a chunk
    for (unsigned int i = 0; i < conf->getNumReducer(); i++) {
        if (i == host_id_ - 1) {
            continue;
        }
        unsigned long long numLine = partitionCollection[i]->size();
        partitionTxData[i].data = new unsigned char[numLine * lineSize];
        partitionTxData[i].numLine = numLine;
        auto lit = partitionCollection[i]->begin();
        for (unsigned long long j = 0; j < numLine * lineSize; j += lineSize) {
            memcpy(partitionTxData[i].data + j, *lit, lineSize);
            delete[] * lit;
            lit++;
        }
        delete partitionCollection[i];
    }
    // LOG_INFO("[worker] host_id: %d print partitionTxData", host_id_);
    // PrintPartitionTxData();
    auto pack_end = std::chrono::high_resolution_clock::now();
    rTime = std::chrono::duration_cast<std::chrono::duration<double>>(pack_end - pack_start).count();
    // MPI_Gather(&rTime, 1, MPI_DOUBLE, NULL, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
    // LOG_INFO("[worker] my_host_name: %s, id: %d send pack time %lf to master", my_host_name_.c_str(), id_, rTime);
    Send(master_mailbox_, bw_config_->GetBW(BWType::M_W), new double(rTime), sizeof(double));
    delete mailbox_->get<unsigned char>();
}

void Worker::ExecReduce() {
    std::sort(localList.begin(), localList.end(), Sorter(conf->getKeySize()));
}

TrieNode* Worker::buildTrie(PartitionList* partitionList, int lower, int upper, unsigned char* prefix, int prefixSize,
                            int maxDepth) {
    if (prefixSize >= maxDepth || lower == upper) {
        return new LeafTrieNode(prefixSize, partitionList, lower, upper);
    }
    InnerTrieNode* result = new InnerTrieNode(prefixSize);
    int curr = lower;
    for (unsigned char ch = 0; ch < 255; ch++) {
        prefix[prefixSize] = ch;
        lower = curr;
        while (curr < upper) {
            if (cmpKey(prefix, partitionList->at(curr), prefixSize + 1)) {
                break;
            }
            curr++;
        }
        result->setChild(ch, buildTrie(partitionList, lower, curr, prefix, prefixSize + 1, maxDepth));
    }
    prefix[prefixSize] = 255;
    result->setChild(255, buildTrie(partitionList, curr, upper, prefix, prefixSize + 1, maxDepth));
    return result;
}

void Worker::PrintLocalList() {
    unsigned long int i = 0;
    for (auto it = localList.begin(); it != localList.end(); ++it) {
        std::cout << host_id_ << ": " << i++ << "| ";
        printKey(*it, conf->getKeySize());
        std::cout << std::endl;
    }
}

void Worker::PrintPartitionCollection() {
    for (auto iter : partitionCollection) {
        for (auto it = iter.second->begin(); it != iter.second->end(); it++) {
            std::cout << "src: " << host_id_ << " dst: " << iter.first << " ";
            printKey(*it, conf->getKeySize());
            std::cout << std::endl;
        }
    }
}
void Worker::PrintPartitionTxData() {
    for (auto it : partitionTxData) {
        int dst_id = it.first;
        unsigned char* data = it.second.data;
        for (int i = 0; i < it.second.numLine; i++) {
            printf("src: %d, dst: %d ", host_id_, dst_id);
            printKey(data, conf->getKeySize());
            data += conf->getLineSize();
            std::cout << std::endl;
        }
    }
}