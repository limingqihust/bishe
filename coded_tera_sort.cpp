#include "master.h"
#include "worker.h"
#define PRINT_LOCAL_LIST
UtilityInfo Master::CodedTeraSort() {
    LOG_INFO("[master] master_id: %d, CodedTeraSort start", id_);

    UtilityInfo res;
    // GENERATE LIST OF PARTITIONS.
    PartitionSampling partitioner;
    partitioner.setConfiguration(&coded_conf);
    PartitionList* partitionList = partitioner.createPartitions();
    unsigned long int i = 0;

    // BROADCAST CONFIGURATION TO WORKERS
    /* omit temporary*/

    // BROADCAST PARTITIONS TO WORKERS
    assert(partitionList->size() == coded_conf.getNumReducer() - 1);
    for (auto it = partitionList->begin(); it != partitionList->end(); it++) {
        for (auto worker_mailbox : worker_mailboxs_) {
            unsigned char* partition = new unsigned char[coded_conf.getKeySize() + 1];
            memcpy(partition, *it, coded_conf.getKeySize() + 1);
            Send(worker_mailbox, bw_config_->GetBW(BWType::M_W), partition, coded_conf.getKeySize() + 1);
        }
    }

    // COMPUTE CODE GENERATION TIME
    int numWorker = coded_conf.getNumReducer();
    double avgTime = 0;
    double maxTime = 0;
    for (int i = 1; i <= numWorker; i++) {
        double* time = mailbox_->get<double>();
        avgTime += *time;
        maxTime = max(maxTime, *time);
        delete time;
    }
    std::cout << "[master] CODEGEN     | Avg = " << setw(10) << avgTime / numWorker << "   Max = " << setw(10)
              << maxTime << endl;
    for (auto mailbox : worker_mailboxs_) {
        Send(mailbox, bw_config_->GetBW(BWType::MAX), new unsigned char, sizeof(unsigned char));
    }

    // COMPUTE MAP TIME
    avgTime = 0;
    maxTime = 0;
    for (int i = 1; i <= numWorker; i++) {
        double* time = mailbox_->get<double>();
        avgTime += *time;
        maxTime = max(maxTime, *time);
        delete time;
    }
    std::cout << "[master] MAP     | Avg = " << setw(10) << avgTime / numWorker << "   Max = " << setw(10) << maxTime
              << endl;
    res.computation_load = avgTime / numWorker;
    for (auto mailbox : worker_mailboxs_) {
        Send(mailbox, bw_config_->GetBW(BWType::MAX), new unsigned char, sizeof(unsigned char));
    }

    // COMPUTE ENCODE TIME
    avgTime = 0;
    maxTime = 0;
    for (int i = 1; i <= numWorker; i++) {
        double* time = mailbox_->get<double>();
        avgTime += *time;
        maxTime = max(maxTime, *time);
        delete time;
    }
    std::cout << "[master] ENCODE     | Avg = " << setw(10) << avgTime / numWorker << "   Max = " << setw(10) << maxTime
              << endl;
    for (auto mailbox : worker_mailboxs_) {
        Send(mailbox, bw_config_->GetBW(BWType::MAX), new unsigned char, sizeof(unsigned char));
    }

    // COMPUTE SHUFFLE TIME
    avgTime = 0;
    maxTime = 0;
    for (auto mailbox : worker_mailboxs_) {
        // notify worker i to send data
        auto barrier_mailbox = simgrid::s4u::Mailbox::by_name(mailbox->get_name() + ":barrier");
        Send(barrier_mailbox, bw_config_->GetBW(BWType::MAX), new unsigned char, sizeof(unsigned char));
        // wait for all worker receive data done
        for (int i = 0; i < worker_host_num_; i++) {
            delete barrier_mailbox_->get<unsigned char>();
        }
    }
    avgTime = 0;
    maxTime = 0;
    for (int i = 1; i <= numWorker; i++) {
        double* time = mailbox_->get<double>();
        avgTime += *time;
        maxTime = max(maxTime, *time);
        delete time;
    }
    res.network_load = avgTime / numWorker;
    std::cout << "[master] SHUFFLE    | Avg = " << setw(10) << avgTime / numWorker << "   Max = " << setw(10) << maxTime
              << endl;
    for (auto mailbox : worker_mailboxs_) {
        Send(mailbox, bw_config_->GetBW(BWType::MAX), new unsigned char, sizeof(unsigned char));
    }

    // COMPUTE DECODE TIME
    avgTime = 0;
    maxTime = 0;
    for (int i = 1; i <= numWorker; i++) {
        double* time = mailbox_->get<double>();
        avgTime += *time;
        maxTime = max(maxTime, *time);
        delete time;
    }
    std::cout << "[master] DECODE     | Avg = " << setw(10) << avgTime / numWorker << "   Max = " << setw(10) << maxTime
              << endl;
    for (auto mailbox : worker_mailboxs_) {
        Send(mailbox, bw_config_->GetBW(BWType::MAX), new unsigned char, sizeof(unsigned char));
    }

    // COMPUTE REDUCE TIME
    avgTime = 0;
    maxTime = 0;
    for (int i = 1; i <= numWorker; i++) {
        double* time = mailbox_->get<double>();
        avgTime += *time;
        maxTime = max(maxTime, *time);
        delete time;
    }
    std::cout << "[master] REDUCE     | Avg = " << setw(10) << avgTime / numWorker << "   Max = " << setw(10) << maxTime
              << endl;
    return res;
}

void Worker::CodedTeraSort() {
    LOG_INFO(
        "[worker] my_host_name: %s, id: %d, CodedTeraSort start, input_file_num: %d, reducer_num: %d, r: %d, "
        "intpuf_file_prefix: %s",
        my_host_name_prefix_.c_str(), id_, job_text_.input_file_num, job_text_.reducer_num, job_text_.r,
        job_text_.input_file_prefix.c_str());
    // RECEIVE CONFIGURATION FROM MASTER
    // coded_conf = new CodedConfiguration;
    coded_conf = new CodedConfiguration(job_text_.input_file_num, job_text_.reducer_num, job_text_.r,
                                        job_text_.input_file_prefix);
    // MPI_Bcast( (void*) conf, sizeof( Configuration ), MPI_CHAR, 0, MPI_COMM_WORLD );

    // RECEIVE PARTITIONS FROM MASTER
    for (unsigned int i = 1; i < coded_conf->getNumReducer(); i++) {
        // unsigned char* buff = new unsigned char[ conf->getKeySize() + 1 ];
        // MPI_Bcast( buff, conf->getKeySize() + 1, MPI_UNSIGNED_CHAR, 0, MPI_COMM_WORLD );
        unsigned char* buff = mailbox_->get<unsigned char>();
        partitionList.push_back(buff);
    }

    // GENERATE CODING SCHEME AND MULTICAST GROUPS
    auto codegen_start = std::chrono::high_resolution_clock::now();
    cg = new CodeGeneration(coded_conf->getNumInput(), coded_conf->getNumReducer(), coded_conf->getLoad());
    GenMulticastGroup();
    auto codegen_end = std::chrono::high_resolution_clock::now();
    auto rTime = std::chrono::duration_cast<std::chrono::duration<double>>(codegen_end - codegen_start).count();
    Send(master_mailbox_, bw_config_->GetBW(BWType::M_W), new double(rTime), sizeof(double));
    delete mailbox_->get<unsigned char>();
    // EXECUTE MAP PHASE
    auto map_start = std::chrono::high_resolution_clock::now();
    ExecCodedMap();
    auto map_end = std::chrono::high_resolution_clock::now();
    rTime = std::chrono::duration_cast<std::chrono::duration<double>>(map_end - map_start).count();
    Send(master_mailbox_, bw_config_->GetBW(BWType::M_W), new double(rTime), sizeof(double));
    delete mailbox_->get<unsigned char>();

    // EXECTUTE ENCODE PHASE
    auto encode_start = std::chrono::high_resolution_clock::now();
    ExecCodedEncoding();
    auto encode_end = std::chrono::high_resolution_clock::now();
    rTime = std::chrono::duration_cast<std::chrono::duration<double>>(encode_end - encode_start).count();
    Send(master_mailbox_, bw_config_->GetBW(BWType::M_W), new double(rTime), sizeof(double));
    delete mailbox_->get<unsigned char>();

    // EXECTUTE SHUFFLE PHASE
    ExecCodedShuffle();
    delete mailbox_->get<unsigned char>();

    // EXECTUTE DECODE PHASE
    auto decode_start = std::chrono::high_resolution_clock::now();
    ExecCodedDecoding();
    auto decode_end = std::chrono::high_resolution_clock::now();
    rTime = std::chrono::duration_cast<std::chrono::duration<double>>(decode_end - decode_start).count();
    Send(master_mailbox_, bw_config_->GetBW(BWType::M_W), new double(rTime), sizeof(double));
    delete mailbox_->get<unsigned char>();

    // EXECTUTE REDUCE PHASE
    auto reduce_start = std::chrono::high_resolution_clock::now();
    ExecCodedReduce();
    auto reduce_end = std::chrono::high_resolution_clock::now();
    rTime = std::chrono::duration_cast<std::chrono::duration<double>>(reduce_end - reduce_start).count();
    Send(master_mailbox_, bw_config_->GetBW(BWType::M_W), new double(rTime), sizeof(double));
#ifdef PRINT_LOCAL_LIST
    PrintLocalList();
#endif

    Clear();
}

void Worker::GenMulticastGroup() {
    map<NodeSet, SubsetSId> ssmap = cg->getSubsetSIdMap();
    for (auto nsit = ssmap.begin(); nsit != ssmap.end(); nsit++) {
        NodeSet ns = nsit->first;
        SubsetSId nsid = nsit->second;
        int color = (ns.find(host_id_) != ns.end()) ? 1 : 0;
        // MPI_Comm mgComm;
        // MPI_Comm_split(workerComm, color, rank, &mgComm);
        // multicastGroupMap[nsid] = mgComm;
    }
}

void Worker::ExecCodedMap() {
    // Get a set of inputs to be processed
    InputSet inputSet = cg->getM(host_id_);
    // Build trie
    unsigned char prefix[coded_conf->getKeySize()];
    trie = buildTrie(&partitionList, 0, partitionList.size(), prefix, 0, 2);

    // Read input files and partition data
    for (auto init = inputSet.begin(); init != inputSet.end(); init++) {
        unsigned int inputId = *init;

        // Read input
        char filePath[MAX_FILE_PATH];
        sprintf(filePath, "%s_%d", coded_conf->getInputPath(), inputId - 1);
        ifstream inputFile(filePath, ios::in | ios::binary | ios::ate);
        if (!inputFile.is_open()) {
            cout << host_id_ << ": Cannot open input file " << filePath << endl;
            assert(false);
        }

        unsigned long fileSize = inputFile.tellg();
        unsigned long int lineSize = coded_conf->getLineSize();
        unsigned long int numLine = fileSize / lineSize;
        inputFile.seekg(0, ios::beg);
        PartitionCollection& pc = inputPartitionCollection[inputId];

        // Crate lists of lines
        for (unsigned int i = 0; i < coded_conf->getNumReducer(); i++) {
            pc[i] = new LineList;
            // inputPartitionCollection[ inputId ][ i ] = new LineList;
        }

        // Partition data in the input file
        for (unsigned long i = 0; i < numLine; i++) {
            unsigned char* buff = new unsigned char[lineSize];
            inputFile.read((char*)buff, lineSize);
            unsigned int wid = trie->findPartition(buff);
            assert(pc.find(wid) != pc.end());
            pc[wid]->push_back(buff);
            // inputPartitionCollection[ inputId ][ wid ]->push_back( buff );
        }

        // Remove unnecessarily lists (partitions associated with the other nodes having the file)
        NodeSet fsIndex = cg->getNodeSetFromFileID(inputId);
        for (unsigned int i = 0; i < coded_conf->getNumReducer(); i++) {
            if (i + 1 != host_id_ && fsIndex.find(i + 1) != fsIndex.end()) {
                LineList* list = pc[i];
                // LineList* list = inputPartitionCollection[ inputId ][ i ];
                for (auto lit = list->begin(); lit != list->end(); lit++) {
                    delete[] * lit;
                }
                delete list;
            }
        }

        inputFile.close();
    }
}

void Worker::ExecCodedEncoding() {
    vector<NodeSet> subsetS = cg->getNodeSubsetSContain(host_id_);
    unsigned lineSize = coded_conf->getLineSize();
    for (auto nsit = subsetS.begin(); nsit != subsetS.end(); nsit++) {
        SubsetSId nsid = cg->getSubsetSId(*nsit);
        unsigned long long maxSize = 0;

        // Construct chucks of input from data with index ns\{q}
        for (auto qit = nsit->begin(); qit != nsit->end(); qit++) {
            if ((unsigned int)*qit == host_id_) {
                continue;
            }
            int destId = *qit;
            NodeSet inputIdx(*nsit);
            inputIdx.erase(destId);

            unsigned long fid = cg->getFileIDFromNodeSet(inputIdx);
            VpairList vplist;
            vplist.push_back(Vpair(destId, fid));

            unsigned int partitionId = destId - 1;

            LineList* ll = inputPartitionCollection[fid][partitionId];

            auto lit = ll->begin();
            unsigned int numPart = coded_conf->getLoad();
            unsigned long long chunkSize = ll->size() / numPart;  // a number of lines ( not bytes )
            // first chunk to second last chunk
            for (unsigned int ci = 0; ci < numPart - 1; ci++) {
                unsigned char* chunk = new unsigned char[chunkSize * lineSize];
                for (unsigned long long j = 0; j < chunkSize; j++) {
                    memcpy(chunk + j * lineSize, *lit, lineSize);
                    lit++;
                }
                DataChunk dc;
                dc.data = chunk;
                dc.size = chunkSize;
                encodePreData[nsid][vplist].push_back(dc);
            }
            // last chuck
            unsigned long long lastChunkSize = ll->size() - chunkSize * (numPart - 1);
            unsigned char* chunk = new unsigned char[lastChunkSize * lineSize];
            for (unsigned long long j = 0; j < lastChunkSize; j++) {
                memcpy(chunk + j * lineSize, *lit, lineSize);
                lit++;
            }
            DataChunk dc;
            dc.data = chunk;
            dc.size = lastChunkSize;
            encodePreData[nsid][vplist].push_back(dc);

            // Determine associated chunk of a worker ( order in ns )
            unsigned int rankChunk = 0;  // in [ 0, ... , r - 1 ]
            for (auto it = inputIdx.begin(); it != inputIdx.end(); it++) {
                if ((unsigned int)*it == host_id_) {
                    break;
                }
                rankChunk++;
            }
            maxSize = max(maxSize, encodePreData[nsid][vplist][rankChunk].size);

            // Remode unused intermediate data from Map
            for (auto lit = ll->begin(); lit != ll->end(); lit++) {
                delete[] * lit;
            }
            delete ll;
        }

        // Initialize encode data
        encodeDataSend[nsid].data = new unsigned char[maxSize * lineSize]();  // Initial it with 0
        encodeDataSend[nsid].size = maxSize;
        unsigned char* data = encodeDataSend[nsid].data;

        // Encode Data
        for (auto qit = nsit->begin(); qit != nsit->end(); qit++) {
            if ((unsigned int)*qit == host_id_) {
                continue;
            }
            int destId = *qit;
            NodeSet inputIdx(*nsit);
            inputIdx.erase(destId);
            unsigned long fid = cg->getFileIDFromNodeSet(inputIdx);
            VpairList vplist;
            vplist.push_back(Vpair(destId, fid));

            // Determine associated chunk of a worker ( order in ns )
            unsigned int rankChunk = 0;  // in [ 0, ... , r - 1 ]
            for (auto it = inputIdx.begin(); it != inputIdx.end(); it++) {
                if ((unsigned int)*it == host_id_) {
                    break;
                }
                rankChunk++;
            }

            // Start encoding
            unsigned char* predata = encodePreData[nsid][vplist][rankChunk].data;
            unsigned long long size = encodePreData[nsid][vplist][rankChunk].size;
            unsigned long long maxiter = size * lineSize / sizeof(uint32_t);
            for (unsigned long long i = 0; i < maxiter; i++) {
                ((uint32_t*)data)[i] ^= ((uint32_t*)predata)[i];
            }

            // Fill metadata
            MetaData md;
            md.vpList = vplist;
            md.vpSize[vplist[0]] = size;  // Assume Eta = 1;
            md.partNumber = rankChunk + 1;
            md.size = size;
            encodeDataSend[nsid].metaList.push_back(md);
        }

        // Serialize Metadata
        EnData& endata = encodeDataSend[nsid];
        unsigned int ms = 0;
        ms += sizeof(unsigned int);  // metaList.size()
        for (unsigned int m = 0; m < endata.metaList.size(); m++) {
            ms += sizeof(unsigned int);                                                               // vpList.size()
            ms += sizeof(int) * 2 * endata.metaList[m].vpList.size();                                 // vpList
            ms += sizeof(unsigned int);                                                               // vpSize.size()
            ms += (sizeof(int) * 2 + sizeof(unsigned long long)) * endata.metaList[m].vpSize.size();  // vpSize
            ms += sizeof(unsigned int);                                                               // partNumber
            ms += sizeof(unsigned long long);                                                         // size
        }
        encodeDataSend[nsid].metaSize = ms;

        unsigned char* mbuff = new unsigned char[ms];
        unsigned char* p = mbuff;
        unsigned int metaSize = endata.metaList.size();
        memcpy(p, &metaSize, sizeof(unsigned int));
        p += sizeof(unsigned int);
        // meta data List
        for (unsigned int m = 0; m < metaSize; m++) {
            MetaData mdata = endata.metaList[m];
            unsigned int numVp = mdata.vpList.size();
            memcpy(p, &numVp, sizeof(unsigned int));
            p += sizeof(unsigned int);
            // vpair List
            for (unsigned int v = 0; v < numVp; v++) {
                memcpy(p, &(mdata.vpList[v].first), sizeof(int));
                p += sizeof(int);
                memcpy(p, &(mdata.vpList[v].second), sizeof(int));
                p += sizeof(int);
            }
            // vpair size Map
            unsigned int numVps = mdata.vpSize.size();
            memcpy(p, &numVps, sizeof(unsigned int));
            p += sizeof(unsigned int);
            for (auto vpsit = mdata.vpSize.begin(); vpsit != mdata.vpSize.end(); vpsit++) {
                Vpair vp = vpsit->first;
                unsigned long long size = vpsit->second;
                memcpy(p, &(vp.first), sizeof(int));
                p += sizeof(int);
                memcpy(p, &(vp.second), sizeof(int));
                p += sizeof(int);
                memcpy(p, &size, sizeof(unsigned long long));
                p += sizeof(unsigned long long);
            }
            memcpy(p, &(mdata.partNumber), sizeof(unsigned int));
            p += sizeof(unsigned int);
            memcpy(p, &(mdata.size), sizeof(unsigned long long));
            p += sizeof(unsigned long long);
        }
        encodeDataSend[nsid].serialMeta = mbuff;
    }
}

void Worker::ExecCodedShuffle() {
    // NODE-BY-NODE
    clock_t time;
    //map< NodeSet, SubsetSId > ssmap = cg->getSubsetSIdMap();
    double shuffle_start = simgrid::s4u::Engine::get_clock();
    for (unsigned int activeId = 1; activeId <= coded_conf->getNumReducer(); activeId++) {
        // delete mailbox_->get<unsigned char>();
        if (activeId == host_id_) {
            delete barrier_mailbox_->get<unsigned char>();
        }
        vector<NodeSet>& vset = cg->getNodeSubsetSContain(activeId);
        for (auto nsit = vset.begin(); nsit != vset.end(); nsit++) {
            NodeSet ns = *nsit;
            SubsetSId nsid = cg->getSubsetSId(ns);

            // Ignore subset that does not contain the activeId
            if (ns.find(activeId) == ns.end()) {
                continue;
            }

            // MPI_Comm mcComm = multicastGroupMap[nsid];
            if (host_id_ == activeId) {
                std::vector<int> node_set;
                for (auto node : ns) {
                    node_set.push_back(node);
                }
                // LOG_INFO("worker: %d send data, node_set: ", host_id_);
                // std::cout << "{";
                // for(auto node : ns) {
                //     std::cout << node << " ";
                // }
                // std::cout << "}" << std::endl;

                SendEncodeData(encodeDataSend[nsid], node_set);
                EnData& endata = encodeDataSend[nsid];
            } else if (ns.find(host_id_) != ns.end()) {
                //convert activeId to rootId of a particular multicast group
                unsigned int rootId = 0;
                for (auto nid = ns.begin(); nid != ns.end(); nid++) {
                    if ((unsigned int)(*nid) == activeId) {
                        break;
                    }
                    rootId++;
                }
                RecvEncodeData(nsid);
                // LOG_INFO("worker: %d receive data, node_set: ", host_id_);
                // std::cout << "{";
                // for(auto node : ns) {
                //     std::cout << node << " ";
                // }
                // std::cout << "}" << std::endl;
                // receive data done
            }
        }

        // Active node should stop timer here
        auto barrier_master_mailbox_ = simgrid::s4u::Mailbox::by_name(master_mailbox_->get_name() + ":barrier");
        Send(barrier_master_mailbox_, bw_config_->GetBW(BWType::MAX), new unsigned char, sizeof(unsigned char));
        // MPI_Barrier(workerComm);
    }
    double shuffle_end = simgrid::s4u::Engine::get_clock();
    double shuffle_time = shuffle_end - shuffle_start;
    Send(master_mailbox_, bw_config_->GetBW(BWType::M_W), new double(shuffle_time), sizeof(double));
}

void Worker::ExecCodedDecoding() {
    for (auto nsit = encodeDataRecv.begin(); nsit != encodeDataRecv.end(); nsit++) {
        SubsetSId nsid = nsit->first;
        vector<EnData>& endataList = nsit->second;
        for (auto eit = endataList.begin(); eit != endataList.end(); eit++) {
            EnData& endata = *eit;
            unsigned char* cdData = endata.data;
            unsigned long long cdSize = endata.size;
            vector<MetaData>& metaList = endata.metaList;

            unsigned int numDecode = 0;
            // Decode per VpairList
            MetaData dcMeta;
            for (auto mit = metaList.begin(); mit != metaList.end(); mit++) {
                MetaData& meta = *mit;
                if (encodePreData[nsid].find(meta.vpList) == encodePreData[nsid].end()) {
                    dcMeta = meta;
                    // No original data for decoding;
                    continue;
                }
                unsigned char* oData = encodePreData[nsid][meta.vpList][meta.partNumber - 1].data;
                unsigned long long oSize = encodePreData[nsid][meta.vpList][meta.partNumber - 1].size;
                unsigned long long maxByte = min(oSize, cdSize) * coded_conf->getLineSize();
                unsigned long long maxIter = maxByte / sizeof(uint32_t);
                for (unsigned long long i = 0; i < maxIter; i++) {
                    ((uint32_t*)cdData)[i] ^= ((uint32_t*)oData)[i];
                }
                numDecode++;
            }

            // sanity check
            if (numDecode != metaList.size() - 1) {
                cout << host_id_ << ": Decode error " << numDecode << '/' << metaList.size() - 1 << endl;
                assert(numDecode != metaList.size() - 1);
            }

            if (decodePreData[nsid][dcMeta.vpList].empty()) {
                for (unsigned int i = 0; i < coded_conf->getLoad(); i++) {
                    decodePreData[nsid][dcMeta.vpList].push_back(DataChunk());
                }
            }

            decodePreData[nsid][dcMeta.vpList][dcMeta.partNumber - 1].data = cdData;
            decodePreData[nsid][dcMeta.vpList][dcMeta.partNumber - 1].size = dcMeta.size;
        }
    }

    unsigned int partitionId = host_id_ - 1;
    unsigned int lineSize = coded_conf->getLineSize();

    // Get partitioned data from input files, already stored in memory.
    InputSet inputSet = cg->getM(host_id_);
    for (auto init = inputSet.begin(); init != inputSet.end(); init++) {
        unsigned int inputId = *init;
        LineList* ll = inputPartitionCollection[inputId][partitionId];
        // copy line by line
        for (auto lit = ll->begin(); lit != ll->end(); lit++) {
            unsigned char* buff = new unsigned char[lineSize];
            memcpy(buff, *lit, lineSize);
            localList.push_back(buff);
        }
        localLoadSet.insert(inputId);
    }

    // Get partitioned data from other workers
    for (auto nvit = decodePreData.begin(); nvit != decodePreData.end(); nvit++) {
        DataPartMap& dpMap = nvit->second;
        for (auto vvit = dpMap.begin(); vvit != dpMap.end(); vvit++) {
            VpairList vplist = vvit->first;
            vector<DataChunk> vdc = vvit->second;
            // Add inputId to localLoadSet
            for (auto vpit = vplist.begin(); vpit != vplist.end(); vpit++) {
                localLoadSet.insert(vpit->second);
            }
            // Add data from each part to locallist
            for (auto dcit = vdc.begin(); dcit != vdc.end(); dcit++) {
                unsigned char* data = dcit->data;
                for (unsigned long long i = 0; i < dcit->size; i++) {
                    unsigned char* buff = new unsigned char[lineSize];
                    memcpy(buff, data + i * lineSize, lineSize);
                    localList.push_back(buff);
                }
                delete[] dcit->data;
            }
        }
    }

    if (localLoadSet.size() != coded_conf->getNumInput()) {
        cout << host_id_ << ": Only have paritioned data from ";
        CodeGeneration::printNodeSet(localLoadSet);
        cout << endl;
        assert(false);
    }
}

void Worker::ExecCodedReduce() {
    sort(localList.begin(), localList.end(), Sorter(coded_conf->getKeySize()));
}

/**
 * send encoded data to dst_ids
*/
void Worker::SendEncodeData(EnData& endata, std::vector<int> dst_ids) {
    // Send actual data
    unsigned lineSize = coded_conf->getLineSize();
    int rootId;
    // MPI_Comm_rank(comm, &rootId);
    // MPI_Bcast(&(endata.size), 1, MPI_UNSIGNED_LONG_LONG, rootId, comm);
    // MPI_Bcast(endata.data, endata.size * lineSize, MPI_UNSIGNED_CHAR, rootId, comm);
    for (auto id : dst_ids) {
        if (id == host_id_) {
            continue;
        }
        auto mailbox = simgrid::s4u::Mailbox::by_name(worker_host_name_prefixs_[id - 1] + ":" +
                                                      std::to_string(worker_partener_ids_[id - 1]));
        Send(mailbox, bw_config_->GetBW(BWType::BRAODCAST), new unsigned long long(endata.size),
             sizeof(unsigned long long));
        unsigned char* data_temp = new unsigned char[endata.size * lineSize];
        memcpy(data_temp, endata.data, endata.size * lineSize);
        Send(mailbox, bw_config_->GetBW(BWType::BRAODCAST), data_temp, endata.size * lineSize);
    }
    delete[] endata.data;

    // Send serialized meta data
    // MPI_Bcast(&(endata.metaSize), 1, MPI_UNSIGNED_LONG_LONG, rootId, comm);
    // MPI_Bcast(endata.serialMeta, endata.metaSize, MPI_UNSIGNED_CHAR, rootId, comm);
    for (auto id : dst_ids) {
        if (id == host_id_) {
            continue;
        }
        auto mailbox = simgrid::s4u::Mailbox::by_name(worker_host_name_prefixs_[id - 1] + ":" +
                                                      std::to_string(worker_partener_ids_[id - 1]));
        mailbox->put(new unsigned long long(endata.metaSize), sizeof(unsigned long long));
        unsigned char* data_temp = new unsigned char[endata.metaSize];
        memcpy(data_temp, endata.serialMeta, endata.metaSize);
        mailbox->put(data_temp, endata.metaSize);
    }
    delete[] endata.serialMeta;
}

/**
 * receive data from worker[rootId]
*/
void Worker::RecvEncodeData(SubsetSId nsid) {
    EnData endata;
    unsigned lineSize = coded_conf->getLineSize();

    // Receive actual data
    // MPI_Bcast(&(endata.size), 1, MPI_UNSIGNED_LONG_LONG, rootId, comm);
    unsigned long long* endata_size_temp = mailbox_->get<unsigned long long>();
    endata.size = *endata_size_temp;
    delete endata_size_temp;

    endata.data = new unsigned char[endata.size * lineSize];
    // MPI_Bcast(endata.data, endata.size * lineSize, MPI_UNSIGNED_CHAR, rootId, comm);
    unsigned char* data_temp = mailbox_->get<unsigned char>();
    memcpy(endata.data, data_temp, endata.size * lineSize);
    delete[] data_temp;

    // Receive serialized meta data
    // MPI_Bcast(&(endata.metaSize), 1, MPI_UNSIGNED_LONG_LONG, rootId, comm);
    unsigned long long* endata_meta_size = mailbox_->get<unsigned long long>();
    endata.metaSize = *endata_meta_size;
    delete endata_meta_size;

    endata.serialMeta = new unsigned char[endata.metaSize];
    // MPI_Bcast((unsigned char*)endata.serialMeta, endata.metaSize, MPI_UNSIGNED_CHAR, rootId, comm);
    unsigned char* serial_meta_temp = mailbox_->get<unsigned char>();
    memcpy(endata.serialMeta, serial_meta_temp, endata.metaSize);
    delete[] serial_meta_temp;

    // De-serialized meta data
    unsigned char* p = endata.serialMeta;
    unsigned int metaNum;
    memcpy(&metaNum, p, sizeof(unsigned int));
    p += sizeof(unsigned int);
    // meta data List
    for (unsigned int m = 0; m < metaNum; m++) {
        MetaData mdata;
        // vpair List
        unsigned int numVp;
        memcpy(&numVp, p, sizeof(unsigned int));
        p += sizeof(unsigned int);
        for (unsigned int v = 0; v < numVp; v++) {
            Vpair vp;
            memcpy(&(vp.first), p, sizeof(int));
            p += sizeof(int);
            memcpy(&(vp.second), p, sizeof(int));
            p += sizeof(int);
            mdata.vpList.push_back(vp);
        }
        // VpairSize Map
        unsigned int numVps;
        memcpy(&numVps, p, sizeof(unsigned int));
        p += sizeof(unsigned int);
        for (unsigned int vs = 0; vs < numVps; vs++) {
            Vpair vp;
            unsigned long long size;
            memcpy(&(vp.first), p, sizeof(int));
            p += sizeof(int);
            memcpy(&(vp.second), p, sizeof(int));
            p += sizeof(int);
            memcpy(&size, p, sizeof(unsigned long long));
            p += sizeof(unsigned long long);
            mdata.vpSize[vp] = size;
        }
        memcpy(&(mdata.partNumber), p, sizeof(unsigned int));
        p += sizeof(unsigned int);
        memcpy(&(mdata.size), p, sizeof(unsigned long long));
        p += sizeof(unsigned long long);
        endata.metaList.push_back(mdata);
    }
    delete[] endata.serialMeta;

    //Serial decoder
    encodeDataRecv[nsid].push_back(endata);
}

void Worker::PrintInputPartitionCollection() {
    LOG_INFO("[worker] host_id: %d, print inputPartitionCollection", host_id_);
    for (auto it : inputPartitionCollection) {
        std::cout << "inputId: " << it.first << std::endl;
        for (auto iter : it.second) {
            for (auto it1 = iter.second->begin(); it1 != iter.second->end(); it1++) {
                std::cout << "src: " << host_id_ << " dst: " << iter.first + 1 << " ";
                printKey(*it1, coded_conf->getKeySize());
                std::cout << std::endl;
            }
        }
    }
}