#pragma once
#include <cassert>
#include <deque>
#include <fstream>
#include <mutex>
#include <string>
#include "logger.h"
#include "master.h"
class ConcurrencyQueue {
public:
    ConcurrencyQueue(const std::string& input_file_path) {
        std::ifstream input(input_file_path);
        if (!input.is_open()) {
            LOG_ERROR("open file:%s fail", input_file_path.c_str());
            assert(false);
        }
        std::string line;
        while (std::getline(input, line)) {
            JobText job;
            std::stringstream ss(line);
            std::string job_type;
            ss >> job_type >> job.input_file_num >> job.reducer_num >> job.r >> job.input_file_prefix;
            job.type = StringToJobType(job_type);
            queue_.push_back(job);
        }
    }

    ~ConcurrencyQueue() {}

    void Push(const JobText& master_job_text) {
        mutex_.lock();
        queue_.push_back(master_job_text);
        mutex_.unlock();
    }

    JobText Pop() {
        while (true) {
            mutex_.lock();
            if (queue_.size() != 0) {
                JobText res = queue_.front();
                queue_.pop_front();
                mutex_.unlock();
                return res;
            } else {
                LOG_WARN("[ConcurrencyQueue] queue is empty, wait for master_job_text");
                mutex_.unlock();
                sleep(1);
            }
        }
    }

private:
    std::deque<JobText> queue_;
    std::mutex mutex_;
};