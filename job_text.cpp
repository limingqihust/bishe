#include "job_text.h"
#include <cassert>
#include <sstream>
#include <vector>
#include "logger.h"
JobType StringToJobType(const std::string& str) {
    if (str == "TeraSort") {
        return JobType::TeraSort;
    } else if (str == "CodedTeraSort") {
        return JobType::CodedTeraSort;
    } else {
        assert(false && "undefined JobType");
    }
}

std::string JobTypeToString(const JobType& job_type) {
    switch (job_type) {
        case JobType::TeraSort:
            return "TeraSort";
        case JobType::CodedTeraSort:
            return "CodedTeraSort";
    }
}

/**
 * serialize job_text to string
 * master_id:job_type:input_file_num:reducer_num:r:input_file_prefix
*/
std::string JobTextToString(int master_id, const JobText& job_text) {
    std::string res;
    res += std::to_string(master_id) + ":";
    res += JobTypeToString(job_text.type) + ":";
    res += std::to_string(job_text.input_file_num) + ":";
    res += std::to_string(job_text.reducer_num) + ":";
    res += std::to_string(job_text.r) + ":";
    res += job_text.input_file_prefix;
    // LOG_INFO("send job: %s", res.c_str());
    return res;
}

/**
 * parse string to job_text
*/
JobText StringToJobText(int& master_id, const std::string& job_text_str) {
    JobText res;
    std::istringstream iss(job_text_str);
    std::vector<std::string> job_infos;
    std::string token;

    while (std::getline(iss, token, ':')) {
        job_infos.push_back(token);
    }

    assert(job_infos.size() == 6);
    // LOG_INFO("receive job_text: %s, %s, %s, %s, %s, %s", job_infos[0].c_str(), job_infos[1].c_str(),
    //          job_infos[2].c_str(), job_infos[3].c_str(), job_infos[4].c_str(), job_infos[5].c_str());

    master_id = std::stoi(job_infos[0]);
    res.type = StringToJobType(job_infos[1]);
    res.input_file_num = std::stoi(job_infos[2]);
    res.reducer_num = std::stoi(job_infos[3]);
    res.r = std::stoi(job_infos[4]);
    res.input_file_prefix = job_infos[5];
    return res;
}