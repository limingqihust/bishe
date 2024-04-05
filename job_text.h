#pragma once
#include <string>
enum class JobType { TeraSort, CodedTeraSort };
struct JobText {
    JobType type;               // TeraSort or CodedTeraSort
    int input_file_num;             // num of file assigned to worker
    int reducer_num;                // num of reducers
    int r;                          // specified by master_manager/online_learning_module
    std::string input_file_prefix;  // prefix of input file
};

JobType StringToJobType(const std::string& str);
std::string JobTypeToString(const JobType& job_type);
std::string JobTextToString(int master_id, const JobText& job_text);
JobText StringToJobText(int& master_id, const std::string& job_text_str);