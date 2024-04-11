#pragma once
#include <memory>
#include <mutex>
#include <simgrid/s4u.hpp>
#include <string>
#include <vector>
#include <cassert>
#include <fstream>
#include <chrono>
#include "logger.h"

enum class CommandType {
    Map, Send, Receive, Omit, End
};
struct Command {
    CommandType type;
    std::vector<int> file_ids;
    int receive_id;
    std::pair<int, int> reduce_schedule_info;   // data routed to first originly should routed to second
};

/**
 * parse char array to command
 * free memory of char array
*/
static Command ParseCommand(const char* command_temp) {
    const std::string command(command_temp);
    delete [] command_temp;
    Command res;
    std::istringstream iss(command);
    std::string token;
    assert(std::getline(iss, token, ':'));
    if (token == "Map") {
        res.type = CommandType::Map;
        while(std::getline(iss, token, ':')) {
            res.file_ids.emplace_back(stoi(token));
        }
    } else if (token == "Send") {               // map done, send data to node according to partitions 
        res.type = CommandType::Send;
        assert(std::getline(iss, token, ':'));
        res.reduce_schedule_info.first = stoi(token);
        assert(std::getline(iss, token, ':'));
        res.reduce_schedule_info.second = stoi(token);
    } else if (token == "Receive") {            // receive from node
        res.type = CommandType::Receive;
        assert(std::getline(iss, token, ':'));
        res.receive_id = stoi(token);
    } else if (token == "Omit") {
        res.type = CommandType::Omit;
    } else if (token == "End") {
        res.type = CommandType::End;
    } else {
        assert(false && "ParseCommand error, undefined command type");
    }
    return res;
}

/**
 * serialize command to char array
 * malloc memory for char array in this func
 * end of '\0'
*/
static char* SerializeCommand(const Command& command, int& command_size) {
    std::string command_str;
    if (command.type == CommandType::Map) {
        command_str += "Map";
        for (auto file_id: command.file_ids) {
            command_str += (":" + std::to_string(file_id));
        }
    } else if (command.type == CommandType::Send) {
        command_str += "Send";
        command_str += (":" + std::to_string(command.reduce_schedule_info.first) + ":" + std::to_string(command.reduce_schedule_info.second));
    } else if (command.type == CommandType::Receive) {
        command_str += "Receive";
        command_str += (":" + std::to_string(command.receive_id));
    } else if (command.type == CommandType::Omit) {
        command_str += "Omit";
    } else if (command.type == CommandType::End) {
        command_str += "End";
    } else {
        assert(false && "SerializeCommand receive undefined command type");
    }
    char* command_temp = new char [command_str.size() + 1];
    memcpy(command_temp, command_str.c_str(), command_str.size());
    command_temp[command_str.size()] = 0;
    command_size = command_str.size() + 1;
    return command_temp;
}