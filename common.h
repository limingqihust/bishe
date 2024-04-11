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
    Map, Send, Receive, End
};
struct Command {
    CommandType type;
    std::vector<int> file_ids;
    std::vector<int> send_ids;
    int receive_id;
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
        while(std::getline(iss, token, ':')) {
            res.send_ids.emplace_back(stoi(token));
        }
    } else if (token == "Receive") {            // receive from node
        res.type = CommandType::Receive;
        assert(std::getline(iss, token, ':'));
        res.receive_id = stoi(token);
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
        for (auto send_id: command.send_ids) {
            command_str += (":" + std::to_string(send_id));
        }
    } else if (command.type == CommandType::Receive) {
        command_str += "Receive";
        command_str += (":" + std::to_string(command.receive_id));
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