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

enum class State {
    FREE, BUSY
};

struct MasterWorkerState {
    std::mutex mutex;
    std::unordered_map<int, State> master_state;
    std::vector<int, std::unordered_map<int, State>> worker_state;
};