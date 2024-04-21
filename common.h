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
    Free, Busy
};

struct MasterWorkerState {
    std::mutex mutex;
    std::unordered_map<int, State> master_state;                            // master id --> state
    std::unordered_map<int, std::unordered_map<int, State>> worker_state;   // worker manager id --> worker id --> state
};