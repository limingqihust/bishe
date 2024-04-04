#pragma once
#include <unistd.h>
#include <atomic>
#include <cassert>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include "logger.h"
#include <simgrid/s4u.hpp>
enum class BWType { M_W, W_W, BRAODCAST, MAX };
struct BandWidthConfigUnit {
    double time;
    double m_w_bw;
    double w_w_bw;
    double broadcast_bw;
    double max_bw;
};

class BandWidthConfigModule {
public:
    BandWidthConfigModule(const std::string& path);

    ~BandWidthConfigModule();

    std::atomic<double> m_w_bw_;        // bandwidth between master and workers
    std::atomic<double> w_w_bw_;        // bandwidth between worker and worker
    std::atomic<double> broadcast_bw_;  // bandwidth when worker broadcasts
    std::atomic<double> max_bw_;        // max bandwidth, used by barrier
    double GetBW(BWType bw_type);
private:
    void DynamicAdjustBandWidth();

    std::thread dynamic_adjust_bw_thd_;  // responsible for dynamic adjustment of bandwidth
    std::vector<BandWidthConfigUnit> bw_config_;
    int cur_pos = 0;
};

void Send(simgrid::s4u::Mailbox* mailbox, double bw, void* message, int size);