#include "bandwidth_config.h"

/**
 * interval/s   master-worker-bw/Bps    worker-worker-bw/Bps    broadcast-bw/Bls 
 * 10           100000                  20000                   80000
 * 10           10000                   2000                    8000
*/
BandWidthConfigModule::BandWidthConfigModule(const std::string& path) {
    std::ifstream input(path);
    std::string line;
    double interval, m_w_bw, w_w_bw, broadcast_bw;

    if (!input.is_open()) {
        assert(false && "could open bandwidth config file");
    }
    while (std::getline(input, line)) {
        std::istringstream iss(line);
        std::vector<double> temp;
        double token;
        while (iss >> token) {
            temp.emplace_back(token);
        }
        assert(temp.size() == 4);
        bw_config_.push_back({temp[0], temp[1], temp[2], temp[3]});
    }

    input.close();
    m_w_bw = bw_config_.front().m_w_bw;
    w_w_bw = bw_config_.front().w_w_bw;
    broadcast_bw_ = bw_config_.front().broadcast_bw;

    dynamic_adjust_bw_thd_ = std::thread([&] { DynamicAdjustBandWidth(); });
}

BandWidthConfigModule::~BandWidthConfigModule() {
    dynamic_adjust_bw_thd_.join();
}

void BandWidthConfigModule::DynamicAdjustBandWidth() {
    while (true) {
        sleep(bw_config_[cur_pos].time);
        cur_pos = (cur_pos + 1) % bw_config_.size();
        m_w_bw_ = bw_config_[cur_pos].m_w_bw;
        w_w_bw_ = bw_config_[cur_pos].w_w_bw;
        broadcast_bw_ = bw_config_[cur_pos].broadcast_bw;
    }
}