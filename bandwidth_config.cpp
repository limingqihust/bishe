#include "bandwidth_config.h"

/**
 * interval/s   master-worker-bw/Bps    worker-worker-bw/Bps    broadcast-bw/Bls    max-bw/Bps 
 * 10           10                      10                      80                  250000000
 * 10           250000000               250000000               250000000           250000000
*/
BandWidthConfigModule::BandWidthConfigModule(const std::string& path) {
    std::ifstream input(path);
    std::string line;
    double interval;

    if (!input.is_open()) {
        assert(false && "could open bandwidth config file");
    }
    while (std::getline(input, line)) {
        if (line[0] == '#') {
            continue;
        }
        std::istringstream iss(line);
        std::vector<double> temp;
        double token;
        while (iss >> token) {
            temp.emplace_back(token);
        }
        assert(temp.size() == 5);
        bw_config_.push_back({temp[0], temp[1], temp[2], temp[3], temp[4]});
    }

    input.close();
    m_w_bw_ = bw_config_.front().m_w_bw;
    w_w_bw_ = bw_config_.front().w_w_bw;
    broadcast_bw_ = bw_config_.front().broadcast_bw;
    max_bw_ = bw_config_.front().max_bw;

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
        max_bw_ = bw_config_[cur_pos].max_bw;
    }
}

double BandWidthConfigModule::GetBW(BWType bw_type) {
    switch(bw_type) {
        case BWType::M_W:
            return m_w_bw_;
        case BWType::W_W:
            return w_w_bw_;
        case BWType::BRAODCAST:
            return broadcast_bw_;
        default:
            return max_bw_;
    }
}

/**
 * send message to mailbox with bandwidth bw
 * bw: byte/s
 * size: bype
*/
void Send(simgrid::s4u::Mailbox* mailbox, double bw, void* message, int size) {
    simgrid::s4u::CommPtr comm = mailbox->put_init(message, size);
    comm->set_rate(bw)->wait();
}