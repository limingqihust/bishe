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
#include <random>
#include "logger.h"
#include <simgrid/s4u.hpp>
enum class BWType { M_W, W_W, BRAODCAST, MAX };

enum class BandWidthDistributionType { Uniform, Exponential, Pareto};

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
   
    // config
    BandWidthDistributionType bw_distribution_type_;
    int interval_;
    // used by uniform distribution
    double m_w_bw_min_;
    double w_w_bw_min_;
    double broadcast_bw_min_;
    double max_bw_min_;
    double m_w_bw_max_;
    double w_w_bw_max_;
    double broadcast_bw_max_;
    double max_bw_max_;

    // used by exponential distribution
    std::default_random_engine exponential_generator_;
    std::exponential_distribution<double> exponential_distribution_; // 1.5
    double exponential_base_;                                        // 50

};

void Send(simgrid::s4u::Mailbox* mailbox, double bw, void* message, int size);

static BandWidthDistributionType StrToBandWidthDistributionType(const std::string str) {
    if (str == "Uniform") {
        return BandWidthDistributionType::Uniform;
    } else if (str == "Exponential") {
        return BandWidthDistributionType::Exponential;
    } else if (str == "Pareto") {
        return BandWidthDistributionType::Pareto;
    } else {
        assert(false && "undefined BandWidthDistributionType");
    }
}