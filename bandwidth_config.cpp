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
        if (line.size() == 0) {
            continue;
        }
        auto pos = line.find(' ');
        const std::string key = line.substr(0, pos);
        const std::string value = line.substr(pos + 1, line.size());
        if (key == "bandwidth_distribution_type") {
            bw_distribution_type_ = StrToBandWidthDistributionType(value);
        } else if (key == "interval") {
            interval_ = std::stoi(value);
        } else if (key == "m_w_bw_min") {
            m_w_bw_min_ = std::stoi(value);
        } else if (key == "w_w_bw_min") {
            w_w_bw_min_ = std::stoi(value);
        } else if (key == "broadcast_bw_min") {
            broadcast_bw_min_ = std::stoi(value);
        } else if (key == "max_bw_min") {
            max_bw_min_ = std::stoi(value);
        } else if (key == "m_w_bw_max") {
            m_w_bw_max_ = std::stoi(value);
        } else if (key == "w_w_bw_max") {
            w_w_bw_max_ = std::stoi(value);
        } else if (key == "broadcast_bw_max") {
            broadcast_bw_max_ = std::stoi(value);
        } else if (key == "max_bw_max") {
            max_bw_max_ = std::stoi(value);
        } else if (key == "exponential_distribution") {
            exponential_distribution_ = std::exponential_distribution<double>(std::stod(value));
        } else if (key == "exponential_base") {
            exponential_base_ = std::stod(value);
        } else if (key == "pareto_scale") {
            pareto_scale_ = std::stod(value);
        } else if (key == "pareto_shape") { 
            pareto_shape_ = std::stod(value);
        } else {
            assert(false && "undefine key");
        }
    }
    // LOG_INFO("[BandWidthConfigModule] interval: %d m_w_bw_min: %f w_w_bw_min: %f broadcast_bw_min: %f max_bw_min: %f m_w_bw_max: %f w_w_bw_max: %f broadcast_bw_max: %f", 
    //                                     interval_, m_w_bw_min_, w_w_bw_min_, broadcast_bw_min_, max_bw_min_, m_w_bw_max_, w_w_bw_max_, broadcast_bw_max_);
    
    // init 
    switch (bw_distribution_type_) {
        case BandWidthDistributionType::Uniform:
            m_w_bw_ = m_w_bw_min_;
            w_w_bw_ = w_w_bw_min_;
            broadcast_bw_ = broadcast_bw_min_;
            max_bw_ = max_bw_min_;
            break;
        case BandWidthDistributionType::Exponential: 
            m_w_bw_ = exponential_base_ + exponential_distribution_(exponential_generator_);
            w_w_bw_ = m_w_bw_.load();
            broadcast_bw_ = 8 * m_w_bw_.load();
            max_bw_ = max_bw_min_;
            break;
        case BandWidthDistributionType::Pareto:
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_real_distribution<> dis(0.0, 1.0);
            m_w_bw_ = pareto_scale_ / std::pow(dis(gen), 1.0 / pareto_shape_);
            w_w_bw_ = m_w_bw_.load();
            broadcast_bw_ = 8 * m_w_bw_.load();
            max_bw_ = max_bw_min_;
            break;
    }
    input.close();

    dynamic_adjust_bw_thd_ = std::thread([&] { DynamicAdjustBandWidth(); });
}

BandWidthConfigModule::~BandWidthConfigModule() {
    dynamic_adjust_bw_thd_.join();
}

void BandWidthConfigModule::DynamicAdjustBandWidth() {
    while (true) {
        sleep(interval_);
        switch (bw_distribution_type_) {
            case BandWidthDistributionType::Uniform:
                if (m_w_bw_ == m_w_bw_min_) {
                    m_w_bw_ = m_w_bw_max_;
                    w_w_bw_ = w_w_bw_max_;
                    broadcast_bw_ = broadcast_bw_max_;
                } else {
                    m_w_bw_ = m_w_bw_min_;
                    w_w_bw_ = w_w_bw_min_;
                    broadcast_bw_ = broadcast_bw_min_;
                }
                break;
            case BandWidthDistributionType::Exponential:
                m_w_bw_ = exponential_base_ + exponential_distribution_(exponential_generator_) * 10;
                w_w_bw_ = m_w_bw_.load();
                broadcast_bw_ = 10 * m_w_bw_.load();
                break;
            case BandWidthDistributionType::Pareto:
                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_real_distribution<> dis(0.0, 1.0);
                m_w_bw_ = pareto_scale_ / std::pow(dis(gen), 1.0 / pareto_shape_);
                w_w_bw_ = m_w_bw_.load();
                broadcast_bw_ = 10 * m_w_bw_.load();
                break;
        }
        std::cout << "m_w_bw: " << m_w_bw_ << " w_w_bw: " << w_w_bw_ << " broadcast_bw: " << broadcast_bw_ << " max_bw: " << max_bw_ << std::endl;
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