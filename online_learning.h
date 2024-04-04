#pragma once

#include <deque>
#include <memory>
#include "master.h"
#include "concurrency_queue.h"
/**
 * do a/b test every a interval
 * created by my_master
 * receive job from pop_queue_, assign it to master_manager_ with r
 * modify params of coded-terasort
*/
class OnlineLearningModule {
public:
    OnlineLearningModule(std::shared_ptr<MasterManager> master_manager, std::shared_ptr<ConcurrencyQueue> job_queue,
                         int r, int max_r, double interval)
        : master_manager_(master_manager), job_queue_(job_queue), r_(r), max_r_(max_r), interval_(interval) {}

    void DoWork();

private:
    void DoMicroExperient();
    int GetAR() { return std::max(max_r_, int((1 + eta_) * r_)); }
    int GetBR() { return std::min(1, int((1 - eta_) * r_)); }
    void UpdateRAccordingToABTest(const UtilityInfo& u_1_a, const UtilityInfo& u_1_b, const UtilityInfo& u_2_a,
                                  const UtilityInfo& u_2_b);
    void UpdateEta();
    double GetUtility(const UtilityInfo& utility_info) const;

    std::shared_ptr<ConcurrencyQueue> job_queue_;
    std::shared_ptr<MasterManager> master_manager_;
    int r_;
    double eta_ = 0.1;
    double eta_min_ = 0.01;
    double eta_max_ = 0.05;
    int max_r_;
    double interval_ = 10.0;
};