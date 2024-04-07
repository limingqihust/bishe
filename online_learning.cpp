#include "online_learning.h"

/**
 * do a/b test every a interval
 * created by my_master
 * receive job from pop_queue_, assign it to master_manager_ with r
 * modify params of coded-terasort
*/
void OnlineLearningModule::DoWork() {
    while (true) {
        sleep(interval_);
        DoMicroExperient();
    }
}

/**
 * receive job from job_queue
 * 
*/
void OnlineLearningModule::DoMicroExperient() {
    std::pair<JobText, JobText> lab1 = {job_queue_->Pop(), job_queue_->Pop()};
    std::pair<JobText, JobText> lab2 = {job_queue_->Pop(), job_queue_->Pop()};
    std::thread thd_a, thd_b;
    std::pair<UtilityInfo, UtilityInfo> utility_1;  // utility of first a/b test
    std::pair<UtilityInfo, UtilityInfo> utility_2;  // utility of second a/b test
    int a_r = GetAR();
    int b_r = GetBR();
    // do first two a/b test
    LOG_INFO("[OnlineLearningModule] do first a/b test, assign job to master_manager with a_r: %d, b_r: %d", a_r, b_r);
    // thd_a = std::thread([&] { utility_1.first = master_manager_->RunTryR(job_queue_->Pop(), a_r); });
    utility_1.first = master_manager_->RunTryR(lab1.first, a_r);
    utility_1.second = master_manager_->RunTryR(lab1.second, b_r);

    // thd_b = std::thread([&] { utility_1.second = master_manager_->RunTryR(job_queue_->Pop(), b_r); });

    // thd_a.join();
    // thd_b.join();
    LOG_INFO("[OnlineLearningModule] do first a/b test done");
    // do second two a/b test
    LOG_INFO("[OnlineLearningModule] do second a/b test begin, assign job to master_manager with a_r: %d, b_r: %d", a_r,
             b_r);
    utility_2.first = master_manager_->RunTryR(lab2.first, a_r);
    utility_2.second = master_manager_->RunTryR(lab2.second, b_r);
    // thd_a = std::thread([&] { utility_2.first = master_manager_->RunTryR(job_queue_->Pop(), a_r); });

    // thd_b = std::thread([&] { utility_2.second = master_manager_->RunTryR(job_queue_->Pop(), b_r); });

    // thd_a.join();
    // thd_b.join();
    LOG_INFO("[OnlineLearningModule] do second a/b test done");
    UpdateRAccordingToABTest(utility_1.first, utility_1.second, utility_2.first, utility_2.second);
}

/**
 * update r according to a/b test
*/
void OnlineLearningModule::UpdateRAccordingToABTest(const UtilityInfo& u_1_a, const UtilityInfo& u_1_b,
                                                    const UtilityInfo& u_2_a, const UtilityInfo& u_2_b) {
    if (GetUtility(u_1_a) > GetUtility(u_1_b) && GetUtility(u_2_a) > GetUtility(u_2_b)) {
        LOG_INFO("[OnlineLearningModule] modify r from %d to %d", r_, GetAR());
        r_ = GetAR();
        master_manager_->SetR(r_);
    } else if (GetUtility(u_1_a) < GetUtility(u_1_b) && GetUtility(u_2_a) < GetUtility(u_2_b)) {
        LOG_INFO("[OnlineLearningModule] modify r from %d to %d", r_, GetBR());
        r_ = GetBR();
        master_manager_->SetR(r_);
    } else {
        LOG_INFO("[OnlineLearningModule] update eta");
        UpdateEta();
    }
}

/**
 * update eta when inconclusive
*/
void OnlineLearningModule::UpdateEta() {
    eta_ = std::max(eta_ + eta_min_, eta_max_);
}

/**
 * calculate utility of a/b test
 * 1. job executing time
 * 2. network load
 * 3. node computing load
*/
double OnlineLearningModule::GetUtility(const UtilityInfo& utility_info) const {
    return 1 / utility_info.time;
}