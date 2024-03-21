#include "tera_sort.h"
#include "master.h"
#include "worker.h"

void Master::TeraSort() {
    LOG_INFO("[master] master_id: %d, TeraSort start", id_);
}

void Worker::TeraSort() {
    LOG_INFO("[worker] my_host_name: %s, id: %d, TeraSort start", my_host_name_.c_str(), id_);
}