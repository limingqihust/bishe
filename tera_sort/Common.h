#ifndef _MR_COMMON
#define _MR_COMMON

#include <iomanip>
#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
using namespace std;


#define MAX_FILE_PATH 1024


typedef vector< unsigned char* > PartitionList;
typedef vector< unsigned char* > LineList;
typedef std::unordered_map<unsigned int, LineList*> PartitionCollection;
typedef struct _TxData {
    unsigned char* data;
    unsigned long long numLine;  // Number of lines
} TxData;
typedef std::unordered_map<unsigned int, TxData> PartitionPackData;  // key in { 0, 1, 2, ... }

#endif
