#ifndef _MR_COMMON
#define _MR_COMMON

#include <iomanip>
#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <set>
#include <map>
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

typedef set< int > NodeSet;
typedef set< int > InputSet;
typedef vector< vector< bool > > ImMatrix;
typedef pair< int, int > Vpair; // < destId, inputId >
typedef vector< Vpair > VpairList;
typedef struct _Vj {
  VpairList vpList;
  int dest;
  int order;
_Vj( VpairList vpl, int _dest, int _order ): vpList( vpl ), dest( _dest ), order( _order ) {}
} Vj;
typedef vector< Vj > VjList;
typedef unsigned int SubsetSId;


typedef unordered_map< unsigned int, LineList* > PartitionCollection; // key = destID
  typedef unordered_map< unsigned int, PartitionCollection > InputPartitionCollection;  // key = inputID
  // typedef unordered_map< SubsetSId, MPI::Intracomm > MulticastGroupMap;
    typedef unordered_map< SubsetSId, int > MulticastGroupMap;
  /* typedef map< unsigned int, LineList* > PartitionCollection; // key = destID */
  /* typedef map< unsigned int, PartitionCollection > InputPartitionCollection;  // key = inputID */
  /* typedef map< SubsetSId, MPI::Intracomm > MulticastGroupMap;   */
  typedef map< Vpair, unsigned long long > VpairSizeMap;

  typedef struct _DataChunk {
    unsigned char* data;
    unsigned long long size; // number of lines
  } DataChunk;
  typedef map< VpairList, vector< DataChunk > > DataPartMap;
  typedef unordered_map< SubsetSId, DataPartMap > NodeSetDataPartMap;  // [Encode/Decode]PreData
  /* typedef map< SubsetSId, DataPartMap > NodeSetDataPartMap;  // [Encode/Decode]PreData   */
  
  typedef struct _MetaData {
    VpairList vpList;
    VpairSizeMap vpSize;
    unsigned int partNumber; // { 1, 2, ... }
    unsigned long long size; // number of lines
  } MetaData;
  
  typedef struct _EnData {
    vector< MetaData > metaList;
    unsigned char* data;          // encoded chunk
    unsigned long long size;      // in number of lines
    unsigned char* serialMeta;
    unsigned long long metaSize;  // in number of bytes
  } EnData;
  typedef unordered_map< SubsetSId, EnData > NodeSetEnDataMap;  // SendData
  typedef unordered_map< SubsetSId, vector< EnData > > NodeSetVecEnDataMap;  // RecvData
  /* typedef map< SubsetSId, EnData > NodeSetEnDataMap;  // SendData */
  /* typedef map< SubsetSId, vector< EnData > > NodeSetVecEnDataMap;  // RecvData   */
  

  typedef struct {
    SubsetSId sid;
    EnData endata;
  } DecodeJob;
#endif
