#ifndef _CMR_CONFIGURATION
#define _CMR_CONFIGURATION

#include "Configuration.h"

class CodedConfiguration : public Configuration {

private:
    unsigned int load;

public:
    CodedConfiguration() : Configuration() {
        numInput = 15;   // N is assumed to be K choose r
        numReducer = 6;  // K
        load = 2;        // r

        strcpy(inputPath, "./Input/Input10000-C");
        strcpy(outputPath, "./Output/Output10000-C");
        strcpy(partitionPath, "./Partition/Partition10000-C");
        numSamples = 10000;
    }

	/**
	 * set job_text to coded_conf
	*/
	CodedConfiguration(int input_file_num, int reducer_num, int r, const std::string& input_file_prefix) : Configuration() {
		numInput = input_file_num;
		numReducer = reducer_num;
		load = r;

		strcpy(inputPath, input_file_prefix.c_str());
        strcpy(outputPath, "./Output/Output10000-C");
        strcpy(partitionPath, "./Partition/Partition10000-C");
        numSamples = 10000;
	}
    ~CodedConfiguration() {}

    unsigned int getLoad() const { return load; }
};

#endif
