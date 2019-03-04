include "shared.thrift"

service ComputeNodeService {
    //Takes in the input file and ourputs a file with a sentiment value
    string mapTask(1:string inputFilename),

    //Takes the list of intermediate files and outputs a single sorted file with sentiment values
    string sortTask(1:list<string> intermediateFilenames)
}