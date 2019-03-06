include "shared.thrift"

service WorkerNodeService {
    //Takes in the input file and returns true if the file will be processed by the worker
    bool mapTask(1:string inputFilename),

    //Takes the intermediate files folder and outputs a single sorted file with sentiment values
    string sortTask(1: string intermediateFilesFolder)
}