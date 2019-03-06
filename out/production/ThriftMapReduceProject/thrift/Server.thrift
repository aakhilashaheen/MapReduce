include "shared.thrift"

service ServerService {
    //enrolls the compute node and sends it the scheduling policy
    i32 enroll(1: shared.Node node),
    //uses map reduce to ouput an ordered file of sentiment values
    string mapReduceJob(1: string inputDirectory)
    void completedMapTask(1: string inputFile, 2: string intermediateFile)
    void completedSortTask(1: string outputFile)
}