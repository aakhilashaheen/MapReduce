include "shared.thrift"

service ServerService {
    //enrolls the compute node and sends it the scheduling policy
    i32 enroll(1: shared.Machine machine),
    //uses map reduce to ouput an ordered file of sentiment values
    string mapReduceJob(1: list<string> inputfileNames)
}