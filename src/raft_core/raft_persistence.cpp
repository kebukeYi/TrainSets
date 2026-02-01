//
// Created by 19327 on 2026/01/30/星期五.
//

#include "raft.h"

void Raft::persistRaftState() {
    auto data=doPersistRaftState();
    persist_->SaveRaftState(data);
}

std::string Raft::doPersistRaftState() {
    BoostPersistRaftNode node;
    node.term = currentTerm;
    node.votedFor = votedFor;
    node.snapshotIndex = snapshotIndex;
    node.snapshotTerm = snapshotTerm;
    for (int i = 0; i < logs.size(); ++i) {
        // protoc.SerializeAsString()
        node.logs.push_back(logs[i].SerializeAsString());
    }
    std::stringstream sa;
    boost::archive::text_oarchive oa(sa);
    oa << node;
    return sa.str();
}

void Raft::readPersistRaftState(std::string &data){
    if (data.empty()){
        return;
    }
    std::stringstream  sin(data);
    boost::archive::text_iarchive ia(sin);
    BoostPersistRaftNode node;
    ia >> node;
    currentTerm = node.term;
    votedFor = node.votedFor;
    snapshotIndex = node.snapshotIndex;
    snapshotTerm = node.snapshotTerm;
    logs.clear();
    for (int i = 0; i < node.logs.size(); ++i) {
        RaftNodeRpcProtoc::LogEntry logEntry;
        logEntry.ParseFromString(node.logs[i]);
        logs.emplace_back(logEntry);
    }
}

int64_t Raft::getRaftStateSize() {
    return persist_->GetRaftStateFileSize();
}