//
// Created by 19327 on 2026/01/27/星期二.
//
#include "raft.h"


void Raft::init(int me, std::vector<std::shared_ptr<RaftNodeRpcUtil>> peers,
                std::shared_ptr<Persisted> persist,
                std::shared_ptr<LockQueue<ApplyMsg>> applyCh) {
    me = me;
    peers = peers;
    persist_ = persist;
    applyChan = applyCh;

    currentTerm = -1;
    votedFor = -1;
    logs.clear();
    snapshotIndex = -1;
    snapshotTerm = -1;
    commitIndex = -1;
    lastAppliedIndex = -1;
    for (int i = 0; i < peers.size(); ++i) {
        nextIndex[i] = 0;
        matchIndex[i] = 0;
    }
    role = Role::Follower;
    electionResetTime = now();
    heartbeatResetTime = now();
    ioManager = std::make_unique<monsoon::IOManager>(fiberThreadPoolSize, fiberUseCallerThread);

    auto data = persist_->LoadRaftState();
    readPersistRaftState(data);
    if (snapshotIndex > 0) {
        lastAppliedIndex = snapshotIndex;
    }
    DPrintf("RaftNode-%d init: term %d, votedFor %d, logs.size: %d, snapshotIndex %d, snapshotTerm %d, commitIndex %d, lastAppliedIndex %d",
            me, currentTerm, votedFor, logs.size(), snapshotIndex, snapshotTerm, commitIndex, lastAppliedIndex);
    ioManager->scheduler([this]() {
        electionTicker();
    });
    ioManager->scheduler([this]() {
        replicationTicker();
    });

    std::thread t3(&Raft::replicationTicker, this);
    t3.detach();
}

void Raft::Start(Op op, int64_t *logIndex, int64_t *logTerm, bool *isLeader) {
    if (role != Role::Leader) {
        DPrintf("[func-Start-rf{%d}]  is not leader");
        *logIndex = -1;
        *logTerm = -1;
        *isLeader = false;
        return;
    }
    RaftNodeRpcProtoc::LogEntry logEntry;
    logEntry.set_command(op.toString());
    logEntry.set_logindex(getNextCommandIndex());
    logEntry.set_logterm(currentTerm);
    logs.emplace_back(logEntry);
    int64_t lastLogIndex = logEntry.logindex();
    std::string command = logEntry.command();
    DPrintf("[func-Start-rf{%d}]  lastLogIndex:%d,command:%s\n", me, lastLogIndex, &command);

    persistRaftState();
    *logIndex = logEntry.logindex();
    *logTerm = logEntry.logterm();
    *isLeader = true;
}

void Raft::getStateForCli(int64_t *term, bool *isLeader) {
    std::lock_guard<std::mutex> lock(mtx);
    *term = currentTerm;
    *isLeader = (role == Role::Leader);
}






























































