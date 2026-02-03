//
// Created by 19327 on 2026/01/27/星期二.
//
#include "raft.h"

#include <utility>


void Raft::init(int me, std::vector<std::shared_ptr<RaftNodeRpcUtil>> peers,
                std::shared_ptr<Persisted> persist,
                std::shared_ptr<LockQueue<ApplyMsg>> applyCh) {
    DPrintf("[func-init-rf{%d}]  init raft node", me);
    this->me = me;
    this->peers = peers;
    isStop.store(false);
    mtx.lock();

    persist_ = std::move(persist);
    applyChan = std::move(applyCh);

    currentTerm = 1;
    votedFor = -1; // -1 means vote for none;
    logs.clear();
    snapshotIndex = 0;
    snapshotTerm = 0;
    commitIndex = 0;
    lastAppliedIndex = 0;
    nextIndex.resize(peers.size());
    matchIndex.resize(peers.size());
    for (int i = 0; i < peers.size(); ++i) {
        nextIndex[i] = 0;
        matchIndex[i] = 0;
    }
    role = Role::Follower;
    electionResetTime = now();
    heartbeatResetTime = now();

    printf("RaftNode-%d LoadRaftState...\n", me);
    auto data = persist_->LoadRaftState();
    readPersistRaftState(data);
    printf("RaftNode-%d LoadRaftState.over: term %ld, votedFor %ld, logs.size: %ld, snapshotIndex %ld, snapshotTerm %ld, commitIndex %ld, lastAppliedIndex %ld\n",
           me, currentTerm, votedFor, logs.size(), snapshotIndex, snapshotTerm, commitIndex, lastAppliedIndex);
    if (snapshotIndex > 0) {
        lastAppliedIndex = snapshotIndex;
    }

    mtx.unlock();

    ioManager = std::make_unique<monsoon::IOManager>(fiberThreadPoolSize, fiberUseCallerThread);

    ioManager->scheduler([this]() {
        electionTicker();
    });

    ioManager->scheduler([this]() {
        replicationTicker();
    });

    printf("RaftNode-%d start applyTicker \n", me);
    std::thread t3(&Raft::applyTicker, this);
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

void Raft::close(){
    DPrintf("[func-close-rf{%d}]  close raft node", me);
    ioManager->stop();
    ioManager = nullptr;
    isStop.store(true);
    DPrintf("[func-close-rf{%d}]  raft node closed", me);
}





























































