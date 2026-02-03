//
// Created by 19327 on 2026/01/31/星期六.
//
#include "raft.h"

int64_t Raft::getFirstLogIndex(int64_t term) {
    for (int i = 0; i < logs.size(); i++) {
        auto entry = logs[i];
        if (entry.logterm() == term) {
            return i + snapshotIndex;
        }
    }
    return InvalidIndex;
}

int64_t Raft::getNextCommandIndex() {
    auto lastLogIndex = getLastLogIndex();
    return lastLogIndex + 1;
}

void Raft::getPrevLogInfo(int server, int64_t *prevLogIndex, int64_t *prevLogTerm) {
    if (nextIndex[server] == snapshotIndex + 1) {
        *prevLogIndex = snapshotIndex;
        *prevLogTerm = snapshotTerm;
        return;
    }
    *prevLogIndex = nextIndex[server] - 1;
    *prevLogTerm = logs[static_cast<int>(getLogicLogIndex(*prevLogIndex))].logterm();
}


void Raft::getLastLogIndexAndTerm(int64_t *lastLogIndex, int64_t *lastLogTerm) {
    if (logs.empty()) {
        *lastLogIndex = snapshotIndex;
        *lastLogTerm = snapshotTerm;
        return;
    } else {
        *lastLogIndex = logs[logs.size() - 1].logindex();
        *lastLogTerm = logs[logs.size() - 1].logterm();
        return;
    }
}

bool Raft::isVoteFor(int64_t candidateLogIndex, int64_t candidateLogTerm) {
    // 是否要给 candidate 投票;
    int64_t lastLogIndex, lastLogTerm;
    getLastLogIndexAndTerm(&lastLogIndex, &lastLogTerm);
    return candidateLogTerm > lastLogTerm ||
           (candidateLogTerm == lastLogTerm && candidateLogIndex >= lastLogIndex);
}

int64_t Raft::getLastLogIndex() {
    int64_t lastLogIndex;
    int64_t lastLogTerm;
    getLastLogIndexAndTerm(&lastLogIndex, &lastLogTerm);
    return lastLogIndex;
}

int64_t Raft::getLastLogTerm() {
    int64_t lastLogIndex;
    int64_t lastLogTerm;
    getLastLogIndexAndTerm(&lastLogIndex, &lastLogTerm);
    return lastLogTerm;
}

int64_t Raft::getLogTermFromIndex(int64_t logIndex) {
    //  snapshotIndex <= logIndex <= lastLogIndex
    if (logIndex < snapshotIndex || logIndex > getLastLogIndex()) {
        return -1;
    }
    // 1. logIndex 在 快照中;
    if (logIndex == snapshotIndex) {
        return snapshotTerm;
    }
    // 2. logIndex 在 日志中;
    auto index = getLogicLogIndex(logIndex);
    return logs[index].logterm();
}

bool Raft::isMatchLog(int64_t prevLogIndex, int64_t prevLogTerm) {
    if (prevLogIndex < snapshotIndex || prevLogIndex > getLastLogIndex()) {
        return false;
    }
    // 1. prevLogIndex 在 快照临界点;
    // 2. prevLogIndex 在 日志中;
    auto term = getLogTermFromIndex(prevLogIndex);
    return term == prevLogTerm;
}

int64_t Raft::getLogicLogIndex(int64_t logIndex) {
    // 找到index对应的真实下标位置;
    if (logIndex < snapshotIndex || logIndex > getLastLogIndex()) {
        return -1;
    }
    return logIndex - snapshotIndex;
}
