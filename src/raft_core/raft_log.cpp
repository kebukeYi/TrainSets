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
    // nextIndex[server] == StartIndex, 表示没有日志下发过;
    if (nextIndex[server] == StartIndex) {
        *prevLogIndex = StartIndex;
        *prevLogTerm = StartTerm;
    } else {
        *prevLogIndex = nextIndex[server] - 1;
        *prevLogTerm = logs[static_cast<int>(getRealLogIndex(*prevLogIndex))].logterm();
    }
}

void Raft::getLastLogIndexAndTerm(int64_t *lastLogIndex, int64_t *lastLogTerm) {
    if (logs.size() == 1) {
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
    if (logIndex == StartIndex) {
        return StartTerm;
    }
    //  snapshotIndex <= logIndex <= lastLogIndex
    if (logIndex < snapshotIndex || logIndex > getLastLogIndex()) {
        return -1;
    }
    // 1. logIndex 在 快照中;
    if (logIndex == snapshotIndex) {
        return snapshotTerm;
    }
    // 2. logIndex 在 日志中;
    auto index = getRealLogIndex(logIndex);
    return logs[index].logterm();
}

bool Raft::isMatchLog(int64_t prevLogIndex, int64_t prevLogTerm) {
    // 0. prevLogIndex 在 日志没有下发过时, 都是零 0 0;
    // 1. prevLogIndex 在 快照临界点;
    // 2. prevLogIndex 在 日志中;
    auto term = getLogTermFromIndex(prevLogIndex);
    return term == prevLogTerm;
}

int64_t Raft::getTailLogIndex(int64_t logIndex) {
    if (logIndex >= getLastLogIndex()) {
        return getLastLogIndex();
    }
    return getRealLogIndex(logIndex);
}

int64_t Raft::getRealLogIndex(int64_t logIndex) {
    // 找到index对应的真实下标位置;
    if (logIndex < snapshotIndex) {
        DPrintf("getRealLogIndex: logIndex %ld out of range [%ld, %ld];log.size: %ld exit;",
                logIndex, snapshotIndex, getLastLogIndex(), logs.size());
        exit(-1);
    }
    // 如果参数中逻辑下标 大于 日志所存 的 最大逻辑下标, 则返回-1, 表示不存在;
    if (logIndex > getLastLogIndex()) {
        return -1;
    }
    // 120 - 100 = 20;
    // 第 20 个元素;
    // 0-19
    // 1-20
    return logIndex - snapshotIndex;
}
