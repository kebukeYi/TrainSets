//
// Created by 19327 on 2026/01/30/星期五.
//
#include "raft.h"

void Raft::applyTicker() {
    while (!isStop.load(std::memory_order_relaxed)) {
        mtx.lock();
        auto msgs = getApplyMsgs();
        mtx.unlock();
        if (!msgs.empty()) {
            DPrintf("[Raft::applyTicker-raft{%d}] 向 kvServer 应用的applyMsgs.size:{%d}", me, msgs.size());
        }
        for (auto &msg: msgs) {
            DPrintf("[Raft::applyTicker-raft{%d}] 向 kvServer 应用的applyMsg.index:{%ld};", me, msg.CommandIndex);
            doApply(msg);
        }
        usleep(1000 * ApplyInterval);
    }
}

std::vector<ApplyMsg> Raft::getApplyMsgs() {
    std::vector<ApplyMsg> msgs;
    myAssert(commitIndex <= getLastLogIndex(),format("[func- Raft::getApplyMsgs()-raft{%d}] commitIndex: {%d} > getLastLogIndex: {%d}",
                    me, commitIndex, getLastLogIndex()));

    while (lastAppliedIndex < commitIndex) {
        lastAppliedIndex++;
        ApplyMsg applyMsg;
        applyMsg.SnapshotValid = false;
        applyMsg.CommandValid = true;
        auto idx = getRealLogIndex(lastAppliedIndex);
        applyMsg.Command = logs[idx].command();
        applyMsg.CommandIndex = lastAppliedIndex;
        DPrintf("[Raft::getApplyMsgs-raft{%d}] applyMsg.logIndex: {%d}", me, applyMsg.CommandIndex);
        msgs.emplace_back(applyMsg);
    }

    return msgs;
};

void Raft::doApply(ApplyMsg msg) {
    applyChan->Push(msg);
};