//
// Created by 19327 on 2026/01/30/星期五.
//
#include "raft.h"

bool Raft::sendSnapshot(int server) {
    mtx.lock();
    RaftNodeRpcProtoc::InstallSnapshotArgs args;
    args.set_term(currentTerm);
    args.set_lastincludedindex(snapshotIndex);
    args.set_lastincludedterm(snapshotTerm);
    args.set_snapshot(persist_->LoadSnapshot()); // 加载快照,可能会比较耗时;
    mtx.unlock();

    RaftNodeRpcProtoc::InstallSnapshotReply reply;
    auto status = peers[server]->CallInstallSnapshot(&args, &reply);
    if (!status.ok()) {
        DPrintf("[SendSnapshot]Server %d send snapshot to %d fail", me, server);
        return false;
    }

    std::lock_guard<std::mutex> lock(mtx);
    if (role != Raft::Leader || currentTerm != args.term()) {
        return false;  // 中间释放过锁，可能状态已经改变了
    }

    if (reply.success()) {
        DPrintf("[SendSnapshot]Server %d send snapshot to %d success", me, server);
        if (reply.term() > currentTerm) {
            role = Raft::Follower;
            votedFor = -1;
            currentTerm = reply.term();
            DPrintf("[SendSnapshot]Server %d find a new term %d", me, currentTerm);
            persistRaftState();
            electionResetTime = now();
            return false;
        }
        matchIndex[server] = args.lastincludedindex();
        nextIndex[server] = matchIndex[server] + 1;
        return true;
    }
    return false;
};

grpc::Status Raft::InstallSnapshot(grpc::ServerContext *context, const RaftNodeRpcProtoc::InstallSnapshotArgs *request,
                                   RaftNodeRpcProtoc::InstallSnapshotReply *response) {
    std::lock_guard<std::mutex> lock(mtx);
    response->set_term(currentTerm);
    response->set_success(true);

    if (request->term() < currentTerm) {
        return grpc::Status::OK;
    }

    if (request->term() > currentTerm) {
        currentTerm = request->term();
        votedFor = -1;
        role = Raft::Follower;
        persistRaftState();
    }
    role = Raft::Follower;
    electionResetTime = now();
    // 本地快照 >= 参数中的快照
    if (snapshotIndex >= request->lastincludedindex()) {
        return grpc::Status::OK;
    }
    // 截断日志: 修改 commitIndex和lastApplied
    // 截断日志包括：日志长了，截断一部分，日志短了，全部清空，其实两个是一种情况
    // 但是由于现在getSlicesIndexFromLogIndex的实现，不能传入不存在logIndex，否则会panic
    auto lastLogIndex = getLastLogIndex();
    if (lastLogIndex > request->lastincludedindex()) {
        logs.erase(logs.begin(), logs.begin() + getLogicLogIndex(request->lastincludedindex()) + 1);
    } else {
        // 日志短了，全部清空;
        logs.clear();
    }
    commitIndex = std::max(commitIndex, request->lastincludedindex());
    lastAppliedIndex = std::max(lastAppliedIndex, request->lastincludedindex());
    snapshotIndex = request->lastincludedindex();
    snapshotTerm = request->lastincludedterm();

    ApplyMsg msg;
    msg.SnapshotValid = true;
    msg.Snapshot = request->snapshot();
    msg.SnapshotIndex = snapshotIndex;
    msg.SnapshotTerm = snapshotTerm;
    std::thread t(&Raft::doApply, this, msg);
    t.detach();
    auto data = doPersistRaftState();

    persist_->Save(data, msg.Snapshot);
    return grpc::Status::OK;
};

void Raft::Snapshot(int64_t logIndex, std::string &snapshot) {
    if (logIndex <= snapshotIndex || logIndex > commitIndex) {
        return;
    }
    auto lastLogIndex = getLastLogIndex();
    int64_t newSnapshotIndex = logIndex;
    int64_t newSnapshotTerm = getLogTermFromIndex(newSnapshotIndex);
    std::vector<RaftNodeRpcProtoc::LogEntry> newLogs;
    for (int i = logIndex + 1; i <= lastLogIndex; ++i) {
        auto idx = getLogicLogIndex(i);
        newLogs.emplace_back(logs[idx]);
    }
    snapshotIndex = newSnapshotIndex;
    snapshotTerm = newSnapshotTerm;
    logs = newLogs;
    auto data = doPersistRaftState();
    persist_->Save(data, snapshot);
    DPrintf("[SnapShot]Server %d snapshot snapshot index {%d}, term {%d}, log.len {%d}",
            me, snapshotIndex,
            snapshotTerm, logs.size());
}