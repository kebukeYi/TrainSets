//
// Created by 19327 on 2026/01/30/星期五.
//
#include "raft.h"

void Raft::replicationTicker() {
    while (!isStop.load(std::memory_order_relaxed)) {
        while (role != Raft::Leader) {
            usleep(1000 * HeartBeatTimeOut);
        }

        std::chrono::duration<signed long int, std::ratio<1, 1000000000>> suitableSleepTime{};
        std::chrono::system_clock::time_point wakeTime{};
        {
            mtx.lock();
            wakeTime = now();
            // 这一次的合适的 睡眠时间;
            suitableSleepTime = getRandomizedReplicationTimeout() + heartbeatResetTime - wakeTime;
            mtx.unlock();
        }

        if (std::chrono::duration<double, std::milli>(suitableSleepTime).count() > 1) {
            // 获取当前时间点
            auto start = std::chrono::steady_clock::now();

            usleep(std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count());

            // 获取函数运行结束后的时间点
            auto end = std::chrono::steady_clock::now();

            // 计算时间差并输出结果（单位为毫秒）
            std::chrono::duration<double, std::milli> duration = end - start;

            // 使用ANSI控制序列将输出颜色修改为紫色
//            std::cout << "\033[1;35m replicationTicker();函数设置睡眠时间为: "
//                      << std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count()
//                      << " 毫秒\033[0m"
//                      << std::endl;
//            std::cout << "\033[1;35m replicationTicker();函数实际睡眠时间为: " << duration.count() << " 毫秒\033[0m" << std::endl;
        }

        if (std::chrono::duration<double, std::milli>(heartbeatResetTime - wakeTime).count() > 0) {
            // 说明睡眠的这段时间有重置定时器，那么就没有超时，再次睡眠;
            continue;
        }

        // 时间到了, 进行下发日志;
        //DPrintf("[replicationTicker-raft{%d}] 时间到了,进行下发日志;\n", me);
        doReplication();
    }
};

void Raft::doReplication() {
    // 下发日志来代替心跳
    std::lock_guard<std::mutex> lock(mtx);
    if (role == Raft::Leader) {
        auto nodeNums = std::make_shared<int>(1);
        for (int i = 0; i < peers.size(); ++i) {
            if (i == me) {
                // 逻辑上序列号;
                nextIndex[i] = getLastLogIndex() + 1;
                matchIndex[i] = getLastLogIndex();
                continue;
            }

            int64_t prevLogIndex, prevLogTerm;
            getPrevLogInfo(i, &prevLogIndex, &prevLogTerm);

            if (prevLogIndex < snapshotIndex) {
                DPrintf("[func-Raft::doReplication-raft{%d}] leader 向节点{%d}发送快照", me, i);
                std::thread t1(&Raft::sendSnapshot, this, i);
                t1.detach();
                continue;
            }

            auto appendEntriesArgs = std::make_shared<RaftNodeRpcProtoc::AppendEntriesArgs>();
            appendEntriesArgs->set_leaderid(me);
            appendEntriesArgs->set_curterm(currentTerm);
            appendEntriesArgs->set_prevlogindex(prevLogIndex);
            appendEntriesArgs->set_prevlogterm(prevLogTerm);
            appendEntriesArgs->set_leadercommitindex(commitIndex);
            appendEntriesArgs->clear_entries();

            // logIndex: 120
            // 快照:100
            // 物理上: 20+1 = 21
            // 逻辑上
            // 物理上
            // 逻辑区间: [prevLogIndex + 1, lastLogIndex]
            auto maybeIndex = prevLogIndex + 1;
            auto endLogIndex = getLastLogIndex();
            // DPrintf("[func-Raft::doReplication-raft{%d}] leader 向节点{%d}发送AE rpc 前, maybeIndex:{%ld}, endLogIndex:{%ld},log.size: %d",me, i, maybeIndex, endLogIndex, logs.size());
            // 逻辑值 > 现存的逻辑值
            if (maybeIndex > endLogIndex) {
            // DPrintf("[func-Raft::doReplication-raft{%d}] leader 不需要向节点{%d}发送AE rpc , maybeIndex:{%ld}, endLogIndex:{%ld},log.size: %d",me, i, maybeIndex, endLogIndex, logs.size());
            } else {
                auto startIndex = getRealLogIndex(maybeIndex);
                for (int j = startIndex; j < logs.size(); ++j) {
                    RaftNodeRpcProtoc::LogEntry *entry = appendEntriesArgs->add_entries();
                    *entry = logs[j];
                }
            }

            // leader对每个节点发送的日志长短不一，但是都保证从prevIndex发送直到最后
            myAssert(appendEntriesArgs->prevlogindex() + appendEntriesArgs->entries_size() == endLogIndex,
                     format("appendEntriesArgs.PrevLogIndex{%d}+len(appendEntriesArgs.Entries){%ld} != endLogIndex{%ld}",
                            appendEntriesArgs->prevlogindex(), appendEntriesArgs->entries_size(), endLogIndex));

            if (appendEntriesArgs->entries_size() > 0){
                DPrintf("[func-Raft::doReplication-raft{%d}] leader 向节点{%d}发送AE rpc 中, args->entries_size():{%d}",me, i, appendEntriesArgs->entries_size());
            }

            auto reply = std::make_shared<RaftNodeRpcProtoc::AppendEntriesReply>();
            std::thread t1(&Raft::sendAppendEntries, this, i, appendEntriesArgs, reply, nodeNums);
            t1.detach();
        }
        heartbeatResetTime = now();
    }
};

bool Raft::sendAppendEntries(int server, std::shared_ptr<RaftNodeRpcProtoc::AppendEntriesArgs> args,
                             std::shared_ptr<RaftNodeRpcProtoc::AppendEntriesReply> reply,
                             std::shared_ptr<int> nodeNums) {
    auto peer = peers[server];
    if (peer == nullptr) {
        DPrintf("[func-Raft::sendAppendEntries-raft{%d}] peer{%d} is nullptr; exit;", me, server);
        return false;
    }
    auto status = peer->CallAppendEntries(args.get(), reply.get());
    if (!status.ok()) {
        DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc失败", me, server);
        return false;
    }

    std::lock_guard<std::mutex> lock(mtx);
    if (reply->term() > currentTerm) {
        currentTerm = reply->term();
        role = Raft::Follower;
        votedFor = -1;
        persistRaftState();
        return false;
    }

    if (role != Raft::Leader) {
        return false;
    }
    if (!reply->success()) {
        auto prevNextLogIndex = nextIndex[server];
        if (reply->term() == InvalidTerm) {
            nextIndex[server] = reply->nextindex();
        } else {
            auto firstLogIndex = getFirstLogIndex(reply->term());
            if (firstLogIndex != InvalidIndex) {
                nextIndex[server] = firstLogIndex;
            } else {
                nextIndex[server] = reply->nextindex();
            }
        }

        if (nextIndex[server] > prevNextLogIndex) {
            nextIndex[server] = prevNextLogIndex;
        }

        auto nextPrevLogIndex = nextIndex[server];
        auto nextPrevTerm = InvalidTerm;
        if (nextPrevLogIndex >= snapshotIndex) {
            nextPrevTerm = logs[getRealLogIndex(nextPrevLogIndex)].logterm();
        }
        DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc not match, nextIndex{%ld} -> {%ld}",
                me, server, nextPrevLogIndex, nextPrevTerm);
        return false;
    }

    // DPrintf("[func-Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc成功", me, server);
    // 如果发送成功, 但是follower节点并没有把 log[]应用到状态机中, 这该怎么办?
    matchIndex[server] = std::max(matchIndex[server], args->prevlogindex() + args->entries_size());
    nextIndex[server] = matchIndex[server] + 1;

    auto majorityIndex = getMajorityIndexLocked();
    // leader 不能随便提交日志; 只能提交自己任期内的日志;
    if (majorityIndex > commitIndex && logs[majorityIndex].logterm() == currentTerm) {
        DPrintf("[Raft::sendAppendEntries-raft{%d}] leader 向节点{%d}发送AE rpc成功, 更新majorityIndex{%ld}", me, server, majorityIndex);
        commitIndex = majorityIndex;
    }
    return true;
}

int64_t Raft::getMajorityIndexLocked() {
    std::vector<int64_t> matchIndexCopy = matchIndex;
    std::sort(matchIndexCopy.begin(), matchIndexCopy.end());
    return matchIndexCopy[(matchIndexCopy.size() - 1) / 2];
}

grpc::Status Raft::AppendEntries(grpc::ServerContext *context, const RaftNodeRpcProtoc::AppendEntriesArgs *request,
                                 RaftNodeRpcProtoc::AppendEntriesReply *response) {
    std::lock_guard<std::mutex> lock(mtx);
    response->set_term(currentTerm);
    response->set_success(false);

    if (request->curterm() < currentTerm) {
        // 直接返回自己的大term;
        return grpc::Status::OK;
    }

    if (request->curterm() >= currentTerm) {
        role = Raft::Follower;
        currentTerm = request->curterm();
        votedFor = -1;
    }

    electionResetTime = now();
    // 0任期 size()索引
    if (request->prevlogindex() > getLastLogIndex()) {
        response->set_term(InvalidTerm); // 日志过短;
        response->set_nextindex(getLastLogIndex() + 1);
        return grpc::Status::OK;
    }

    // 快照任期 快照索引
    // 这种情况会发生吗?
    // 既然follower节点的快照索引有值,那么说明之前leader下发过快照;
    // 前提leader下发快照, 那么就说明这部分数据已经被大多数提交了,
    if (snapshotIndex > request->prevlogindex()) {
        response->set_term(snapshotTerm); // 日志过短;
        response->set_nextindex(snapshotIndex);
        return grpc::Status::OK;
    }

    if (isMatchLog(request->prevlogindex(), request->prevlogterm())) {
        // 不能直接截断，必须一个一个检查，因为发送来的log可能是之前的，直接截断可能导致“取回”已经在follower日志中的条目
        // 可能会有一段发来的AE中的logs中前半是匹配的，后半是不匹配的;
        // 1.follower如何处理?
        // 2.如何给leader回复?
        // 3. leader如何处理?
        std::string logsIDs;
        for (int i = 0; i < request->entries_size(); ++i) {
            auto log = request->entries(i);
            if (log.logindex() > getLastLogIndex()) {
                logs.push_back(log);
                logsIDs += std::to_string(log.logindex()) + " , ";
            } else {
                // 有可能是重复的数据;
                // index 相同，term 相同，command 不同;
                auto logIndex = getRealLogIndex(log.logindex());
                if (logs[logIndex].logterm() == log.logterm() && logs[logIndex].command() != log.command()) {
                    DPrintf("[func-Raft::AppendEntries-raft{%d}] follower 向leader{%d}发送AE rpc失败，日志重复",
                            me, request->leaderid());
                    response->set_term(logs[logIndex].logterm());
                    response->set_nextindex(logIndex);
                    response->set_success(false);
                    return grpc::Status::OK;
                }
                // 目前比较是否 term 相等;
                if (logs[logIndex].logterm() != log.logterm()) {
                    // 不匹配就更新, 以leader 下发的 log 为主;
                    logs[logIndex] = log;
                }
            }
        }

        if (request->leadercommitindex() > commitIndex) {
            DPrintf("[Raft::AppendEntries-raft{%d}] follower 保存 leader{%d}发送的AE, leader.CommitIndex{%ld}",
                    me, request->leaderid(), request->leadercommitindex());
            commitIndex = std::min(request->leadercommitindex(), getLastLogIndex());
        }

        myAssert(getLastLogIndex() >= request->prevlogindex() + request->entries_size(),
                 format("[func-AppendEntries1-rf{%d}] rf.getLastLogIndex(){%ld} != args.PrevLogIndex{%ld}+len(args.Entries){%d}",
                        me, getLastLogIndex(), request->prevlogindex(), request->entries_size()));

        if (request->entries_size() > 0) {
            DPrintf("[Raft::AppendEntries-raft{%d}] follower 保存 leader{%d}发送的AE(%s),size: %ld",
                    me, request->leaderid(), logsIDs.c_str(), request->entries_size());
        }
        response->set_success(true);
    } else {
        // 前一个 日志不匹配;
        auto conflictTerm = logs[getRealLogIndex(request->prevlogindex())].logterm();
        response->set_term(conflictTerm);
        auto conflictIndex = getFirstLogIndex(conflictTerm);
        response->set_nextindex(conflictIndex);
        response->set_success(false);
    }
    electionResetTime = now();
    return grpc::Status::OK;
};
