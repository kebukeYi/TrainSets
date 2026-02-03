//
// Created by 19327 on 2026/01/30/星期五.
//
#include "raft.h"

void Raft::electionTicker() {
    while (!isStop.load(std::memory_order_relaxed)) {

//        while (role == Raft::Leader) {
//            usleep(HeartBeatTimeOut);
//        }
//
//        std::chrono::duration<signed long int, std::ratio<1, 1000000000>> suitableSleepTime{};
//        std::chrono::system_clock::time_point wakeTime{};
//        {
//            mtx.lock();
//            wakeTime = now();
//            if ((role != Raft::Leader)&&(wakeTime-electionResetTime>getRandomizedElectionTimeout())){
//                // 时间到了, 进行选举;
//                DPrintf("[electionTicker().rf{%d}] 选举超时, 开始进行选举", me);
//                doElection();
//            }
//            suitableSleepTime = electionResetTime + getRandomizedElectionTimeout() - wakeTime;
//            mtx.unlock();
//        }
//
//        if (std::chrono::duration<double, std::milli>(suitableSleepTime).count() > 1) {
//            // 获取当前时间点
//            auto start = std::chrono::steady_clock::now();
//
//            usleep(std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count());
//            // std::this_thread::sleep_for(suitableSleepTime);
//
//            // 获取函数运行结束后的时间点
//            auto end = std::chrono::steady_clock::now();
//
//            // 计算时间差并输出结果（单位为毫秒）
//            std::chrono::duration<double, std::milli> duration = end - start;
//
//            // 使用ANSI控制序列将输出颜色修改为紫色
////            std::cout << "\033[1;35m electionTimeOutTicker(rf{ld%}).函数设置睡眠时间为: "<< me <<
////                      << std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count() << " 毫秒\033[0m"
////                      << std::endl;
//            std::cout << "\033[1;35m electionTimeOutTicker(rf{ld%}).函数实际睡眠时间为: " << me << duration.count()
//                      << " 毫秒\033[0m"
//                      << std::endl;
//        }
//
//        // 说明睡眠的这段时间有重置定时器, 那么就没有超时, 再次睡眠;
//        if (std::chrono::duration<double, std::milli>(electionResetTime - wakeTime).count() > 0) {
//            DPrintf("[electionTicker().rf{%d}] 选举超时，但在此期间有重置定时器，再次睡眠;", me);
//            continue;
//        }

        mtx.lock();
        auto wakeTime = now();
        // 最大 400 ms 就得进行选举
        if ((role != Raft::Leader) && (wakeTime - electionResetTime > getRandomizedElectionTimeout())) {
            // 时间到了, 进行选举;
            DPrintf("[electionTicker().rf{%d}] 选举超时, 开始进行选举", me);
            std::thread t(&Raft::doElection, this);
            t.detach();
        }
        mtx.unlock();

        // 随机睡眠一段时间, 再进行选举;
        sleepForNMilliseconds(getRandomTimeout() + 2000);
    }
};

void Raft::doElection() {
    std::lock_guard<std::mutex> lock(mtx);
    if (role != Raft::Leader) {
        role = Raft::Candidate;
        ++currentTerm;
        votedFor = me;
        persistRaftState();
        std::shared_ptr<int> votedNum = std::make_shared<int>(1);
        electionResetTime = now();

        for (int i = 0; i < peers.size(); ++i) {
            if (i == me) {
                continue;
            }
            DPrintf("[doElection().rf{%d}-term{%ld}] 向 server{%d} 发送 RequestVote", me, currentTerm, i);
            int64_t lastLogIndex, lastLogTerm;
            getLastLogIndexAndTerm(&lastLogIndex, &lastLogTerm);
            std::shared_ptr<RaftNodeRpcProtoc::RequestVoteArgs> args = std::make_shared<RaftNodeRpcProtoc::RequestVoteArgs>();
            args->set_term(currentTerm);
            args->set_candidateid(me);
            args->set_lastlogindex(lastLogIndex);
            args->set_lastlogterm(lastLogTerm);
            auto reply = std::make_shared<RaftNodeRpcProtoc::RequestVoteReply>();
            std::thread t(&Raft::sendRequestVote, this, i, args, reply, votedNum);
            t.detach();
        }
    }
};

bool Raft::sendRequestVote(int server, std::shared_ptr<RaftNodeRpcProtoc::RequestVoteArgs> args,
                           std::shared_ptr<RaftNodeRpcProtoc::RequestVoteReply> reply, std::shared_ptr<int> votedNum) {
    auto start = now();
    auto status = peers[server]->CallRequestVote(args.get(), reply.get());

    if (!status.ok()) {
        DPrintf("[sendRequestVote-rf{%d}] 向 server{%d} 发送 RequestVote 失败; 原因:%s",
                me, server, status.error_message().c_str());
        sleep(3);
    } else {
        DPrintf("[sendRequestVote-rf{%d}] 向 server{%d} 发送 RequestVote 成功; 响应结果: %s, %s",
                me, server, reply->term(), reply->votegranted());
    }

    std::lock_guard<std::mutex> lock(mtx);
    if (reply->term() > currentTerm) {
        printf("[sendRequestVote-rf{%d}] 服务器(%d)返回的 term 大于当前 term; 服务器 term: {%ld}, 我的 term: {%ld}\n",
               me, server, reply->term(), currentTerm);
        currentTerm = reply->term();
        role = Raft::Follower;
        votedFor = -1;
        persistRaftState();
        return true;
    }

    myAssert(reply->term() <= currentTerm, "reply->term() <= currentTerm");

    if (reply->votegranted()) {
        (*votedNum)++;
        if (*votedNum > peers.size() / 2) {
            role = Raft::Leader;
            electionResetTime = now();
            DPrintf("[func-sendRequestVote rf{%d}] 成为 Leader", me);
            auto lastLogIndex = getLastLogIndex();
            for (int i = 0; i < nextIndex.size(); ++i) {
                nextIndex[i] = lastLogIndex + 1;
                matchIndex[i] = 0;
            }
            std::thread t(&Raft::doReplication, this);
            t.detach();
            persistRaftState();
        }
    }
    return true;
};

grpc::Status Raft::RequestVote(grpc::ServerContext *context, const RaftNodeRpcProtoc::RequestVoteArgs *request,
                               RaftNodeRpcProtoc::RequestVoteReply *response) {
    std::lock_guard<std::mutex> lock(mtx);
    response->set_term(currentTerm);
    response->set_votegranted(false);

    if (request->term() < currentTerm) {
        return grpc::Status::OK;
    }

    if (request->term() > currentTerm) {
        currentTerm = request->term();
        role = Raft::Follower;
        votedFor = -1;
    }

    myAssert(request->term() == currentTerm,
             format("[func--rf{%d}] 前面校验过args.Term==rf.currentTerm，这里却不等", me));

    if (votedFor != -1 && votedFor != request->candidateid()) {
        // 已经投过票了;
        return grpc::Status::OK;
    }

    if (isVoteFor(request->lastlogindex(), request->lastlogterm())) {
        response->set_votegranted(true);
        votedFor = request->candidateid();
        electionResetTime = now();
        persistRaftState();
    }

    return grpc::Status::OK;
};
