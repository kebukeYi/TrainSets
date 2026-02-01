//
// Created by 19327 on 2026/01/30/星期五.
//
#include "raft.h"

void Raft::electionTicker(){
    while (!isStop.load(std::memory_order_relaxed)){
        while(role==Raft::Leader){
            usleep(HeartBeatTimeOut);
        }

        std::chrono::duration<signed long int, std::ratio<1, 1000000000>>
                suitableSleepTime{};
        std::chrono::system_clock::time_point wakeTime{};
        {
            mtx.lock();
            wakeTime = now();
            suitableSleepTime = getRandomizedElectionTimeout() + electionResetTime - wakeTime;
            mtx.unlock();
        }

        if (std::chrono::duration<double, std::milli>(suitableSleepTime).count() >1) {
            // 获取当前时间点
            auto start = std::chrono::steady_clock::now();

            usleep(std::chrono::duration_cast<std::chrono::microseconds>(suitableSleepTime).count());

            // 获取函数运行结束后的时间点
            auto end = std::chrono::steady_clock::now();

            // 计算时间差并输出结果（单位为毫秒）
            std::chrono::duration<double, std::milli> duration = end - start;

            // 使用ANSI控制序列将输出颜色修改为紫色
            std::cout << "\033[1;35m electionTimeOutTicker();函数设置睡眠时间为: "
                      << std::chrono::duration_cast<std::chrono::milliseconds>(suitableSleepTime).count() << " 毫秒\033[0m"
                      << std::endl;
            std::cout << "\033[1;35m electionTimeOutTicker();函数实际睡眠时间为: " << duration.count() << " 毫秒\033[0m"
                      << std::endl;
        }

        if (std::chrono::duration<double, std::milli>(electionResetTime - wakeTime).count() > 0) {
            // 说明睡眠的这段时间有重置定时器，那么就没有超时，再次睡眠;
            continue;
        }

        // 时间到了, 进行选举;
        doElection();
    }
};

void Raft::doElection(){
    std::lock_guard<std::mutex> lock(mtx);
    if (role!=Raft::Leader){
        role=Raft::Candidate;
        ++currentTerm;
        votedFor = me;
        persistRaftState();
        std::shared_ptr<int> votedNum=std::make_shared<int>(1);
        electionResetTime=now();
        for (int i = 0; i < peers.size(); ++i) {
            if (i==me){
                continue;
            }
            int64_t lastLogIndex, lastLogTerm;
            getLastLogIndexAndTerm(&lastLogIndex, &lastLogTerm);
            std::shared_ptr<RaftNodeRpcProtoc::RequestVoteArgs> args = std::make_shared<RaftNodeRpcProtoc::RequestVoteArgs>();
            args->set_term(currentTerm);
            args->set_candidateid(me);
            args->set_lastlogindex(lastLogIndex);
            args->set_lastlogterm(lastLogTerm);
            auto reply = std::make_shared<RaftNodeRpcProtoc::RequestVoteReply>();
            std::thread t(&Raft::sendRequestVote, this,i, args, reply, votedNum);
            t.detach();
        }
    }
};

bool Raft::sendRequestVote(int server, std::shared_ptr<RaftNodeRpcProtoc::RequestVoteArgs> args,
                           std::shared_ptr<RaftNodeRpcProtoc::RequestVoteReply> reply, std::shared_ptr<int> votedNum) {
    auto start = now();
    std::cout << "sendRequestVote" << std::endl;
    auto ok = peers[server]->CallRequestVote(args.get(), reply.get());
    DPrintf("[func-sendRequestVote rf{%d}] 向server{%d} 发送 RequestVote 完畢，耗時:{%d} ms", me, currentTerm,
            getLastLogIndex(), now() - start);
    if (!ok){
        DPrintf("[func-sendRequestVote rf{%d}] 向server{%d} 發送 RequestVote 失敗", me, server);
    }
    std::lock_guard<std::mutex> lock(mtx);
    if (reply->term() > currentTerm){
        currentTerm = reply->term();
        role = Raft::Follower;
        votedFor = -1;
        persistRaftState();
        return true;
    } else if (reply->term() < currentTerm){
        return false;
    }
    myAssert(reply->term() == currentTerm, "");

    if (reply->votegranted()){
        (*votedNum)++;
        if (*votedNum > peers.size()/2+1){
            role = Raft::Leader;
            electionResetTime = now();
            DPrintf("[func-sendRequestVote rf{%d}] 成為 Leader", me);
            auto lastLogIndex = getLastLogIndex();
            for (int i = 0; i < nextIndex.size(); ++i) {
                nextIndex[i]=lastLogIndex+1;
                matchIndex[i]=0;
            }
            std::thread t(&Raft::doReplication, this);
            t.detach();
            persistRaftState();
        }
    }
    return true;
};

grpc::Status Raft::RequestVote(grpc::ServerContext *context, const RaftNodeRpcProtoc::RequestVoteArgs *request,RaftNodeRpcProtoc::RequestVoteReply *response){
    std::lock_guard<std::mutex> lock(mtx);
    response->set_term(currentTerm);
    response->set_votegranted(false);
    if (request->term() < currentTerm){
        return grpc::Status::OK;
    }
    if (request->term() > currentTerm){
        currentTerm = request->term();
        role = Raft::Follower;
        votedFor = -1;
    }
    myAssert(response->term() == currentTerm,
             format("[func--rf{%d}] 前面校验过args.Term==rf.currentTerm，这里却不等", me));

    if (votedFor != -1 && votedFor != request->candidateid()) {
        // 已经投过票了;
        return grpc::Status::OK;
    }

    if (isVoteFor(request->lastlogindex(), request->lastlogterm())){
        response->set_votegranted(true);
        votedFor = request->candidateid();
        persistRaftState();
        electionResetTime=now();
    }

    return grpc::Status::OK;
};
