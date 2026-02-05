//
// Created by 19327 on 2026/01/27/星期二.
//
#pragma once

#include <boost/serialization/serialization.hpp>
#include <boost/serialization/string.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>

#include "config.h"
#include "monsoon.h"
#include "apply_msg.h"
#include "util.h"
#include "persisted.h"
#include "raft_node_rpc_util.h"
#include "raft_node_rpc.pb.h"
#include "raft_node_rpc.grpc.pb.h"

class Raft : public RaftNodeRpcProtoc::RaftNodeRpc::Service {

private:
    std::atomic<bool> isStop;
    std::mutex mtx;
    std::vector<std::shared_ptr<RaftNodeRpcUtil>> peers;
    std::shared_ptr<Persisted> persist_;

    int me;
    int64_t currentTerm;
    int64_t votedFor;
    std::vector<RaftNodeRpcProtoc::LogEntry> logs;
    int64_t snapshotIndex;
    int64_t snapshotTerm;

    int64_t commitIndex; // 逻辑序列号
    int64_t lastAppliedIndex; // 逻辑序列号
    std::vector<int64_t> nextIndex; // 逻辑序列号
    std::vector<int64_t> matchIndex;
    enum Role {
        Leader, Follower, Candidate
    };
    Role role;
    std::shared_ptr<LockQueue<ApplyMsg>> applyChan;

    std::chrono::system_clock::time_point electionResetTime;
    std::chrono::system_clock::time_point heartbeatResetTime;

    std::unique_ptr<monsoon::IOManager> ioManager;
private:
    class BoostPersistRaftNode {
    public:
        friend class boost::serialization::access;

        int64_t term;
        int64_t votedFor;
        int64_t snapshotIndex;
        int64_t snapshotTerm;
        std::vector<std::string> logs;

        // When the class Archive corresponds to an output archive, the
        // & operator is defined similar to <<.  Likewise, when the class Archive
        // is a type of input archive the & operator is defined similar to >>.
        template<class A>
        void serialize(A &ar, const unsigned int version) {
            ar & term;
            ar & votedFor;
            ar & snapshotIndex;
            ar & snapshotTerm;
            ar & logs;
        }
    };

public:
    Raft() = default;

    ~Raft() = default;

    void init(int me, std::vector<std::shared_ptr<RaftNodeRpcUtil>> peers, std::shared_ptr<Persisted> persist,
              std::shared_ptr<LockQueue<ApplyMsg>> applyChan);

    void Start(Op op, int64_t *logIndex, int64_t *logTerm, bool *isLeader);

    void close();

public:
    void electionTicker();

    void doElection();

    bool sendRequestVote(int server, std::shared_ptr<RaftNodeRpcProtoc::RequestVoteArgs> args,
                         std::shared_ptr<RaftNodeRpcProtoc::RequestVoteReply> reply, std::shared_ptr<int> votedNum);

    grpc::Status RequestVote(grpc::ServerContext *context, const RaftNodeRpcProtoc::RequestVoteArgs *request,
                             RaftNodeRpcProtoc::RequestVoteReply *response) override;

    void replicationTicker();

    void doReplication();

    bool sendAppendEntries(int server, std::shared_ptr<RaftNodeRpcProtoc::AppendEntriesArgs> args,
                           std::shared_ptr<RaftNodeRpcProtoc::AppendEntriesReply> reply, std::shared_ptr<int> nodeNums);

    grpc::Status AppendEntries(grpc::ServerContext *context, const RaftNodeRpcProtoc::AppendEntriesArgs *request,
                               RaftNodeRpcProtoc::AppendEntriesReply *response) override;

    bool sendSnapshot(int server);

    grpc::Status InstallSnapshot(grpc::ServerContext *context, const RaftNodeRpcProtoc::InstallSnapshotArgs *request,
                                 RaftNodeRpcProtoc::InstallSnapshotReply *response) override;

    void applyTicker();

    std::vector<ApplyMsg> getApplyMsgs();

    void doApply(ApplyMsg msg);

    void Snapshot(int64_t logIndex, std::string &snapshot);

    void getStateForCli(int64_t *term, bool *isLeader);

public:
    int64_t getNextCommandIndex();

    void getPrevLogInfo(int server, int64_t *prevLogIndex, int64_t *prevLogTerm);

    bool isMatchLog(int64_t prevLogIndex, int64_t prevLogTerm);

    bool isVoteFor(int64_t candidateLogIndex, int64_t candidateLogTerm);

    int64_t getLastLogIndex();

    int64_t getLastLogTerm();

    void getLastLogIndexAndTerm(int64_t *lastLogIndex, int64_t *lastLogTerm);

    int64_t getLogTermFromIndex(int64_t logIndex);

    int64_t getFirstLogIndex(int64_t term);

    int64_t getRealLogIndex(int64_t logIndex);

    int64_t getTailLogIndex(int64_t logIndex);

    int64_t getMajorityIndexLocked();

    ssize_t getRaftStateSize();

    void persistRaftState();

    std::string doPersistRaftState();

    void readPersistRaftState(std::string &data);
};