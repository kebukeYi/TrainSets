//
// Created by 19327 on 2026/01/27/星期二.
//
#pragma once

#include <unordered_map>
#include "application_rpc.pb.h"
#include "application_rpc.grpc.pb.h"
#include "state_machine.h"
#include "raft.h"
#include <grpcpp/grpcpp.h>

class ApplicationServer : public ApplicationRpcProto::ApplicationRpc::Service {
private:
    std::atomic<bool> isShutdown;
    int me;
    std::mutex mtx;
    std::shared_ptr<Raft> raft;
    std::shared_ptr<LockQueue<ApplyMsg>> applyChan;
    int64_t maxRaftState;
    std::unique_ptr<StateMachine> state_machine;
    std::shared_ptr<grpc::Server> grpc_server;

    // key: clientId, value: waitApplyCh
    std::unordered_map<int64_t , LockQueue<Op> *> waitApplyCh;
    // key: clientId, value: lastRequestId
    std::unordered_map<int64_t, int64_t> lastRequestId;
    int64_t lastAppliedIndex;
public:
    ApplicationServer() = delete;
    ~ApplicationServer()=default;
    ApplicationServer(int me, int64_t maxRaftState, std::string nodeConfFilePath, std::string &stateMachineConfPath);

    void ReadRaftCommandTicker();
    void handleRaftCommand(ApplyMsg msg);
    void executeCommand(Op &op, Op *opReply);
    bool SendMessageToWaitChan(Op &op,int64_t logIndex);

    void handleRaftSnapshot(ApplyMsg msg);
    void ReadSnapShotToInstall(std::string &snapshot);
    bool IfRequestDuplicate(int64_t clientId, int64_t requestId);
    bool IfNeedToSendSnapShotCommand(int64_t  logIndex, int proportion);
    std::string MakeSnapshot();
    void close();
public:
    grpc::Status Cmd(::grpc::ServerContext *context,
                     const ::ApplicationRpcProto::CommandArgs *request,
                     ::ApplicationRpcProto::CommandReply *response) override;
};