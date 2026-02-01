//
// Created by 19327 on 2026/01/27/星期二.
//
#include "application_server.h"
#include "cof_config.h"

grpc::Status ApplicationServer::Cmd(::grpc::ServerContext *context, const ::ApplicationRpcProto::CommandArgs *request,
                                    ::ApplicationRpcProto::CommandReply *response) {
    auto result = state_machine->Cmd(request->command());
    if (!result.Error.empty()){
        response->set_err(result.Error);
        return grpc::Status::OK;
    }
    response->set_value(result.Value);
    return grpc::Status::OK;
}


ApplicationServer::ApplicationServer(int me, short port, int64_t maxRaftState, std::string nodeName, std::string &stateMachinePath){
    std::shared_ptr<Persisted> persisted = std::make_shared<Persisted>(me);
    maxRaftState = maxRaftState;
    applyChan=std::make_shared<LockQueue<ApplyMsg>>();
    raft=std::make_shared<Raft>();
    std::thread t([this,port]->void {
        grpc::ServerBuilder builder;
        builder.RegisterService(this);
        builder.RegisterService(raft.get());
        builder.AddListeningPort("localhost:" + std::to_string(port), grpc::InsecureServerCredentials());
        std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
        server->Wait();
    });
    t.detach();
    std::cout << "raftServer node:" << me << " start to sleep to wait all other raftnode start!!!!" << std::endl;
    sleep(6);
    std::cout << "raftServer node:" << me << " wake up!!!! start to connect other raftnode" << std::endl;
    Config config;
    config.LocalConfigFile(nodeName.c_str());
}

void ApplicationServer::ReadRaftCommandTicker(){
    while (!isShutdown.load()){

    }
}

void ApplicationServer::handleRaftCommand(ApplyMsg msg){}
void ApplicationServer::executeCommand(Op op){}
bool ApplicationServer::SendMessageToWaitChan(Op op,int64_t logIndex){}
void ApplicationServer::handleRaftSnapshot(ApplyMsg msg){}
void ApplicationServer::ReadSnapShotToInstall(std::string &snapshot){}
bool ApplicationServer::IfRequestDuplicate(int64_t clientId, int64_t requestId){}
bool ApplicationServer::IfNeedToSendSnapShotCommand(int64_t  logIndex, int proportion){}
std::string ApplicationServer::MakeSnapshot(){}