//
// Created by 19327 on 2026/02/02/星期一.
//
#include <grpcpp/grpcpp.h>
#include "to_server_rpc_util.h"

ToServerRpcUtil::ToServerRpcUtil(std::string ip, short port) {
    std::string server_address{ip + ":" + std::to_string(port)};
    stub_ = ApplicationRpcProto::ApplicationRpc::NewStub(
            grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials()));
}

grpc::Status
ToServerRpcUtil::CallCmd(const ApplicationRpcProto::CommandArgs &request, ApplicationRpcProto::CommandReply &response) {
    grpc::ClientContext clientContext;
    return stub_->Cmd(&clientContext, request, &response);
}