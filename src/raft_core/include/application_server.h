//
// Created by 19327 on 2026/01/27/星期二.
//

#pragma once

#include "application_rpc.pb.h"
#include "application_rpc.grpc.pb.h"

class ApplicationServer : public ApplicationRpcProto::ApplicationRpc::Service {
public:
    grpc::Status Cmd(::grpc::ServerContext *context, const ::ApplicationRpcProto::CommandArgs *request,
                       ::ApplicationRpcProto::CommandReply *response) override;
};