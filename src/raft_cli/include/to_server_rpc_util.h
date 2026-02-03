//
// Created by 19327 on 2026/02/02/星期一.
//
#pragma once

#include "application_rpc.pb.h"
#include "application_rpc.grpc.pb.h"

class ToServerRpcUtil {
public:
    std::shared_ptr<ApplicationRpcProto::ApplicationRpc::Stub> stub_;
public:
    ToServerRpcUtil(std::string server_address, short port);

    ~ToServerRpcUtil() = default;

    grpc::Status CallCmd(const ApplicationRpcProto::CommandArgs &request, ApplicationRpcProto::CommandReply &response);
};