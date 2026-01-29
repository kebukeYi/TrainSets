//
// Created by 19327 on 2026/01/27/星期二.
//
#pragma once

#include "node_rpc.pb.h"
#include "node_rpc.grpc.pb.h"

class RaftServer : RaftNodeRpcProtoc::RaftNodeRpc::Service {
public:
    RaftServer(int port);
    ~RaftServer();

    void run();

private:
    int port;
};