//
// Created by 19327 on 2026/01/30/星期五.
//
#pragma once

#include <grpcpp/grpcpp.h>
#include "node_rpc.pb.h"
#include "node_rpc.grpc.pb.h"

class RaftNodeRpcUtil {
private:
    std::shared_ptr<RaftNodeRpcProtoc::RaftNodeRpc::Stub> stub;

public:
    RaftNodeRpcUtil(std::string address, short port);

    ~RaftNodeRpcUtil();

    grpc::Status CallAppendEntries(RaftNodeRpcProtoc::AppendEntriesArgs *args, RaftNodeRpcProtoc::AppendEntriesReply *reply);

    grpc::Status CallRequestVote(RaftNodeRpcProtoc::RequestVoteArgs *args, RaftNodeRpcProtoc::RequestVoteReply *reply);

    grpc::Status
    CallInstallSnapshot(RaftNodeRpcProtoc::InstallSnapshotArgs *args, RaftNodeRpcProtoc::InstallSnapshotReply *reply);
};