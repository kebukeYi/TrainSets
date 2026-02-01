//
// Created by 19327 on 2026/01/31/星期六.
//

#include "raft_node_rpc_util.h"

RaftNodeRpcUtil::RaftNodeRpcUtil(std::string ip, short port) {
    std::string server_address{ip + ":" + std::to_string(port)};
    auto channel = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
    stub = std::move(RaftNodeRpcProtoc::RaftNodeRpc::NewStub(channel));
}
grpc::Status RaftNodeRpcUtil::CallRequestVote(RaftNodeRpcProtoc::RequestVoteArgs *args,
                                      RaftNodeRpcProtoc::RequestVoteReply *reply) {
    grpc::ClientContext context;
    return stub->RequestVote(&context, *args, reply);
}
grpc::Status RaftNodeRpcUtil::CallAppendEntries(RaftNodeRpcProtoc::AppendEntriesArgs *args,
                                        RaftNodeRpcProtoc::AppendEntriesReply *reply) {
    grpc::ClientContext context;
    return stub->AppendEntries(&context, *args, reply);
}
grpc::Status RaftNodeRpcUtil::CallInstallSnapshot(RaftNodeRpcProtoc::InstallSnapshotArgs *args,
                                          RaftNodeRpcProtoc::InstallSnapshotReply *reply) {
    grpc::ClientContext context;
    return stub->InstallSnapshot(&context, *args, reply);
}