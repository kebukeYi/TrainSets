//
// Created by 19327 on 2026/01/27/星期二.
//
#include "application_server.h"
#include "cof_config.h"

grpc::Status ApplicationServer::Cmd(::grpc::ServerContext *context, const ::ApplicationRpcProto::CommandArgs *request,
                                    ::ApplicationRpcProto::CommandReply *response) {
    Op op;
    op.seqId = request->seqid();
    op.clientId = request->clientid();
    op.command = request->command();
    int64_t raftIndex = -1;
    int64_t _ = -1;
    bool isLeader = false;

    if (!IfRequestDuplicate(op.clientId, op.seqId)) {
        // 等待 raft commit 当前指令;
        raft->Start(op, &raftIndex, &_, &isLeader);
        if (!isLeader) {
            response->set_err(ErrWrongLeader);
            return grpc::Status::OK;
        }
    }

    mtx.lock();
    if (waitApplyCh.end() == waitApplyCh.find(raftIndex)) {
        waitApplyCh[raftIndex] = new LockQueue<Op>();
    }

    auto chan = waitApplyCh[raftIndex];
    mtx.unlock();

    Op opReply;
    if (!chan->timeOutPop(consensusTimeOut, &opReply)) {
        printf("raftServer node:%d, raftIndex:%ld, seqId:%ld, clientId:%ld\n", me, raftIndex, opReply.seqId,
               opReply.clientId);
        response->set_err(ErrTimeout);
        return grpc::Status::OK;
    } else {
        if (!opReply.err.empty()) {
            response->set_err(opReply.err);
            return grpc::Status::OK;
        } else {
            response->set_value(opReply.value);
            response->set_err(OK);
        }
    }

    mtx.lock();
    auto tmp = waitApplyCh[raftIndex];
    waitApplyCh.erase(raftIndex);
    delete tmp;
    mtx.unlock();
    return grpc::Status::OK;
}


ApplicationServer::ApplicationServer(int me, int64_t maxRaftState_, std::string nodeConfFilePath,
                                     std::string &stateMachineConfPath) {
    std::shared_ptr<Persisted> persisted = std::make_shared<Persisted>(me);
    maxRaftState = maxRaftState_;
    lastAppliedIndex = 0;
    isShutdown.store(false);
    applyChan = std::make_shared<LockQueue<ApplyMsg>>();
    raft = std::make_shared<Raft>();

    Config config;
    config.LocalConfigFile(nodeConfFilePath.c_str());
    std::vector<std::pair<std::string, short>> ipAndPort;
    for (int i = 0; i < config.getConfigLen(); i++) {
        // node0ip=127.0.1.1
        std::string node = "node" + std::to_string(i);
        std::string ip = config.get(node + "ip");
        std::string port = config.get(node + "port");
        // node0port=27899
        ipAndPort.emplace_back(ip, std::stoi(port));
    }

    // 获得本地 ip 和端口;
    auto localIp = ipAndPort[me];
    std::string address = "127.0.0.1:" + std::to_string(localIp.second);

    // 使用 shared_ptr 确保服务器对象的生命周期
    std::shared_ptr<grpc::Server> server;
    std::thread t([this, me, localIp, address, &server]() -> void {
        grpc::ServerBuilder builder;
        builder.RegisterService(this);
        if (raft) {
            builder.RegisterService(raft.get());
        }
        builder.AddListeningPort(address, grpc::InsecureServerCredentials());
        server = builder.BuildAndStart();
        // 增加错误检查：如果 BuildAndStart 失败，打印日志
        if (!server) {
            printf("local-raft-node:%d, address:%s start FAILED! \n", me, address.c_str());
            return;
        }
        // 保存服务器引用以便后续可以关闭
        this->grpc_server = server;
        printf("local-raft-node:%d, ip:%s, port:%d address:%s starting...\n",
               me, localIp.first.c_str(), localIp.second, address.c_str());
        // 节点启动
        server->Wait();
    });
    t.detach();

    std::cout << "raftServer node:" << me << " start to sleep to wait all other raft_node start!" << std::endl;
    sleep(8);
    std::cout << "raftServer node:" << me << " wake up!!!! start to connect other raft_node" << std::endl;

    // 进行网络连接;
    std::vector<std::shared_ptr<RaftNodeRpcUtil>> peers;
    for (int i = 0; i < ipAndPort.size(); i++) {
        if (i == me) {
            peers.push_back(nullptr);
            continue;
        }
        RaftNodeRpcUtil raftNodeRpcUtil(ipAndPort[i].first, ipAndPort[i].second);
        peers.push_back(std::make_shared<RaftNodeRpcUtil>(raftNodeRpcUtil));
    }

    sleep(ipAndPort.size() - me + 1);  // 等待所有节点相互连接成功, 再启动raft;

    printf("raftServer node:%d, start to init raft\n", me);

    raft->init(me, peers, persisted, applyChan);

    state_machine = std::make_unique<StateMachine>(stateMachineConfPath);
    if (!state_machine->init()) {
        std::cout << "state_machine init failed" << std::endl;
        return;
    }

    auto snapshot = persisted->LoadSnapshot(); // 固定路径的快照文件;
    if (!snapshot.empty()) {
        ReadSnapShotToInstall(snapshot);
    }

    std::thread loop(&ApplicationServer::ReadRaftCommandTicker, this);
    loop.join(); // 阻塞调用线程，直到目标线程执行完毕;
    // loop.detach(); // 分离线程，使其独立运行，不阻塞调用线程;
}

void ApplicationServer::ReadRaftCommandTicker() {
    while (!isShutdown.load()) {
        auto msg = applyChan->Pop();
        DPrintf("[ApplicationServer::ReadRaftApplyCommandLoop()-kvServer{%d}] 收到了来自raft的消息", me);
        if (msg.CommandValid) {
            DPrintf("[ApplicationServer::ReadRaftApplyCommandLoop()-kvServer{%d}] 收到了来自raft的命令", me);
            handleRaftCommand(msg);
        } else if (msg.SnapshotValid) {
            DPrintf("[ApplicationServer::ReadRaftApplyCommandLoop()-kvServer{%d}] 收到了来自raft的快照", me);
            handleRaftSnapshot(msg);
        }
    }
}

void ApplicationServer::handleRaftCommand(ApplyMsg msg) {
    Op op;
    op.decodeFromString(msg.Command);
    if (msg.CommandIndex <= lastAppliedIndex) {
        DPrintf("[ApplicationServer::handleRaftCommand()-kvServer{%d}] 收到的命令索引{%ld}小于等于lastAppliedIndex{%ld}",
                me, msg.CommandIndex, lastAppliedIndex);
        return;
    }
    Op opReply;
    if (IfRequestDuplicate(op.clientId, op.seqId)) {
        DPrintf("[ApplicationServer::IfRequestDuplicate-kvServer{%d}] 客户端{%ld}的请求{%ld}重复;",me, op.clientId, op.seqId);
        auto resp = respSimpleString("OK");
        opReply.value = resp;
    } else {
        lastAppliedIndex = msg.CommandIndex;
        // 执行到状态机中;
        executeCommand(op, &opReply);
        // 是否需要持久化 raft 元信息;
        if (maxRaftState != -1) {
            IfNeedToSendSnapShotCommand(msg.CommandIndex, 9);
        }
    }

    // 发送给 client 通道;
    SendMessageToWaitChan(opReply, msg.CommandIndex);
}

void ApplicationServer::executeCommand(Op &op, Op *opReply) {
    std::lock_guard<std::mutex> lock(mtx);
    opReply->clientId = op.clientId;
    opReply->seqId = op.seqId;
    // 状态机执行;
    auto result = state_machine->Cmd(op.command);
    if (!result.Error.empty()) {
        DPrintf("[ApplicationServer::executeCommand-kvserver{%d}] 执行 raft 命令出错, 错误信息: %s", me,result.Error.c_str());
        opReply->err = result.Error;
        opReply->value = "";
        return;
    } else {
        opReply->value = result.Value;
        opReply->err = "";
    }
    lastRequestId[op.clientId] = op.seqId; // 更新最后一个请求的 clientId 和 seqId;
}

bool ApplicationServer::SendMessageToWaitChan(Op &opReply, int64_t logIndex) {
    std::lock_guard<std::mutex> lock(mtx);
    auto it = waitApplyCh.find(logIndex);
    if (it == waitApplyCh.end()) {
        waitApplyCh[logIndex] = new LockQueue<Op>();
    }
    waitApplyCh[logIndex]->Push(opReply);
    return true;
}

void ApplicationServer::handleRaftSnapshot(ApplyMsg msg) {
    std::lock_guard<std::mutex> lock(mtx);
    ReadSnapShotToInstall(msg.Snapshot);
    lastAppliedIndex = msg.SnapshotIndex;
}

void ApplicationServer::ReadSnapShotToInstall(std::string &snapshot) {
    if (!snapshot.empty()) {
        state_machine->parseSnapshot(snapshot);
    }
}

bool ApplicationServer::IfRequestDuplicate(int64_t clientId, int64_t requestId) {
    std::lock_guard<std::mutex> lock(mtx);
    if (lastRequestId.find(clientId) != lastRequestId.end() && lastRequestId[clientId] >= requestId) {
        return true;
    }
    return false;
}

bool ApplicationServer::IfNeedToSendSnapShotCommand(int64_t logIndex, int proportion) {
    if (proportion <= 0) {
        proportion = 10;
    }
    if (raft->getRaftStateSize() > maxRaftState / proportion) {
        std::string snapshot = MakeSnapshot();
        raft->Snapshot(logIndex, snapshot);
    }
    return true;
}

std::string ApplicationServer::MakeSnapshot() {
    std::lock_guard<std::mutex> lock(mtx);
    auto snapshot = state_machine->genSnapshot();
    return snapshot;
}

void ApplicationServer::close() {
    raft->close();
    state_machine->close();
    isShutdown.store(true);
}