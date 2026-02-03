//
// Created by 19327 on 2026/01/27/星期二.
//
#include "application_server.h"
#include "cof_config.h"

grpc::Status ApplicationServer::Cmd(::grpc::ServerContext *context, const ::ApplicationRpcProto::CommandArgs *request,
                                    ::ApplicationRpcProto::CommandReply *response) {
    auto result = state_machine->Cmd(request->command());
    if (!result.Error.empty()) {
        response->set_err(result.Error);
        return grpc::Status::OK;
    }
    response->set_value(result.Value);
    response->set_err(OK);
    return grpc::Status::OK;
}


ApplicationServer::ApplicationServer(int me, int64_t maxRaftState, std::string nodeConfFilePath,
                                     std::string &stateMachineConfPath) {
    std::shared_ptr<Persisted> persisted = std::make_shared<Persisted>(me);
    maxRaftState = maxRaftState;
    isShutdown.store(false);
    applyChan = std::make_shared<LockQueue<ApplyMsg>>();
    raft = std::make_shared<Raft>();

    Config config;
    config.LocalConfigFile(nodeConfFilePath.c_str());
    std::vector<std::pair<std::string, short>> ipAndPort;
    // ipAndPort.resize(config.getConfigLen());
    for (int i = 0; i < config.getConfigLen(); i++) {
        // node0ip=127.0.1.1
        std::string node = "node" + std::to_string(i);
        std::string ip = config.get(node + "ip");
        // node0port=27899
        ipAndPort.emplace_back(ip, std::stoi(config.get(node + "port")));
    }
    // 获得本地 ip 和端口;
    auto localIp = ipAndPort[me];
    auto address = localIp.first + ":" + std::to_string(localIp.second);

    // 使用 shared_ptr 确保服务器对象的生命周期
    std::shared_ptr<grpc::Server> server;
    std::thread t([this, me, localIp, address, &server]() -> void {
        grpc::ServerBuilder builder;
        builder.RegisterService(this);
        builder.RegisterService(raft.get());
        builder.AddListeningPort(address, grpc::InsecureServerCredentials());
        server = builder.BuildAndStart();
        // 增加错误检查：如果 BuildAndStart 失败，打印日志
        if (!server) {
            printf("local-raft-node:%d, address:%s start FAILED!\n", me, address.c_str());
            return;
        }
        // 保存服务器引用以便后续可以关闭
        this->grpc_server = server;
        printf("local-raft-node:%d, ip:%s, port:%d address:%s starting...\n", me, localIp.first.c_str(), localIp.second,
               address.c_str());
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
        peers.push_back(std::make_shared<RaftNodeRpcUtil>(ipAndPort[i].first, ipAndPort[i].second));
    }

    sleep(ipAndPort.size() - me + 1);  // 等待所有节点相互连接成功, 再启动raft;

    printf("raftServer node:%d, start to init raft\n", me);
    raft->init(me, peers, persisted, applyChan);

    state_machine = std::make_unique<StateMachine>(stateMachineConfPath);
    if (!state_machine->init()) {
        std::cout << "state_machine init failed" << std::endl;
        return;
    };

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
        DPrintf("--------tmp----[func-KvServer::ReadRaftApplyCommandLoop()-kvserver{%d}] 收到了下raft的消息", me);
        if (msg.CommandValid) {
            handleRaftCommand(msg);
        } else if (msg.SnapshotValid) {
            handleRaftSnapshot(msg);
        }
    }
}

void ApplicationServer::handleRaftCommand(ApplyMsg msg) {
    Op op;
    op.parseFromString(msg.Command);
    if (msg.CommandIndex <= lastAppliedIndex) {
        return;
    }
    Op opReply;
    if (!IfRequestDuplicate(op.clientId, op.seqId)) {
        // 执行到状态机中;
        executeCommand(op, &opReply);
    }
    // 是否需要持久化 raft 元信息;
    if (maxRaftState != -1) {
        IfNeedToSendSnapShotCommand(msg.CommandIndex, 9);
    }
    // 发送给 client 通道;
    SendMessageToWaitChan(opReply, msg.CommandIndex);
}

void ApplicationServer::executeCommand(Op &op, Op *opReply) {
    std::lock_guard<std::mutex> lock(mtx);
    opReply->clientId = op.clientId;
    opReply->seqId = op.seqId;
    auto result = state_machine->Cmd(op.command);
    if (!result.Error.empty()) {
        DPrintf("[func-application_server::executeCommand()-kvserver{%d}] 执行 raft 命令出错, 错误信息: %s", me,
                result.Error.c_str());
        opReply->err = result.Error;
        return;
    } else {
        opReply->value = result.Value;
    }
    lastRequestId[op.clientId] = op.seqId; // 更新最后一个请求的 clientId 和 seqId
}

bool ApplicationServer::SendMessageToWaitChan(Op &opReply, int64_t logIndex) {
    std::lock_guard<std::mutex> lock(mtx);
    auto it = waitApplyCh.find(logIndex);
    if (it == waitApplyCh.end()) {
        return false;
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