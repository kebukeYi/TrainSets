//
// Created by 19327 on 2026/02/02/星期一.
//
#include "util.h"
#include "client.h"
#include "cof_config.h"

Client::Client() : clientId(GenClientId()), seqId(0), leaderId(0) {}

void Client::init(std::string &configFileName) {
    Config config;
    config.LocalConfigFile(configFileName.c_str());
    printf("config_map:%s\n", config.toString().c_str());

    std::vector<std::pair<std::string, short>> ipAndPort;
    for (int i = 0; i < config.getConfigLen(); i++) {
        // node0ip=127.0.1.1
        std::string node = "node" + std::to_string(i);
        std::string ip = config.get(node + "ip");
        if (ip.empty()) {
            DPrintf("【Client::init】%s", "ip为空");
        }
        DPrintf("【Client::init】ip：%s", ip.c_str());

        // node0port=27899
        std::string portStr = config.get(node + "port");
        if (portStr.empty()) {
            DPrintf("【Client::init】%s", "port为空");
        }
        int port = 0;
        try {
            port = std::stoi(portStr);
        } catch (std::invalid_argument) {
            DPrintf("【Client::init】%s", "port转换失败");
            return;
        }
        ipAndPort.push_back(std::make_pair(ip, port));
    }

    // 进行连接
    for (const auto &item: ipAndPort) {
        std::string ip = item.first;
        short port = item.second;
        auto *rpc = new ToServerRpcUtil(ip, port);
        servers.push_back(std::shared_ptr<ToServerRpcUtil>(rpc));
    }
}

std::string Client::Cmd(std::string &command) {
    seqId++;
    ApplicationRpcProto::CommandArgs request;
    ApplicationRpcProto::CommandReply response;
    request.set_command(command);
    request.set_clientid(clientId);
    request.set_seqid(seqId);
    auto server = leaderId;
    while (true) {

        grpc::Status status = servers[server]->CallCmd(request, response);

        if (!status.ok()) {
            printf("【Client::Cmd】CallCmd {%ld}请求失败, 错误原因:{%s}\n",
                   server,status.error_message().c_str());
            sleep(4);
            continue;
        }

        if (response.err() == ErrWrongLeader) {
            DPrintf("【Client::Cmd】原以为的leader：{%d}请求失败,3秒后向新leader{%d}重试;", server, server + 1);
            server++;
            server = static_cast<int64_t >(server % static_cast<int64_t >(servers.size()));

            sleep(3);
            continue;
        }

        if (response.err() == OK) {
            leaderId = server;
            DPrintf("【Client::Cmd】请求成功，返回值：{%s}", response.value().c_str());
            return response.value();
        }
    }
}

// string : set mm kk ttl
std::string Client::set(const std::string &key, const std::string &value, std::optional<int64_t> ttl_ms) {
    std::vector<std::string> parts;
    parts.push_back("set");
    parts.push_back(key);
    parts.push_back(value);
    if (ttl_ms.has_value()) {
        parts.push_back(std::to_string(ttl_ms.value()));
    }
    auto command = toRespArray(parts);
    return Cmd(command);
}

// string : Client::set mm kk 123456789
std::string Client::setWithExpireAtMs(const std::string &key, const std::string &value, int64_t expire_at_ms) {
    return "";
}

// string : Client::get mm
std::string Client::get(const std::string &key) {
    std::vector<std::string> parts;
    parts.push_back("get");
    parts.push_back(key);
    auto command = toRespArray(parts);
    return Cmd(command);
}

//  string :Client:: del mm
std::string Client::del(const std::vector<std::string> &keys) {
    std::vector<std::string> parts;
    parts.push_back("del");
    for (const auto &key: keys) {
        parts.push_back(key);
    }
    auto command = toRespArray(parts);
    return Cmd(command);
}

//  string :Client:: exists mm
std::string Client::exists(const std::string &key) {
    return "";
}

//  string :Client:: expire mm 123456789
std::string Client::expire(const std::string &key, int64_t ttl_seconds) {
    return "";
}

//  string :Client:: ttl mm
std::string Client::ttl(const std::string &key) {
    return "";
}


//  list allClient:: keys;
std::string Client::listKeys() {
    return "";
}

// Hash APIs
// returns 1Client:: if new field created, 0 if overwritten
// HSET myhaClient::sh field1 "foo"
std::string Client::hset(const std::string &key, const std::string &field, const std::string &value) {
    return "";
}

std::string Client::hget(const std::string &key, const std::string &field) {
    return "";
}

std::string Client::hdel(const std::string &key, const std::vector<std::string> &fields) {
    return "";
}

std::string Client::hexists(const std::string &key, const std::string &field) {
    return "";
}

// return flClient::atten [field, value, field, value, ...]; 扁平化值返回;
std::string Client::hgetallFlat(const std::string &key) {
    return "";
}

std::string Client::hlen(const std::string &key) {
    return "";
}

std::string Client::setHashExpireAtMs(const std::string &key, int64_t expire_at_ms) {
    return "";
}

// ZSet APIs
// returns number of new elements added
// zadd user:rank   kk 90 kk1 89 kk2 88
// zadd order:rank  mm 90 qw1 89 we6 88
std::string Client::zadd(const std::string &key, double score, const std::string &member) {
    return "";
}

// returns number of members removed
std::string Client::zrem(const std::string &key, const std::vector<std::string> &members) {
    return "";
}

// return members between start and stop (inclusive), negative indexes allowed
// zrange user:rank 0 -1
std::string Client::zrange(const std::string &key, int64_t start, int64_t stop) {
    return "";
}

std::string Client::zscore(const std::string &key, const std::string &member) {
    return "";
}

std::string Client::setZSetExpireAtMs(const std::string &key, int64_t expire_at_ms) {
    return "";
}
