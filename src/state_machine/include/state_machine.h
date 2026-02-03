//
// Created by 19327 on 2026/01/27/星期二.
//
#pragma once
#include <string>
#include "kv.h"
#include "aof.h"
#include "rdb.h"
#include "resp.h"

class StateMachine {
private:
    KVStorage g_store;
    AOFManager g_aof;
    RespParser respParser;
    RDBManager g_rdb;
    ServerConfig serverConfig;
public:
    struct Result {
        std::string Value;
        std::string Error;
    };

    StateMachine(const std::string &conf_path);

    ~StateMachine();

    bool init();

    Result Cmd(const std::string &cmd);

    std::string handle_cmd(const RespValue &r, const std::string *raw);

    std::string genSnapshot();

    bool parseSnapshot(const std::string &snapshot);

    void close();
};
