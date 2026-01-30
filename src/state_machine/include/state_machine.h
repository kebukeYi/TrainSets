//
// Created by 19327 on 2026/01/27/星期二.
//
#pragma once

#include <string>

class StateMachine {
public:
    struct Result {
        int Code;
        int64_t Index;
        std::string Value;
        std::string Error;
    };

    virtual ~StateMachine() = default;

    virtual Result Cmd(const std::string &cmd);

};
