//
// Created by 19327 on 2026/02/01/星期日.
//
#pragma once

#include "string"
#include "unordered_map"

class Config {
private:
    std::unordered_map<std::string, std::string> config_map;

    void ParseTerm(std::string &term);

public:
    void LocalConfigFile(const char *filename);

    std::string get(const std::string &key);
};
