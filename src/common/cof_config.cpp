//
// Created by 19327 on 2026/02/01/星期日.
//
#include "cof_config.h"
#include <iostream>

void Config::LocalConfigFile(const char *filename) {
    FILE *fp = fopen(filename, "r");
    if (fp == nullptr) {
        printf("Cannot open config file: %s\n", filename);
        exit(EXIT_FAILURE);
    }

    // 1.注释行
    // 2.正确的配置项 =
    // 3.去掉开头的多余的空格
    while (!feof(fp)) {
        char buf[512] = {0};
        fgets(buf, 512, fp);

        std::string read_buf(buf);
        ParseTerm(read_buf);

        if (read_buf.empty() || read_buf[0] == '#') {
            // 下一行
            continue;
        }

        int idx = read_buf.find('=');
        if (idx == -1) {
            // 配置项不合法
            continue;
        }

        std::string  key;
        std::string  val;
        key = read_buf.substr(0, idx);
        ParseTerm(key);
        // rpc.server.ip=127.0.0.1\n
        int endIdx = read_buf.find('\n', idx);
        val = read_buf.substr(idx + 1, endIdx - idx - 1);
        ParseTerm(val);
        config_map.insert({key, val});
    }

    fclose(fp);
}

std::string Config::get(const std::string &key) {
    auto it = config_map.find(key);
    if (it == config_map.end()) {
        return "";
    }
    return it->second;
}

void Config::ParseTerm(std::string &term) {
    int idx = term.find_first_not_of(' ');
    if (idx != -1) {
        // 说明字符串前面有空格
        term = term.substr(idx, term.size() - idx);
    }
    // 去掉字符串后面多余的空格
    idx = term.find_last_not_of(' ');
    if (idx != -1) {
        // 说明字符串后面有空格
        term = term.substr(0, idx + 1);
    }
}