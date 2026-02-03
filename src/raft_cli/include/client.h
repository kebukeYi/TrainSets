//
// Created by 19327 on 2026/02/02/星期一.
//
#pragma once

#include <vector>
#include <optional>
#include "resp.h"
#include "to_server_rpc_util.h"

class Client {
public:
    std::vector<std::shared_ptr<ToServerRpcUtil>> servers;
    int64_t clientId;
    int64_t seqId;
    int64_t leaderId;
public:
    Client();

    ~Client() = default;

    void init(std::string &configFileName);

    int64_t GenClientId() {
        return rand();
    }

    std::string Cmd(std::string &command);

// string : set mm kk
    std::string set(const std::string &key, const std::string &value, std::optional<int64_t> ttl_ms = std::nullopt);

// string : set mm kk 123456789
    std::string setWithExpireAtMs(const std::string &key, const std::string &value, int64_t expire_at_ms);

// string : get mm
    std::string get(const std::string &key);

//  string : del mm
    std::string del(const std::vector<std::string> &keys);

//  string : exists mm
    std::string exists(const std::string &key);

//  string : expire mm 123456789
    std::string expire(const std::string &key, int64_t ttl_seconds);

//  string : ttl mm
    std::string ttl(const std::string &key);


//  list all keys;
    std::string listKeys(); // union of string/hash/zset keys (unique)

// Hash APIs
// returns 1 if new field created, 0 if overwritten
// HSET myhash field1 "foo"
    std::string hset(const std::string &key, const std::string &field, const std::string &value);

    std::string hget(const std::string &key, const std::string &field);

    std::string hdel(const std::string &key, const std::vector<std::string> &fields);

    std::string hexists(const std::string &key, const std::string &field);

// return flatten [field, value, field, value, ...]; 扁平化值返回;
    std::string hgetallFlat(const std::string &key);

    std::string hlen(const std::string &key);

    std::string setHashExpireAtMs(const std::string &key, int64_t expire_at_ms);

// ZSet APIs
// returns number of new elements added
// zadd user:rank   kk 90 kk1 89 kk2 88
// zadd order:rank  mm 90 qw1 89 we6 88
    std::string zadd(const std::string &key, double score, const std::string &member);

// returns number of members removed
    std::string zrem(const std::string &key, const std::vector<std::string> &members);

// return members between start and stop (inclusive), negative indexes allowed
// zrange user:rank 0 -1
    std::string zrange(const std::string &key, int64_t start, int64_t stop);

    std::string zscore(const std::string &key, const std::string &member);

    std::string setZSetExpireAtMs(const std::string &key, int64_t expire_at_ms);

    std::string toRespArray(const std::vector<std::string> &parts) {
        std::string out;
        out.reserve(16 * parts.size());
        // *<count>\r\n $<size>\r\n<data>\r\n
        out.append("*").append(std::to_string(parts.size())).append("\r\n");
        for (const auto &p: parts) {
            out.append("$").append(std::to_string(p.size())).append("\r\n");
            out.append(p).append("\r\n");
        }
        return out;
    }

    std::string RespValueToString(RespValue &respValue, std::string &bulk) {
        switch (respValue.type) {
            case RespType::SimpleString:
                return bulk + respValue.bulk_string;
            case RespType::Error:
                return bulk + respValue.bulk_string;
            case RespType::Integer:
                return bulk + respValue.bulk_string;
            case RespType::BulkString:
                return bulk + respValue.bulk_string;
            case RespType::Array:
                for (auto &item: respValue.array) {
                    RespValueToString(item, bulk);
                }
                return bulk;
            case RespType::Null:
                return bulk + "null";
            default:
                return "unknown";
        }
    }


};