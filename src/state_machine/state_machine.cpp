//
// Created by 19327 on 2026/01/27/星期二.
//
#include <cstring>
#include "config_loader.h"
#include "state_machine.h"
#include "iostream"
#include "resp.h"
#include "kv.h"
#include "aof.h"

StateMachine::StateMachine(const std::string &confPath) {
    std::string err;
    ServerConfig config;
    if (load_config(confPath, config, err)) {
        printf("Load StateMachine config error: %s\n", err.c_str());
        return;
    }
    serverConfig = config;
}

StateMachine::~StateMachine() {
    g_aof.shutdown();
}

bool StateMachine::init() {
    std::cout << "StateMachine init over" << std::endl;
    if (serverConfig.rdb_conf.enabled) {
        g_rdb.setConfig(serverConfig.rdb_conf);
        std::string err;
        if (!g_rdb.load(this->g_store, err)) {
            std::cerr << "RDB load error: " << err << std::endl;
            return false;
        }
    }

    if (serverConfig.aof_conf.enabled) {
        std::string err;
        if (!g_aof.init(serverConfig.aof_conf, err)) {
            std::cerr << "AOF init error: " << err << std::endl;
            return false;
        }
        if (g_aof.load(this->g_store, err)) {
            std::cerr << "AOF load error: " << err << std::endl;
            return false;
        }
    }

    return true;
}

StateMachine::Result StateMachine::Cmd(const std::string &cmd) {
    StateMachine::Result result;
    auto null = respNullBulk();
    result.Value = null;
    respParser.append(cmd);
    auto maybe = respParser.tryParseOneWithRaw();
    if (!maybe.has_value()) {
        return result;
    }
    const RespValue &v = maybe->first;
    const std::string &raw = maybe->second;
    auto resp = handle_cmd(const_cast<RespValue &>(v), &raw);
    if (resp.empty()) {
        return result;
    }
    respParser.clear();
    result.Value = resp;
    return result;
}

std::string StateMachine::genSnapshot() {
    std::string err;
    if (!g_rdb.dump(this->g_store, err)) {
        std::cerr << "RDB dump error: " << err << std::endl;
        return err;
    } else {
        return g_rdb.snapshot();
    }
}

bool StateMachine::parseSnapshot(const std::string &snapshot) {
    std::string err;
    this->g_store.clearAll();
    auto rdb_file_path = g_rdb.path();
    remove(rdb_file_path.c_str());
    FILE *fp = fopen(rdb_file_path.c_str(), "wb");
    if (!fp) {
        std::cerr << "Failed to open RDB file for writing: " << strerror(errno) << std::endl;
        return false;
    }
    fwrite(snapshot.c_str(), snapshot.size(), 1, fp);
    fclose(fp);
    // 打开文件, 重映 g_store;
    if (g_rdb.load(this->g_store, err)) {
        std::cout << "StateMachine parseSnapshot success" << std::endl;
        return true;
    } else {
        std::cerr << "RDB load error: " << err << std::endl;
        return false;
    }
}

std::string StateMachine::handle_cmd(const RespValue &r, const std::string *raw) {
    if (r.type != RespType::Array || r.array.empty()) {
        return respError("ERR protocol error");;
    }
    auto head = r.array[0];
    if (head.type != RespType::BulkString && head.type != RespType::SimpleString) {
        return respError("ERR unknown command '" + head.bulk_string + "'");
    }
    std::string cmd;
    cmd.reserve(head.bulk_string.size());
    for (char &s: head.bulk_string) {
        cmd.push_back((char) toupper(s));
    }

    if (cmd == "PING") {
        if (r.array.size() <= 1) {
            return respSimpleString("PONG");
        } else if (r.array.size() == 2 && r.array[1].type == RespType::BulkString) {
            return respBulk(r.array[1].bulk_string);
        } else {
            return respError("ERR wrong number of arguments for '" + cmd + "' command");
        }
    }
    if (cmd == "ECHO") {
        if (r.array.size() == 2 && r.array[1].type == RespType::BulkString)
            return respBulk(r.array[1].bulk_string);
        return respError("ERR wrong number of arguments for 'ECHO'");
    }

    if (cmd == "SET") {
        if (r.array.size() < 3) {
            return respError("ERR wrong number of arguments for 'SET'");
        }
        if (r.array[1].type != RespType::BulkString || r.array[2].type != RespType::BulkString) {
            return respError("ERR value is not a valid string");
        }
        std::optional<int64_t> ttls;
        size_t index = 3;
        while (index < r.array.size()) {
            if (r.array[index].type != RespType::SimpleString) {
                return respError("ERR syntax error");
            }
            std::string opt;
            opt.resize(r.array[index].bulk_string.size());
            for (const char &c: r.array[index].bulk_string) {
                opt.push_back((char) toupper(c));
            }
            if (opt == "EX") {
                if (index + 1 >= r.array.size() || r.array[index + 1].type != RespType::BulkString) {
                    return respError("ERR value is not an integer");
                }
                try {
                    int64_t sec = std::stoll(r.array[index + 1].bulk_string);
                    if (sec < 0) {
                        return respError("ERR invalid expire time in 'EX'");
                    }
                    ttls = sec * 1000;
                } catch (...) {
                    return respError("ERR value is not an integer or out of range");
                }
                index += 2;
                continue;
            } else if (opt == "PX") {
                if (index + 1 >= r.array.size() || r.array[index + 1].type != RespType::BulkString) {
                    return respError("ERR value is not an integer");
                }
                try {
                    int64_t ms = std::stoll(r.array[index + 1].bulk_string);
                    if (ms < 0) {
                        return respError("ERR invalid expire time in 'EX'");
                    }
                    ttls = ms;
                } catch (...) {
                    return respError("ERR value is not an integer or out of range");
                }
                index += 2;
                continue;
            } else if (opt == "NX") {

            } else if (opt == "XX") {

            } else if (opt == "KEEPTTL") {

            } else {
                return respError("ERR syntax error");
            }
        }// while opt over

        g_store.set(r.array[1].bulk_string, r.array[2].bulk_string, ttls);
        if (raw) {
            g_aof.appendCmdRaw(*raw);
        } else {
            std::vector<std::string> parts;
            for (auto &s: r.array) {
                parts.emplace_back(s.bulk_string);
            }
            // parts ->
            //  write_queue.push_back(AofItem{std::move(cmd), my_seq});
            //    ->  local.emplace_back(std::move(write_queue.front()));
            g_aof.appendCmd(parts, false);
        }
        {
            std::vector<std::string> parts;
            for (auto &s: r.array) {
                parts.emplace_back(s.bulk_string);
            }
        }
        return respSimpleString("OK");
    }

    if (cmd == "GET") {
        if (r.array.size() != 2) {
            return respError("ERR wrong number of arguments for 'GET'");
        } else if (r.array[1].type != RespType::BulkString) {
            return respError("ERR value is not a valid string");
        }
        std::optional<std::string> v = g_store.get(r.array[1].bulk_string);
        return v ? respBulk(*v) : respNullBulk();
    }

    if (cmd == "KEYS") {
        std::string pattern;
        if (r.array.size() == 2) {
            if (r.array[1].type == RespType::BulkString || r.array[1].type == RespType::SimpleString) {
                pattern = r.array[1].bulk_string;
            }
        } else if (r.array.size() != 1) {
            return respError("ERR wrong number of arguments for 'KEYS'");
        }
        std::vector<std::string> keys = g_store.listKeys();
        if (pattern != "*") {
            keys.clear();
            keys.shrink_to_fit();
        }
        std::string out = "*" + std::to_string(keys.size()) + "\r\n";
        for (auto &key: keys) {
            out += respBulk(key);
        }
        return out;
    }

    if (cmd == "FLUSHALL") {
        if (r.array.size() != 1) {
            return respError("ERR wrong number of arguments for 'FLUSHALL'");
        }
        {
            auto kvs = g_store.stringSnapshot();
            std::vector<std::string> keys;
            for (auto &kv: kvs) {
                keys.push_back(kv.first);
            }
            g_store.del(keys);
        }
        {
            auto ks = g_store.hashSnapshot();
            for (auto &kv: ks) {
                std::vector<std::string> flds;
                for (auto &fv: kv.second.field_vals) {
                    flds.push_back(fv.first);
                }
                g_store.hdel(kv.first, flds);
            }
        }
        {
            auto ks = g_store.zSetSnapshot();
            for (auto &kv: ks) {
                std::vector<std::string> ms;
                for (auto &mv: kv.items) {
                    ms.push_back(mv.second);
                }
                g_store.zremove(kv.key, ms);
            }
        }
        if (raw) {
            g_aof.appendCmdRaw(*raw);
        } else {
            g_aof.appendCmd({"FLUSHALL"}, false);
        }
        return respSimpleString("OK");
    }

    if (cmd == "DEL") {
        if (r.array.size() < 2) {
            return respError("ERR wrong number of arguments for 'DEL'");
        }
        std::vector<std::string> keys;
        keys.reserve(r.array.size() - 1);
        for (int i = 1; i < r.array.size(); ++i) {
            if (r.array[i].type != RespType::BulkString) {
                return respError("ERR value is not a valid string");
            }
            keys.emplace_back(r.array[i].bulk_string);
        }
        int removed = g_store.del(keys);
        if (removed > 0) {
            std::vector<std::string> parts;
            parts.reserve(keys.size() + 1);
            parts.emplace_back("DEL");
            for (auto &key: keys) {
                keys.emplace_back(key);
            }
            if (raw) {
                g_aof.appendCmdRaw(*raw);
            } else {
                g_aof.appendCmd(parts, false);
            }
        }
        return respInteger(removed);
    }

    if (cmd == "EXISTS") {
        if (r.array.size() != 2) {
            return respError("ERR wrong number of arguments for 'EXISTS'");

        }
        if (r.array[1].type != RespType::BulkString) {
            return respError("ERR syntax");

        }
        bool ex = g_store.exists(r.array[1].bulk_string);
        return respInteger(ex ? 1 : 0);
    }

    if (cmd == "EXPIRE") {
        if (r.array.size() != 3) {
            return respError("ERR wrong number of arguments for 'EXPIRE'");
        }
        if (r.array[1].type != RespType::BulkString || r.array[2].type != RespType::BulkString) {
            return respError("ERR syntax");
        }
        try {
            int64_t seconds = std::stoll(r.array[2].bulk_string);
            bool ok = g_store.expire(r.array[1].bulk_string, seconds);
            if (ok) {
                if (raw) {
                    g_aof.appendCmdRaw(*raw);
                } else {
                    g_aof.appendCmd({"EXPIRE", r.array[1].bulk_string, std::to_string(seconds)}, false);
                }
            }
            if (ok) {
            }
            return respInteger(ok ? 1 : 0);
        }
        catch (...) {
            return respError("ERR value is not an integer or out of range");
        }
    }

    if (cmd == "TTL") {
        if (r.array.size() != 2) {
            return respError("ERR wrong number of arguments for 'TTL'");
        }
        if (r.array[1].type != RespType::BulkString) {
            return respError("ERR syntax");
        }
        int64_t t = g_store.ttl(r.array[1].bulk_string);
        return respInteger(t);
    }

    if (cmd == "HSET") {
        // hset key f1 v1
        if (r.array.size() != 4) {
            return respError("ERR wrong number of arguments for 'HSET'");
        }

        if (r.array[1].type != RespType::BulkString || r.array[2].type != RespType::BulkString ||
            r.array[3].type != RespType::BulkString) {
            return respError("ERR syntax");
        }

        int created = g_store.hset(r.array[1].bulk_string, r.array[2].bulk_string, r.array[3].bulk_string);
        if (raw) {
            g_aof.appendCmdRaw(*raw);
        } else {
            g_aof.appendCmd({"HSET", r.array[1].bulk_string, r.array[2].bulk_string, r.array[3].bulk_string}, false);
        }
        return respInteger(created);
    }

    if (cmd == "HGET") {
        if (r.array.size() != 3) {
            return respError("ERR wrong number of arguments for 'HGET'");
        }
        if (r.array[1].type != RespType::BulkString || r.array[2].type != RespType::BulkString) {
            return respError("ERR syntax");
        }
        auto val = g_store.hget(r.array[1].bulk_string, r.array[2].bulk_string);
        if (!val.has_value()) {
            return respNullBulk();
        }
        return respBulk(*val);
    }

    if (cmd == "HDEL") {
        if (r.array.size() < 3) {
            return respError("ERR wrong number of arguments for 'HDEL'");
        }
        if (r.array[1].type != RespType::BulkString) {
            return respError("ERR syntax");
        }
        std::vector<std::string> fields;
        for (size_t i = 2; i < r.array.size(); ++i) {
            if (r.array[i].type != RespType::BulkString) {
                return respError("ERR syntax");
            }
            fields.emplace_back(r.array[i].bulk_string);
        }

        int removed = g_store.hdel(r.array[1].bulk_string, fields);
        if (removed > 0) {
            std::vector<std::string> parts;
            parts.reserve(2 + fields.size());
            parts.emplace_back("HDEL");
            parts.emplace_back(r.array[1].bulk_string);
            for (auto &f: fields) {
                parts.emplace_back(f);

            }
            if (raw) {
                g_aof.appendCmdRaw(*raw);
            } else {
                g_aof.appendCmd(parts, false);
            }
        }
        return respInteger(removed);
    }
    if (cmd == "HEXISTS") {
        if (r.array.size() != 3)
            return respError("ERR wrong number of arguments for 'HEXISTS'");
        if (r.array[1].type != RespType::BulkString || r.array[2].type != RespType::BulkString) {
            return respError("ERR syntax");
        }
        bool ex = g_store.hexists(r.array[1].bulk_string, r.array[2].bulk_string);
        return respInteger(ex ? 1 : 0);
    }

    if (cmd == "HGETALL") {
        if (r.array.size() != 2) {
            return respError("ERR wrong number of arguments for 'HGETALL'");
        }
        if (r.array[1].type != RespType::BulkString) {
            return respError("ERR syntax");
        }
        auto flat = g_store.hgetAllByKey(r.array[1].bulk_string);
        RespValue arr;
        arr.type = RespType::Array;
        arr.array.reserve(flat.size());
        std::string out = "*" + std::to_string(flat.size()) + "\r\n";
        for (const auto &s: flat) {
            out += respBulk(s);
        }
        return out;
    }

    if (cmd == "HLEN") {
        if (r.array.size() != 2)
            return respError("ERR wrong number of arguments for 'HLEN'");
        if (r.array[1].type != RespType::BulkString) {
            return respError("ERR syntax");
        }
        int n = g_store.hlen(r.array[1].bulk_string);
        return respInteger(n);
    }

    if (cmd == "ZADD") {
        if (r.array.size() != 4) {
            return respError("ERR wrong number of arguments for 'ZADD'");
        }
        if (r.array[1].type != RespType::BulkString || r.array[2].type != RespType::BulkString ||
            r.array[3].type != RespType::BulkString) {
            return respError("ERR syntax");
        }

        try {
            double sc = std::stod(r.array[2].bulk_string);
            int added = g_store.zadd(r.array[1].bulk_string, sc, r.array[3].bulk_string);
            if (raw) {
                g_aof.appendCmdRaw(*raw);
            } else {
                g_aof.appendCmd({"ZADD", r.array[1].bulk_string, r.array[2].bulk_string, r.array[3].bulk_string},
                                false);
            }
            return respInteger(added);
        } catch (...) {
            return respError("ERR value is not a valid float");
        }
    }
    if (cmd == "ZREM") {
        if (r.array.size() < 3) {
            return respError("ERR wrong number of arguments for 'ZREM'");
        }
        if (r.array[1].type != RespType::BulkString) {
            return respError("ERR syntax");
        }
        std::vector<std::string> members;
        for (size_t i = 2; i < r.array.size(); ++i) {
            if (r.array[i].type != RespType::BulkString) {
                return respError("ERR syntax");
            }
            members.emplace_back(r.array[i].bulk_string);
        }
        int removed = g_store.zremove(r.array[1].bulk_string, members);
        if (removed > 0) {
            std::vector<std::string> parts;
            parts.reserve(2 + members.size());
            parts.emplace_back("ZREM");
            parts.emplace_back(r.array[1].bulk_string);
            for (auto &m: members) {
                parts.emplace_back(m);
            }
            if (raw) {
                g_aof.appendCmdRaw(*raw);
            } else {
                g_aof.appendCmd(parts, false);
            }
        }
        return respInteger(removed);
    }
    if (cmd == "ZRANGE") {
        if (r.array.size() != 4) {
            return respError("ERR wrong number of arguments for 'ZRANGE'");
        }
        if (r.array[1].type != RespType::BulkString || r.array[2].type != RespType::BulkString ||
            r.array[3].type != RespType::BulkString) {
            return respError("ERR syntax");
        }
        try {
            int64_t start = std::stoll(r.array[2].bulk_string);
            int64_t stop = std::stoll(r.array[3].bulk_string);
            auto members = g_store.zrange(r.array[1].bulk_string, start, stop);
            std::string out = "*" + std::to_string(members.size()) + "\r\n";
            for (const auto &m: members) {
                out += respBulk(m);
            }
            return out;
        }
        catch (...) {
            return respError("ERR value is not an integer or out of range");
        }
    }
    if (cmd == "ZSCORE") {
        if (r.array.size() != 3) {
            return respError("ERR wrong number of arguments for 'ZSCORE'");
        }
        if (r.array[1].type != RespType::BulkString || r.array[2].type != RespType::BulkString) {
            return respError("ERR syntax");
        }
        auto s = g_store.zscore(r.array[1].bulk_string, r.array[2].bulk_string);
        if (!s.has_value())
            return respNullBulk();
        return respBulk(std::to_string(*s));
    }

    if (cmd == "BGSAVE" || cmd == "SAVE") {
        if (r.array.size() != 1)
            return respError("ERR wrong number of arguments for 'BGSAVE'");
        std::string err;
        if (!g_rdb.dump(g_store, err)) {
            return respError(std::string("ERR rdb save failed: ") + err);
        }
        return respSimpleString("OK");
    }

    if (cmd == "BGREWRITEAOF") {
        if (r.array.size() != 1) {
            return respError("ERR wrong number of arguments for 'BGREWRITEAOF'");
        }
        std::string err;
        if (!g_aof.isEnabled())
            return respError("ERR AOF disabled");
        if (!g_aof.bgWrite(g_store, err)) {
            return respError(std::string("ERR ") + err);
        }
        return respSimpleString("OK");
    }
    if (cmd == "CONFIG") {
        if (r.array.size() < 2)
            return respError("ERR wrong number of arguments for 'CONFIG'");
        if (r.array[1].type != RespType::BulkString && r.array[1].type != RespType::SimpleString) {
            return respError("ERR syntax");
        }
        std::string sub;
        for (char c: r.array[1].bulk_string) {
            sub.push_back(static_cast<char>(::toupper(c)));
        }

        if (sub == "GET") {
            // 允许 CONFIG GET 与 CONFIG GET <pattern>（未提供时默认 "*")
            std::string pattern = "*";
            if (r.array.size() >= 3) {
                if (r.array[2].type != RespType::BulkString && r.array[2].type != RespType::SimpleString) {
                    return respError("ERR wrong number of arguments for 'CONFIG GET'");
                }
                pattern = r.array[2].bulk_string;
            } else if (r.array.size() != 2) {
                return respError("ERR wrong number of arguments for 'CONFIG GET'");
            }
            auto match = [&](const std::string &k) -> bool {
                if (pattern == "*") return true;
                return pattern == k;
            };
            std::vector<std::pair<std::string, std::string>> kvs;
            // minimal set to satisfy tooling
            kvs.emplace_back("appendonly", g_aof.isEnabled() ? "yes" : "no");
            std::string appendfsync;
            switch (g_aof.mode()) {
                case AofMode::None:
                    appendfsync = "none";
                    break;
                case AofMode::EverySec:
                    appendfsync = "everysec";
                    break;
                case AofMode::Always:
                    appendfsync = "always";
                    break;
            }
            kvs.emplace_back("appendfsync", appendfsync);
            kvs.emplace_back("dir", g_aof.getConfig().dir);
            kvs.emplace_back("dbfilename", g_aof.getConfig().file_name);
            kvs.emplace_back("timeout", std::to_string(g_aof.getConfig().consume_aof_queue_us));
            kvs.emplace_back("databases", "16");
            kvs.emplace_back("maxmemory", "0");
            std::string body;
            size_t elems = 0;

            if (pattern == "*") {
                for (auto &p: kvs) {
                    body += respBulk(p.first);
                    body += respBulk(p.second);
                    elems += 2;
                }
            } else {
                for (auto &p: kvs) {
                    if (match(p.first)) {
                        body += respBulk(p.first);
                        body += respBulk(p.second);
                        elems += 2;
                    }
                }
            }
            return "*" + std::to_string(elems) + "\r\n" + body;
        } else if (sub == "RESETSTAT") {
            if (r.array.size() != 2)
                return respError("ERR wrong number of arguments for 'CONFIG RESETSTAT'");
            return respSimpleString("OK");
        } else {
            return respError("ERR unsupported CONFIG subcommand");
        }
    }
    if (cmd == "INFO") {
        // INFO [section] -> ignore section for now
        std::string info;
        info.reserve(512);
        info += "# TrainSet\r\nversion:0.1.0\r\nrole:master\r\n";
        info += "# Clients\r\nconnected_clients:0\r\n";
        info += "# Stats\r\ntotal_connections_received:0\r\ntotal_commands_processed:0\r\ninstantaneous_ops_per_sec:0\r\n";
        info += "# Persistence\r\naof_enabled:";
        info += (g_aof.isEnabled() ? "1" : "0");
        info += "\r\naof_rewrite_in_progress:0\r\nrdb_bg_save_in_progress:0\r\n";
        return respBulk(info);
    }
    return respError("ERR unknown command");
}

void StateMachine::close() {
    g_aof.shutdown();
}
