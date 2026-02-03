//
// Created by 19327 on 2026/01/30/星期五.
//
#pragma once
#include <chrono>
#include <unistd.h>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/access.hpp>
#include <string>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <thread>
#include <cstdio>
#include <cstdarg>
#include <iostream>
#include <cstdint>

void DPrintf(const char *format...);

void myAssert(bool condition, std::string message);

std::chrono::system_clock::time_point now();

int64_t randInt63();

std::chrono::milliseconds getRandomizedElectionTimeout();

long long getRandomTimeout();

void sleepForNMilliseconds(long long N);

bool isReleasePort(unsigned short port);

template<typename... Args>
std::string format(const char *format_str, Args... args) {
    int size_s = std::snprintf(nullptr, 0, format_str, args...) + 1;  // "\0"
    if (size_s <= 0) {
        throw std::runtime_error("Error during formatting.");
    }
    auto size = static_cast<size_t>(size_s);
    std::vector<char> buf(size);
    std::snprintf(buf.data(), size, format_str, args...);
    return std::string(buf.data(), buf.data() + size - 1);  // remove '\0'
}


template<class T>
class LockQueue {
private:
    std::mutex mutex;
    std::condition_variable cond;
    std::queue<T> queue;

public:
    void Push(const T &item) {
        std::lock_guard<std::mutex> lock(mutex);
        queue.push(item);
        cond.notify_all();
    }

    T Pop() {
        std::unique_lock<std::mutex> lock(mutex);
        while (queue.empty()) {
            // 这里用unique_lock是因为lock_guard不支持解锁，而unique_lock支持;
            cond.wait(lock);
        }
        T item = queue.front();
        queue.pop();
        return item;
    }

    bool timeOutPop(int timeOut, T *item) {
        std::unique_lock<std::mutex> lock(mutex);
        auto now = std::chrono::system_clock::now();
        auto timeOutTime = std::chrono::system_clock::now() + std::chrono::milliseconds(timeOut);
        while (queue.empty()) {
            if (cond.wait_until(lock, timeOutTime) == std::cv_status::timeout) {
                return false;
            } else {
                continue;
            }
        }
        T data = queue.front();
        queue.pop();
        *item = data;
        return true;
    }
};

class Op {
public:
    int64_t clientId;
    int64_t seqId;
    std::string command;
    std::string value;
    std::string err;
public:
    std::string toString() {
        std::stringstream sa;
        boost::archive::text_oarchive oa(sa);
        oa << *this;
        return sa.str();
    }

    bool parseFromString(const std::string &str) {
        std::stringstream sa(str);
        boost::archive::text_iarchive ia(sa);
        ia >> *this;
        return true;
    }

public:
    friend std::ostream &operator<<(std::ostream &os, const Op &obj) {
        os << "Op(clientId=" << obj.clientId << ", seqId=" << obj.seqId << ", command=" << obj.command << ", value="
           << obj.value << ", err=" << obj.err << ")";
        return os;
    }

private:
    // 友元类
    friend class boost::serialization::access;
    template <class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar & clientId;
        ar & seqId;
        ar & command;
        ar & value;
        ar & err;
    }
};

const std::string OK = "OK";
const std::string ErrNoKey = "ErrNoKey";
const std::string ErrWrongLeader = "ErrWrongLeader";