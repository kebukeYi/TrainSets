//
// Created by 19327 on 2026/01/30/星期五.
//
#include <ctime>
#include <cstdio>
#include <cstdint>
#include <cstdarg>
#include <random>
#include "config.h"
#include "util.h"

void DPrintf(const char *format, ...) {
    if (Debug) {
        time_t now = time(0);
        tm *ltm = localtime(&now);
        va_list args;
        va_start(args, format);
        std::printf("[%d-%d-%d-%d-%d-%d]",
                    ltm->tm_year + 1900, ltm->tm_mon + 1, ltm->tm_mday,
                    ltm->tm_hour, ltm->tm_min, ltm->tm_sec);
        std::vprintf(format, args);
        std::printf("\n");
        va_end(args);
    }
}

void myAssert(bool condition, std::string message) {
    if (!condition) {
        std::cerr << "Error: " << message << std::endl;
        std::exit(EXIT_FAILURE);
    }
}

std::chrono::system_clock::time_point now() {
    return std::chrono::system_clock::now();
}

int64_t randInt63() {
    static std::random_device rd;
    static std::mt19937_64 gen(rd());
    // Go的Int63()返回[0, 2^63)范围的值，即0到2^63-1
    std::uniform_int_distribution<int64_t> dis(0, INT64_MAX);
    return dis(gen);
}

std::chrono::milliseconds getRandomizedElectionTimeout() {
    auto randRange = maxRandElectionTimeOut - minRandElectionTimeOut;
    auto timeout = std::chrono::milliseconds(minRandElectionTimeOut + randInt63() % randRange);
    return timeout + std::chrono::milliseconds(3000);
}

std::chrono::milliseconds getRandomizedReplicationTimeout() {
    return std::chrono::milliseconds(HeartBeatTimeOut + randInt63() % 1000);
}

long long getRandomTimeout() {
    static std::random_device rd;
    static std::mt19937_64 gen(rd());
    std::uniform_int_distribution<long long> dis(50, 349);
    return dis(gen);
}

void sleepForNMilliseconds(long long N) {
    std::this_thread::sleep_for(std::chrono::milliseconds(N));
}

bool isReleasePort(unsigned short port) {
    int s = socket(AF_INET, SOCK_STREAM, IPPROTO_IP);
    sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(port);
    int ret = bind(s, (sockaddr *) &addr, sizeof(addr));
    if (ret != 0) {
        close(s);
        return false;
    }
    close(s);
    return true;
}