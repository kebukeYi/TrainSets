//
// Created by 19327 on 2026/01/30/星期五.
//
#pragma once
#include <cstdint>

const bool Debug = true;

const int debugMul = 1;

const int HeartBeatTimeOut = 55 * debugMul;

const int ApplyInterval = 25 * debugMul;

const int minRandElectionTimeOut = 250 * debugMul;
const int maxRandElectionTimeOut = 400 * debugMul;

const int consensusTimeOut = 55 * debugMul;

const int fiberThreadPoolSize = 3;
const bool fiberUseCallerThread = false;

const int64_t StartTerm = 0;
const int64_t StartIndex = 0;

const int64_t InvalidTerm=0;
const int64_t InvalidIndex=0;