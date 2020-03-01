#include <algorithm>
#include <chrono>
#include <iostream>
#include <iterator>
#include <sstream>
#include <thread>
#include <unordered_set>

#include "benchmarks/ssb/Queries.hpp"
#include "common/runtime/Import.hpp"
#include "profile.hpp"
#include "tbb/tbb.h"

using namespace runtime;
using namespace ssb;

static void escape(void* p) { asm volatile("" : : "g"(p) : "memory"); }

size_t nrTuples(Database& db, std::vector<std::string> tables) {
   size_t sum = 0;
   for (auto& table : tables) sum += db[table].nrTuples;
   return sum;
}

/// Clears Linux page cache.
/// This function only works on Linux.
void clearOsCaches() {
  if (system("sync; echo 3 > /proc/sys/vm/drop_caches")) {
    throw std::runtime_error("Could not flush system caches: " +
                             std::string(std::strerror(errno)));
  }
}

int main(int argc, char* argv[]) {
   if (argc <= 2) {
      std::cerr
          << "Usage: ./" << argv[0]
          << "<number of repetitions> <path to sbb dir> [nrThreads = all] \n "
             " EnvVars: [vectorSize = 1024] [SIMDhash = 0] [SIMDjoin = 0] "
             "[SIMDsel = 0]";
      exit(1);
   }

   PerfEvents e;
   Database ssb;
   // load ssb data
   importSSB(argv[2], ssb);

   // run queries
   auto repetitions = atoi(argv[1]);
   size_t nrThreads = std::thread::hardware_concurrency();
   if (argc > 3) nrThreads = atoi(argv[3]);

   std::cout << "Threads is: " << nrThreads << std::endl;

   tbb::task_scheduler_init scheduler(nrThreads);
   
   e.timeAndProfile("count_hyper ", nrTuples(ssb, {"lineorder"}), [&]() { auto result = count_hyper(ssb, nrThreads); escape(&result);}, repetitions);

   e.timeAndProfile("count_vector ", nrTuples(ssb, {"lineorder"}), [&]() { auto result = q11_vectorwise(ssb, nrThreads); escape(&result);}, repetitions);

   e.timeAndProfile("count_if_hyper ", nrTuples(ssb, {"lineorder"}), [&]() { auto result = count_if_hyper(ssb, nrThreads); escape(&result);}, repetitions);

   e.timeAndProfile("count_if_vector ", nrTuples(ssb, {"lineorder"}), [&]() { auto result = q12_vectorwise(ssb, nrThreads); escape(&result);}, repetitions);

   e.timeAndProfile("sum_hyper ", nrTuples(ssb, {"lineorder"}), [&]() { auto result = sum_hyper(ssb, nrThreads); escape(&result);}, repetitions);

   e.timeAndProfile("sum_vector ", nrTuples(ssb, {"lineorder"}), [&]() { auto result = q13_vectorwise(ssb, nrThreads); escape(&result);}, repetitions);

   e.timeAndProfile("sum3_hyper ", nrTuples(ssb, {"lineorder"}), [&]() { auto result = sum3_hyper(ssb, nrThreads); escape(&result);}, repetitions);

   e.timeAndProfile("sum3_vector ", nrTuples(ssb, {"lineorder"}), [&]() { auto result = q21_vectorwise(ssb, nrThreads); escape(&result);}, repetitions);

   e.timeAndProfile("sum_group_hyper ", nrTuples(ssb, {"lineorder"}), [&]() { auto result = sum_group_hyper(ssb, nrThreads); escape(&result);}, repetitions);

   e.timeAndProfile("sum_group_phmap ", nrTuples(ssb, {"lineorder"}), [&]() { auto result = sum_group_phmap(ssb, nrThreads); escape(&result);}, repetitions);

   e.timeAndProfile("sum_group_vector ", nrTuples(ssb, {"lineorder"}), [&]() { auto result = q22_vectorwise(ssb, nrThreads); escape(&result);}, repetitions);

   e.timeAndProfile("sum_group2_hyper ", nrTuples(ssb, {"lineorder"}), [&]() { auto result = sum_group2_hyper(ssb, nrThreads); escape(&result);}, repetitions);

   e.timeAndProfile("sum_group2_phmap ", nrTuples(ssb, {"lineorder"}), [&]() { auto result = sum_group2_phmap(ssb, nrThreads); escape(&result);}, repetitions);

   e.timeAndProfile("sum_group2_vector ", nrTuples(ssb, {"lineorder"}), [&]() { auto result = q23_vectorwise(ssb, nrThreads); escape(&result);}, repetitions);

   return 0;
}
