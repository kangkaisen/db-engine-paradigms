#include <deque>
#include <iostream>

#include "benchmarks/ssb/Queries.hpp"
#include "common/runtime/Hash.hpp"
#include "common/runtime/Types.hpp"
#include "hyper/GroupBy.hpp"
#include "hyper/ParallelHelper.hpp"
#include "tbb/tbb.h"
#include "vectorwise/Operations.hpp"
#include "vectorwise/Operators.hpp"
#include "vectorwise/Primitives.hpp"
#include "vectorwise/QueryBuilder.hpp"
#include "vectorwise/VectorAllocator.hpp"

using namespace runtime;
using namespace std;

namespace ssb {

// select sum(lo_revenue), d_year, p_brand1
// from lineorder, "date", part, supplier
// where lo_orderdate = d_datekey
// and lo_partkey = p_partkey
// and lo_suppkey = s_suppkey
// and p_category = 'MFGR#12'
// and s_region = 'AMERICA'
// group by d_year, p_brand1

//                 sort
//
//                groupby
//
//                 join
//                 hash
//
// tablescan             join
// date                  hash
//
//          tablescan        join
//          supplier         hash
//
//                    tablescan tablescan
//                    part      lineorder

NOVECTORIZE std::unique_ptr<runtime::Query> q21_hyper(Database& db,
                                                      size_t nrThreads) {
   // --- aggregates
   auto resources = initQuery(nrThreads);

   // --- constants
   auto relevant_category = types::Char<7>::castString("MFGR#12");
   auto relevant_region = types::Char<12>::castString("AMERICA");

   using hash = runtime::CRC32Hash;
   const size_t morselSize = 100000;

   // --- ht for join date-lineorder
   Hashmapx<types::Integer, types::Integer, hash> ht1;
   tbb::enumerable_thread_specific<runtime::Stack<decltype(ht1)::Entry>>
       entries1;
   auto& d = db["date"];
   auto d_year = d["d_year"].data<types::Integer>();
   auto d_datekey = d["d_datekey"].data<types::Integer>();
   PARALLEL_SCAN(d.nrTuples, entries1, {
      auto& year = d_year[i];
      auto& datekey = d_datekey[i];
      entries.emplace_back(ht1.hash(datekey), datekey, year);
   });
   ht1.setSize(d.nrTuples);
   parallel_insert(entries1, ht1);

   // --- ht for join supplier-lineorder
   Hashset<types::Integer, hash> ht2;
   tbb::enumerable_thread_specific<runtime::Stack<decltype(ht2)::Entry>>
       entries2;
   auto& su = db["supplier"];
   auto s_suppkey = su["s_suppkey"].data<types::Integer>();
   auto s_region = su["s_region"].data<types::Char<12>>();
   // do selection on part and put selected elements into ht2
   auto found2 = PARALLEL_SELECT(su.nrTuples, entries2, {
      auto& suppkey = s_suppkey[i];
      auto& region = s_region[i];
      if (region == relevant_region) {
         entries.emplace_back(ht2.hash(suppkey), suppkey);
         found++;
      }
   });
   ht2.setSize(found2);
   parallel_insert(entries2, ht2);

   // --- ht for join part-lineorder
   Hashmapx<types::Integer, types::Char<9>, hash> ht3;
   tbb::enumerable_thread_specific<runtime::Stack<decltype(ht3)::Entry>>
       entries3;
   auto& p = db["part"];
   auto p_partkey = p["p_partkey"].data<types::Integer>();
   auto p_category = p["p_category"].data<types::Char<7>>();
   auto p_brand1 = p["p_brand1"].data<types::Char<9>>();
   auto found3 = PARALLEL_SELECT(p.nrTuples, entries3, {
      auto& partkey = p_partkey[i];
      auto& category = p_category[i];
      auto& brand1 = p_brand1[i];
      if (category == relevant_category) {
         entries.emplace_back(ht3.hash(partkey), partkey, brand1);
         found++;
      }
   });
   ht3.setSize(found3);
   parallel_insert(entries3, ht3);

   // --- scan and join lineorder
   auto& lo = db["lineorder"];
   auto lo_orderdate = lo["lo_orderdate"].data<types::Integer>();
   auto lo_partkey = lo["lo_partkey"].data<types::Integer>();
   auto lo_suppkey = lo["lo_suppkey"].data<types::Integer>();
   auto lo_revenue = lo["lo_revenue"].data<types::Numeric<18, 2>>();

   const auto zero = types::Numeric<18, 2>::castString("0.00");
   auto groupOp = make_GroupBy<tuple<types::Char<9>, types::Integer>,
                               types::Numeric<18, 2>, hash>(
       [](auto& acc, auto&& value) { acc += value; }, zero, nrThreads);

   // preaggregation
   tbb::parallel_for(
       tbb::blocked_range<size_t>(0, lo.nrTuples, morselSize),
       [&](const tbb::blocked_range<size_t>& r) {
          auto groupLocals = groupOp.preAggLocals();
          for (size_t i = r.begin(), end = r.end(); i != end; ++i) {
             auto& revenue = lo_revenue[i];
             auto& suppkey = lo_suppkey[i];
             auto& partkey = lo_partkey[i];
             auto& orderdate = lo_orderdate[i];

             auto part = ht3.findOne(partkey);
             if (part) {
                if (ht2.contains(suppkey)) {
                   auto date = ht1.findOne(orderdate);
                   if (date) {
                      // --- aggregation
                      groupLocals.consume(make_tuple(*part, *date), revenue);
                   }
                }
             }
          }
       });
   // --- output
   auto& result = resources.query->result;
   auto revenueAttr =
       result->addAttribute("revenue", sizeof(types::Numeric<18, 2>));
   auto yearAttr = result->addAttribute("d_year", sizeof(types::Integer));
   auto brandAttr = result->addAttribute("p_brand1", sizeof(types::Char<9>));

   groupOp.forallGroups([&](auto& groups) {
      // write aggregates to result
      auto block = result->createBlock(groups.size());
      auto revenue =
          reinterpret_cast<types::Numeric<18, 2>*>(block.data(revenueAttr));
      auto year = reinterpret_cast<types::Integer*>(block.data(yearAttr));
      auto brand = reinterpret_cast<types::Char<9>*>(block.data(brandAttr));
      for (auto block : groups)
         for (auto& group : block) {
            *brand++ = get<0>(group.k);
            *year++ = get<1>(group.k);
            *revenue++ = group.v;
         }
      block.addedElements(groups.size());
   });

   leaveQuery(nrThreads);
   return move(resources.query);
}

std::unique_ptr<Q21Builder::Q21> Q21Builder::getQuery() {
   using namespace vectorwise;
   auto r = make_unique<Q21>();
   auto lineorder = Scan("lineorder");
   FixedAggregation(Expression().addOp(primitives::aggr_static_plus_int64_t_col,
                                       Value(&r->sum_revenue),
                                       Column(lineorder, "lo_revenue"))
                                 .addOp(primitives::aggr_static_plus_int64_t_col,
                                       Value(&r->sum_extend),
                                       Column(lineorder, "lo_extendedprice"))
                                 .addOp(primitives::aggr_static_plus_int64_t_col,
                                       Value(&r->sum_total),
                                       Column(lineorder, "lo_ordtotalprice")));
   r->rootOp = popOperator();
   return r;
}

std::unique_ptr<runtime::Query> q21_vectorwise(Database& db, size_t nrThreads,
                                               size_t vectorSize) {
   using namespace vectorwise;

   // runtime::Relation result;
   auto result = std::make_unique<runtime::Query>();
   vectorwise::SharedStateManager shared;
   WorkerGroup workers(nrThreads);
   std::atomic<size_t> n;
   std::atomic<int64_t> sum_revenue;
   std::atomic<int64_t> sum_extend;
   std::atomic<int64_t> sum_total;
   sum_revenue = 0;
   sum_extend = 0;
   sum_total = 0;
   n = 0;
   workers.run([&]() {
      Q21Builder b(db, shared, vectorSize);
      b.previous = result->participate();
      auto query = b.getQuery();
      auto n_ = query->rootOp->next();
      if (n_) {
         sum_revenue.fetch_add(query->sum_revenue);
         sum_extend.fetch_add(query->sum_extend);
         sum_total.fetch_add(query->sum_total);
         n.fetch_add(n_);
      }

      auto leader = barrier();
      if (leader) {
         if (n.load()) {
            // std::cout <<  "sum_revenue result " << sum_revenue.load() << std::endl;
            // std::cout <<  "sum_extend result " << sum_extend.load() << std::endl;
            // std::cout <<  "sum_total result " << sum_total.load() << std::endl;
         }
      }
   });

   return result;
}

} // namespace ssb
