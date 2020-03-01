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

NOVECTORIZE std::unique_ptr<runtime::Query> q13_hyper(Database& db,
                                                      size_t nrThreads) {
   // --- aggregates

   auto resources = initQuery(nrThreads);

   // --- constants
   const auto relevant_year = types::Integer(1994);
   const auto relevant_weeknuminyear = types::Integer(6);
   const auto discount_min = types::Numeric<18, 2>::castString("5.00");
   const auto discount_max = types::Numeric<18, 2>::castString("7.00");
   const auto quantity_max = types::Integer(35);
   const auto quantity_min = types::Integer(26);

   using hash = runtime::CRC32Hash;
   const size_t morselSize = 100000;

   // --- ht for join date-lineorder
   Hashset<types::Integer, hash> ht;
   tbb::enumerable_thread_specific<runtime::Stack<decltype(ht)::Entry>>
       entries1;
   auto& d = db["date"];
   auto d_year = d["d_year"].data<types::Integer>();
   auto d_weeknuminyear = d["d_weeknuminyear"].data<types::Integer>();
   auto d_datekey = d["d_datekey"].data<types::Integer>();
   // do selection on part and put selected elements into ht
   auto found = PARALLEL_SELECT(d.nrTuples, entries1, {
      auto& year = d_year[i];
      auto& weeknuminyear = d_weeknuminyear[i];
      auto& datekey = d_datekey[i];
      if (year == relevant_year && weeknuminyear == relevant_weeknuminyear) {
         entries.emplace_back(ht.hash(datekey), datekey);
         found++;
      }
   });
   ht.setSize(found);
   parallel_insert(entries1, ht);

   // --- scan lineorder
   auto& lo = db["lineorder"];
   auto lo_orderdate = lo["lo_orderdate"].data<types::Integer>();
   auto lo_quantity = lo["lo_quantity"].data<types::Integer>();
   auto lo_discount = lo["lo_discount"].data<types::Numeric<18, 2>>();
   auto lo_extendedprice = lo["lo_extendedprice"].data<types::Numeric<18, 2>>();

   auto result_revenue = tbb::parallel_reduce(
       tbb::blocked_range<size_t>(0, lo.nrTuples), types::Numeric<18, 4>(0),
       [&](const tbb::blocked_range<size_t>& r,
           const types::Numeric<18, 4>& s) {
          auto revenue = s;
          for (size_t i = r.begin(), end = r.end(); i != end; ++i) {
             auto& quantity = lo_quantity[i];
             auto& discount = lo_discount[i];
             auto& extendedprice = lo_extendedprice[i];

             if ((quantity >= quantity_min) & (quantity <= quantity_max) &
                 (discount >= discount_min) & (discount <= discount_max)) {
                if (ht.contains(lo_orderdate[i])) {
                   // --- aggregation
                   revenue += extendedprice * discount;
                }
             }
          }
          return revenue;
       },
       [](const types::Numeric<18, 4>& x, const types::Numeric<18, 4>& y) {
          return x + y;
       });

   // --- output
   auto& result = resources.query->result;
   auto revAttr =
       result->addAttribute("revenue", sizeof(types::Numeric<18, 4>));
   auto block = result->createBlock(1);
   auto revenue = static_cast<types::Numeric<18, 4>*>(block.data(revAttr));
   *revenue = result_revenue;
   block.addedElements(1);

   leaveQuery(nrThreads);
   return std::move(resources.query);
}

// select sum(LO_REVENUE) from lineorder;
std::unique_ptr<Q13Builder::Q13> Q13Builder::getQuery() {
   using namespace vectorwise;
   auto r = make_unique<Q13>();
   auto lineorder = Scan("lineorder");
   FixedAggregation(Expression().addOp(primitives::aggr_static_plus_int64_t_col,
                                       Value(&r->aggregator),
                                       Column(lineorder, "lo_revenue")));
   r->rootOp = popOperator();
   return r;
}

std::unique_ptr<runtime::Query> q13_vectorwise(Database& db, size_t nrThreads,
                                               size_t vectorSize) {
   using namespace vectorwise;

   // runtime::Relation result;
   auto result = std::make_unique<runtime::Query>();
   vectorwise::SharedStateManager shared;
   WorkerGroup workers(nrThreads);
   std::atomic<size_t> n;
   std::atomic<int64_t> aggr;
   aggr = 0;
   n = 0;
   workers.run([&]() {
      Q13Builder b(db, shared, vectorSize);
      b.previous = result->participate();
      auto query = b.getQuery();
      auto n_ = query->rootOp->next();
      if (n_) {
         aggr.fetch_add(query->aggregator);
         n.fetch_add(n_);
      }

      auto leader = barrier();
      if (leader) {
         auto attribute = result->result->addAttribute(
             "revenue", sizeof(types::Numeric<18, 4>));
         if (n.load()) {
            auto block = result->result->createBlock(1);
            int64_t* revenue = static_cast<int64_t*>(block.data(attribute));
            *revenue = aggr.load();
            std::cout <<  "sum_vector result " << *revenue << std::endl;
            block.addedElements(1);
         }
      }
   });

   return result;
}

} // namespace ssb
