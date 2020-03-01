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

// select count(*) from lineorder where lo_orderdate > 19920101;
std::unique_ptr<Q12Builder::Q12> Q12Builder::getQuery() {
   using namespace vectorwise;

   auto r = make_unique<Q12>();
   auto lineorder = Scan("lineorder");
   Select(
         Expression()
               .addOp(BF(primitives::sel_greater_int32_t_col_int32_t_val),
                        Buffer(filter, sizeof(pos_t)),
                        Column(lineorder, "lo_orderdate"),
                        Value(&r->year_min)));
   FixedAggregation(Expression().addOp(primitives::aggr_static_count_int64,
                                       Value(&r->aggregator),
                                       Buffer(filter)));
   r->rootOp = popOperator();
   return r;
}

std::unique_ptr<runtime::Query> q12_vectorwise(Database& db, size_t nrThreads,
                                             size_t vectorSize) {
   using namespace vectorwise;
   auto result = std::make_unique<runtime::Query>();
   vectorwise::SharedStateManager shared;
   WorkerGroup workers(nrThreads);
   std::atomic<size_t> n;
   std::atomic<int64_t> aggr;
   aggr = 0;
   n = 0;
   workers.run([&]() {
      Q12Builder b(db, shared, vectorSize);
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
            std::cout <<  "count_if_vector result " << *revenue << std::endl;
            block.addedElements(1);
         }
      }
   });

   return result;
}

} // namespace ssb
