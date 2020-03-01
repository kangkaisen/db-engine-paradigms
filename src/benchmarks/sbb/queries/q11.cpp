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

#include "parallel_hashmap/phmap.h"

using namespace runtime;
using namespace std;

namespace ssb {

NOVECTORIZE std::unique_ptr<runtime::Query> count_hyper(Database& db,
                                                      size_t nrThreads) {
   auto resources = initQuery(nrThreads);
   auto& lo = db["lineorder"];

   auto result_revenue = tbb::parallel_reduce(
   tbb::blocked_range<size_t>(0, lo.nrTuples), types::Integer(0),
   [&](const tbb::blocked_range<size_t>& r,
      const types::Integer& s) {
      auto revenue = s;
      for (size_t i = r.begin(), end = r.end(); i != end; ++i) {
         revenue += 1;
      }
      return revenue;
   },
   [](const types::Integer& x, const types::Integer& y) {
      return x + y;
   });

   //std::cout << "lineorder count: " << lo.nrTuples << std::endl;
   std::cout << "count_hyper result: " << result_revenue << std::endl;

   // --- output
   auto& result = resources.query->result;
   auto revAttr =
       result->addAttribute("revenue", sizeof(types::Integer));
   auto block = result->createBlock(1);
   auto revenue = static_cast<types::Integer*>(block.data(revAttr));
   *revenue = result_revenue;
   block.addedElements(1);

   leaveQuery(nrThreads);
   return std::move(resources.query);
}


NOVECTORIZE std::unique_ptr<runtime::Query> count_if_hyper(Database& db,
                                                      size_t nrThreads) {
   auto resources = initQuery(nrThreads);
   auto& lo = db["lineorder"];

   // --- constants
   const auto filter = types::Integer(19920101);

   auto lo_orderdate = lo["lo_orderdate"].data<types::Integer>();

   auto result_revenue = tbb::parallel_reduce(
   tbb::blocked_range<size_t>(0, lo.nrTuples), types::Integer(0),
   [&](const tbb::blocked_range<size_t>& r,
      const types::Integer& s) {
      auto revenue = s;
      for (size_t i = r.begin(), end = r.end(); i != end; ++i) {
         if (lo_orderdate[i] > filter) {
            revenue += 1;
         }
      }
      return revenue;
   },
   [](const types::Integer& x, const types::Integer& y) {
      return x + y;
   });

   //std::cout << "lineorder count: " << lo.nrTuples << std::endl;
   std::cout << "count_if_hyper result: " << result_revenue << std::endl;

   // --- output
   // auto& result = resources.query->result;
   // auto revAttr =
   //     result->addAttribute("revenue", sizeof(types::Integer));
   // auto block = result->createBlock(1);
   // auto revenue = static_cast<types::Integer*>(block.data(revAttr));
   // *revenue = result_revenue;
   // block.addedElements(1);

   leaveQuery(nrThreads);
   return std::move(resources.query);
}


NOVECTORIZE std::unique_ptr<runtime::Query> sum_hyper(Database& db,
                                                      size_t nrThreads) {
   auto resources = initQuery(nrThreads);
   auto& lo = db["lineorder"];
   auto lo_revenues = lo["lo_revenue"].data<types::Numeric<18, 2>>();

   auto result_revenue = tbb::parallel_reduce(
   tbb::blocked_range<size_t>(0, lo.nrTuples), types::Numeric<18, 2>(0),
   [&](const tbb::blocked_range<size_t>& r,
      const types::Numeric<18, 2>& s) {
      auto revenue = s;
      for (size_t i = r.begin(), end = r.end(); i != end; ++i) {
         revenue += lo_revenues[i];
      }
      return revenue;
   },
   [](const types::Numeric<18, 2>& x, const types::Numeric<18, 2>& y) {
      return x + y;
   });

   std::cout << "sum_hyper lo_revenue result: " << result_revenue << std::endl;

   // --- output
   auto& result = resources.query->result;
   auto revAttr = result->addAttribute("revenue", sizeof(types::Numeric<18, 2>));
   auto block = result->createBlock(1);
   auto revenue = static_cast<types::Numeric<18, 2>*>(block.data(revAttr));
   *revenue = result_revenue;
   block.addedElements(1);

   leaveQuery(nrThreads);
   return std::move(resources.query);
}

NOVECTORIZE std::unique_ptr<runtime::Query> sum3_hyper(Database& db,
                                                      size_t nrThreads) {
   using namespace types;
   auto resources = initQuery(nrThreads);
   auto& lo = db["lineorder"];
   auto lo_revenues = lo["lo_revenue"].data<Numeric<18, 2>>();
   auto lo_extendedprice = lo["lo_extendedprice"].data<Numeric<18, 2>>();
   auto lo_ordtotalprice = lo["lo_ordtotalprice"].data<Numeric<18, 2>>();

   auto result_revenue = tbb::parallel_reduce(
   tbb::blocked_range<size_t>(0, lo.nrTuples),
   make_tuple(Numeric<18, 2>(), Numeric<18, 2>(), Numeric<18,2>()),
   [&](const tbb::blocked_range<size_t>& r,
      auto& value) {
      auto acc = value;
      for (size_t i = r.begin(), end = r.end(); i != end; ++i) {
         get<0>(acc) += lo_revenues[i];
         get<1>(acc) += lo_extendedprice[i];
         get<2>(acc) += lo_ordtotalprice[i];
      }
      return acc;
   },
   [](const tuple<Numeric<18, 2>, Numeric<18, 2>, Numeric<18, 2>>& x,
      const tuple<Numeric<18, 2>, Numeric<18, 2>, Numeric<18, 2>>& y) {
      return make_tuple(
         get<0>(x) + get<0>(y),
         get<1>(x) + get<1>(y),
         get<2>(x) + get<2>(y));
   });

   // std::cout << "sum3_hyper lo_revenues : " << get<0>(result_revenue) << std::endl;
   // std::cout << "sum3_hyper lo_extendedprice : " << get<1>(result_revenue) << std::endl;
   // std::cout << "sum3_hyper lo_ordtotalprice : " << get<2>(result_revenue) << std::endl;

   leaveQuery(nrThreads);
   return std::move(resources.query);
}

NOVECTORIZE std::unique_ptr<runtime::Query> sum_group_hyper(Database& db,
                                                      size_t nrThreads) {
   auto resources = initQuery(nrThreads);
   auto& lo = db["lineorder"];
   auto lo_revenues = lo["lo_revenue"].data<types::Numeric<18, 2>>();
   auto lo_shopmode = lo["lo_shopmode"].data<types::Char<10>>();

   using hash = runtime::CRC32Hash;
   const size_t morselSize = 100000;

   const auto zero = types::Numeric<18, 2>::castString("0.00");
   auto groupOp = make_GroupBy<types::Char<10>, types::Numeric<18, 2>, hash>(
       [](auto& acc, auto&& value) { acc += value; }, zero, nrThreads);

   // preaggregation
   tbb::parallel_for(tbb::blocked_range<size_t>(0, lo.nrTuples, morselSize),
                     [&](const tbb::blocked_range<size_t>& r) {
                        auto groupLocals = groupOp.preAggLocals();
                        for (size_t i = r.begin(), end = r.end(); i != end; ++i) {
                           // --- aggregation
                           groupLocals.consume(lo_shopmode[i], lo_revenues[i]);
                        }
                     });

   // --- output
   auto& result = resources.query->result;
   auto revenueAttr = result->addAttribute("revenue", sizeof(types::Numeric<18, 2>));
   auto shopmode_attr = result->addAttribute("lo_shopmode", sizeof(types::Char<10>));

   groupOp.forallGroups([&](auto& groups) {
      // write aggregates to result
      auto block = result->createBlock(groups.size());
      auto revenue = reinterpret_cast<types::Numeric<18, 2>*>(block.data(revenueAttr));
      auto lo_shopmode = reinterpret_cast<types::Char<10>*>(block.data(shopmode_attr));
      for (auto block : groups)
         for (auto& group : block) {
            //std::cout << group.k << " | " << group.v << std::endl;
            *lo_shopmode++ = group.k;
            *revenue++ = group.v;
         }
      block.addedElements(groups.size());
   });

   leaveQuery(nrThreads);
   return std::move(resources.query);
}

using namespace types;

typedef  phmap::parallel_flat_hash_map<Char<10>, Numeric<18, 2>,
    phmap::container_internal::hash_default_hash<Char<10>>,
    phmap::container_internal::hash_default_eq<Char<10>>,
    phmap::container_internal::Allocator<phmap::container_internal::Pair<const Char<10>, Numeric<18, 2>>>,
    10, mutex> map;

NOVECTORIZE std::unique_ptr<runtime::Query> sum_group_phmap(Database& db,
                                                      size_t nrThreads) {
   auto resources = initQuery(nrThreads);
   auto& lo = db["lineorder"];
   auto lo_revenues = lo["lo_revenue"].data<types::Numeric<18, 2>>();
   auto lo_shopmode = lo["lo_shopmode"].data<types::Char<10>>();

   phmap::flat_hash_map<Char<10>, Numeric<18, 2>> global_agg;
   typedef tbb::enumerable_thread_specific<phmap::flat_hash_map<Char<10>, Numeric<18, 2>>> maps;
   maps agg_tables;
   const size_t morselSize = 100000;

   // preaggregation
   tbb::parallel_for(tbb::blocked_range<size_t>(0, lo.nrTuples, morselSize),
                     [&](const tbb::blocked_range<size_t>& r) {
                        maps::reference table = agg_tables.local();
                        for (size_t i = r.begin(), end = r.end(); i != end; ++i) {
                           // --- aggregation
                           table[lo_shopmode[i]] += lo_revenues[i];
                        }
                     });

   for (auto &table: agg_tables) {
      for(auto &it: table) {
         global_agg[it.first] += it.second;
      }
   }
   // --- output
   // for (auto &it: global_agg) {
   //    std::cout << it.first << " | " << it.second << std::endl;
   // }
   leaveQuery(nrThreads);
   return std::move(resources.query);
}

NOVECTORIZE std::unique_ptr<runtime::Query> sum_group2_hyper(Database& db,
                                                      size_t nrThreads) {
   using namespace types;
   auto resources = initQuery(nrThreads);
   auto& lo = db["lineorder"];
   auto lo_revenues = lo["lo_revenue"].data<types::Numeric<18, 2>>();
   auto lo_shopmode = lo["lo_shopmode"].data<types::Char<10>>();
   auto lo_orderdate = lo["lo_orderdate"].data<types::Integer>();

   using hash = runtime::CRC32Hash;
   const size_t morselSize = 100000;

   const auto zero = types::Numeric<18, 2>::castString("0.00");
   auto groupOp = make_GroupBy<tuple<Integer,Char<10>>, types::Numeric<18, 2>, hash>(
       [](auto& acc, auto&& value) { acc += value; }, zero, nrThreads);

   // preaggregation
   tbb::parallel_for(tbb::blocked_range<size_t>(0, lo.nrTuples, morselSize),
                     [&](const tbb::blocked_range<size_t>& r) {
                        auto groupLocals = groupOp.preAggLocals();
                        for (size_t i = r.begin(), end = r.end(); i != end; ++i) {
                           // --- aggregation
                           groupLocals.consume(
                              make_tuple(lo_orderdate[i],lo_shopmode[i]),
                              lo_revenues[i]);
                        }
                     });

   // --- output
   // auto& result = resources.query->result;
   // auto revenueAttr = result->addAttribute("revenue", sizeof(types::Numeric<18, 2>));
   // auto shopmode_attr = result->addAttribute("lo_shopmode", sizeof(types::Char<10>));

   groupOp.forallGroups([&](auto& groups) {
      // write aggregates to result
      // auto block = result->createBlock(groups.size());
      // auto revenue = reinterpret_cast<types::Numeric<18, 2>*>(block.data(revenueAttr));
      // auto lo_shopmode = reinterpret_cast<types::Char<10>*>(block.data(shopmode_attr));

      for (auto block : groups)
         for (auto& group : block) {
            //std::cout << group.k << " | " << group.v << std::endl;
            // std::cout << get<0>(group.k) << " | "
            // << get<1>(group.k) << " | "
            // << group.v << std::endl;
            // *lo_shopmode++ = group.k;
            // *revenue++ = group.v;
         }
      //block.addedElements(groups.size());
   });

   leaveQuery(nrThreads);
   return std::move(resources.query);
}


typedef tuple<Integer,Char<10>> keys;
struct pair_hash
{
   template <class T1, class T2>
	std::size_t operator() (const tuple<T1, T2> &keys) const
	{
		return std::hash<T1>()(get<0>(keys)) ^ std::hash<T2>()(get<1>(keys));
	}
};

NOVECTORIZE std::unique_ptr<runtime::Query> sum_group2_phmap(Database& db,
                                                      size_t nrThreads) {
   using namespace types;
   auto resources = initQuery(nrThreads);
   auto& lo = db["lineorder"];
   auto lo_revenues = lo["lo_revenue"].data<types::Numeric<18, 2>>();
   auto lo_shopmode = lo["lo_shopmode"].data<types::Char<10>>();
   auto lo_orderdate = lo["lo_orderdate"].data<types::Integer>();

   phmap::flat_hash_map<tuple<Integer,Char<10>>, Numeric<18, 2>, pair_hash> global_agg;
   typedef tbb::enumerable_thread_specific<phmap::flat_hash_map<tuple<Integer,Char<10>>, Numeric<18, 2>, pair_hash>> maps;
   maps agg_tables;
   const size_t morselSize = 100000;

   // preaggregation
   tbb::parallel_for(tbb::blocked_range<size_t>(0, lo.nrTuples, morselSize),
                     [&](const tbb::blocked_range<size_t>& r) {
                        maps::reference table = agg_tables.local();
                        for (size_t i = r.begin(), end = r.end(); i != end; ++i) {
                           // --- aggregation
                           table[make_tuple(lo_orderdate[i],lo_shopmode[i])] += lo_revenues[i];
                        }
                     });

   for (auto &table: agg_tables) {
      for(auto &it: table) {
         global_agg[it.first] += it.second;
      }
   }

   // for (auto &it: global_agg) {
   //    std::cout << get<0>(it.first) << " " << get<1>(it.first) << " | " << it.second << std::endl;
   // }

   leaveQuery(nrThreads);
   return std::move(resources.query);
}

// select count(*) from lineorder;
std::unique_ptr<Q11Builder::Q11> Q11Builder::getQuery() {
   using namespace vectorwise;
   auto result = Result();
   previous = result.resultWriter.shared.result->participate();
   auto r = make_unique<Q11>();
   auto lineorder = Scan("lineorder");
   FixedAggregation(Expression().addOp(primitives::aggr_static_count_star,Value(&r->aggregator)));
   r->rootOp = popOperator();
   return r;
}

std::unique_ptr<runtime::Query> q11_vectorwise(Database& db, size_t nrThreads,
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
      Q11Builder b(db, shared, vectorSize);
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
            std::cout <<  "count_vector result " << *revenue << std::endl;
            block.addedElements(1);
         }
      }
   });
   return result;
}

NOVECTORIZE std::unique_ptr<runtime::Query> q11_hyper(Database& db,
                                                   size_t nrThreads) {
      auto resources = initQuery(nrThreads);
      leaveQuery(nrThreads);
      return std::move(resources.query);
}

NOVECTORIZE std::unique_ptr<runtime::Query> q12_hyper(Database& db,
                                                   size_t nrThreads) {
      auto resources = initQuery(nrThreads);
      leaveQuery(nrThreads);
      return std::move(resources.query);
}


} // namespace ssb
