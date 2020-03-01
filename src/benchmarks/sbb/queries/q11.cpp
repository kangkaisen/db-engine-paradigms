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
   auto revAttr =
       result->addAttribute("revenue", sizeof(types::Numeric<18, 2>));
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

   std::cout << "sum3_hyper lo_revenues : " << get<0>(result_revenue) << std::endl;
   std::cout << "sum3_hyper lo_extendedprice : " << get<1>(result_revenue) << std::endl;
   std::cout << "sum3_hyper lo_ordtotalprice : " << get<2>(result_revenue) << std::endl;

   // // --- output
   // auto& result = resources.query->result;
   // auto revAttr =
   //     result->addAttribute("revenue", sizeof(types::Numeric<18, 2>));
   // auto block = result->createBlock(1);
   // auto revenue = static_cast<types::Numeric<18, 2>*>(block.data(revAttr));
   // *revenue = result_revenue;
   // block.addedElements(1);

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

   // // --- output
   // auto& result = resources.query->result;
   // auto revenueAttr = result->addAttribute("revenue", sizeof(types::Numeric<18, 2>));
   // auto shopmode_attr = result->addAttribute("lo_shopmode", sizeof(types::Char<10>));

   groupOp.forallGroups([&](auto& groups) {
      // write aggregates to result
      // auto block = result->createBlock(groups.size());
      // auto revenue = reinterpret_cast<types::Numeric<18, 2>*>(block.data(revenueAttr));
      // auto lo_shopmode = reinterpret_cast<types::Char<10>*>(block.data(shopmode_attr));

      //std::cout <<  "result size " << groups.size() << std::endl;
      for (auto block : groups)
         for (auto& group : block) {
            //std::cout << group.k << " | " << group.v << std::endl;
            // std::cout << get<0>(group.k) << " | "
            // << get<1>(group.k) << " | "
            // << group.v << std::endl;
            // *lo_shopmode++ = group.k;
            // *revenue++ = group.v;
         }
      // block.addedElements(groups.size());
   });

   leaveQuery(nrThreads);
   return std::move(resources.query);
}


// select sum(lo_extendedprice*lo_discount) as revenue
// from lineorder, date
// where lo_orderdate = d_datekey
// and d_year = 1993
// and lo_discount between1 and 3
// and lo_quantity < 25;

NOVECTORIZE std::unique_ptr<runtime::Query> q11_hyper(Database& db,
                                                      size_t nrThreads) {
   // --- aggregates

   auto resources = initQuery(nrThreads);

   // --- constants
   const auto relevant_year = types::Integer(1993);
   const auto discount_min = types::Numeric<18, 2>::castString("1.00");
   const auto discount_max = types::Numeric<18, 2>::castString("3.00");
   const auto quantity_max = types::Integer(25);

   using hash = runtime::CRC32Hash;
   const size_t morselSize = 100000;

   // --- ht for join date-lineorder
   Hashset<types::Integer, hash> ht;
   tbb::enumerable_thread_specific<runtime::Stack<decltype(ht)::Entry>>
       entries1;
   auto& d = db["date"];
   auto d_year = d["d_year"].data<types::Integer>();
   auto d_datekey = d["d_datekey"].data<types::Integer>();
   // do selection on part and put selected elements into ht
   auto found = PARALLEL_SELECT(d.nrTuples, entries1, {
      auto& year = d_year[i];
      auto& datekey = d_datekey[i];
      if (year == relevant_year) {
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

             if ((quantity < quantity_max) & (discount >= discount_min) &
                 (discount <= discount_max)) {
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

std::unique_ptr<Q11Builder::Q11> Q11Builder::getQuery() {
   using namespace vectorwise;

   auto r = make_unique<Q11>();
   auto date = Scan("date");
   // select d_year = 1993
   Select(Expression().addOp(BF(primitives::sel_equal_to_int32_t_col_int32_t_val),
                             Buffer(sel_year, sizeof(pos_t)),
                             Column(date, "d_year"), Value(&r->year)));

   auto lineorder = Scan("lineorder");
   // select lo_discount between 1 and 3, lo_quantity < 25
   Select(
       Expression()
           .addOp(BF(primitives::sel_less_int32_t_col_int32_t_val),
                  Buffer(sel_qty, sizeof(pos_t)),
                  Column(lineorder, "lo_quantity"), Value(&r->quantity_max))
           .addOp(BF(primitives::selsel_greater_equal_int64_t_col_int64_t_val),
                  Buffer(sel_qty, sizeof(pos_t)),
                  Buffer(sel_discount_low, sizeof(pos_t)),
                  Column(lineorder, "lo_discount"), Value(&r->discount_min))
           .addOp(BF(primitives::selsel_less_equal_int64_t_col_int64_t_val),
                  Buffer(sel_discount_low, sizeof(pos_t)),
                  Buffer(sel_discount_high, sizeof(pos_t)),
                  Column(lineorder, "lo_discount"), Value(&r->discount_max)));

   HashJoin(Buffer(join_result, sizeof(pos_t)), conf.joinAll())
       .setProbeSelVector(Buffer(sel_discount_high), conf.joinSel())
       .addBuildKey(Column(date, "d_datekey"), Buffer(sel_year),
                    conf.hash_sel_int32_t_col(),
                    primitives::scatter_sel_int32_t_col)
       .addProbeKey(Column(lineorder, "lo_orderdate"),
                    Buffer(sel_discount_high), conf.hash_sel_int32_t_col(),
                    primitives::keys_equal_int32_t_col);

   Project().addExpression(Expression().addOp(
       primitives::proj_sel_both_multiplies_int64_t_col_int64_t_col,
       Buffer(join_result), Buffer(result_project, sizeof(int64_t)),
       Column(lineorder, "lo_extendedprice"),
       Column(lineorder, "lo_discount")));

   FixedAggregation(Expression().addOp(primitives::aggr_static_plus_int64_t_col,
                                       Value(&r->aggregator),
                                       Buffer(result_project)));
   r->rootOp = popOperator();
   assert(operatorStack.size() == 0);
   return r;
}

std::unique_ptr<runtime::Query> q11_vectorwise(Database& db, size_t nrThreads,
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
            block.addedElements(1);
         }
      }
   });

   return result;
}

} // namespace ssb
