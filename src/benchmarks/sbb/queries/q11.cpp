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

NOVECTORIZE std::unique_ptr<runtime::Query> sum_func_hyper(Database& db,
                                                      size_t nrThreads) {
   auto resources = initQuery(nrThreads);
   auto& lo = db["lineorder"];
   auto lo_revenues = lo["lo_revenue"].data<types::Numeric<18, 2>>();
   auto lo_orderdate = lo["lo_orderdate"].data<types::Integer>();

   auto result_revenue = tbb::parallel_reduce(
   tbb::blocked_range<size_t>(0, lo.nrTuples), types::Numeric<18, 2>(0),
   [&](const tbb::blocked_range<size_t>& r,
      const types::Numeric<18, 2>& s) {
      auto revenue = s;
      for (size_t i = r.begin(), end = r.end(); i != end; ++i) {
         auto result = ceil(abs(sin(pow(abs(floor(sqrt(lo_orderdate[i].value))),2)) * 10));
         if (result > 4) {
            revenue += lo_revenues[i];
         }
      }
      return revenue;
   },
   [](const types::Numeric<18, 2>& x, const types::Numeric<18, 2>& y) {
      return x + y;
   });

   std::cout << "sum_func_hyper lo_revenue result: " << result_revenue << std::endl;

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

template <typename Key, typename Value>
struct LastElementCache
{
   Value value;
   Key key;
};


NOVECTORIZE std::unique_ptr<runtime::Query> sum_group_int_phmap(Database& db,
                                                      size_t nrThreads) {
   auto resources = initQuery(nrThreads);
   auto& lo = db["lineorder"];
   auto lo_revenues = lo["lo_revenue"].data<types::Numeric<18, 2>>();
   auto lo_orderdate = lo["lo_orderdate"].data<types::Integer>();

   phmap::flat_hash_map<Integer, Numeric<18, 2>> global_agg;
   typedef tbb::enumerable_thread_specific<phmap::flat_hash_map<Integer, Numeric<18, 2>>> maps;
   maps agg_tables;
   const size_t morselSize = 100000;

   //using Cache = LastElementCache<Integer, Numeric<18, 2>>;

   // preaggregation
   tbb::parallel_for(tbb::blocked_range<size_t>(0, lo.nrTuples, morselSize),
                     [&](const tbb::blocked_range<size_t>& r) {
                        maps::reference table = agg_tables.local();
                        //Cache cache;
                        for (size_t i = r.begin(), end = r.end(); i != end; ++i) {
                           table[lo_orderdate[i]] += lo_revenues[i];
                        //    if (i != 0 && cache.key == lo_orderdate[i]) {
                        //       cache.value += lo_revenues[i];
                        //       continue;
                        //    }

                        //    auto value = table[lo_orderdate[i]] + lo_revenues[i];
                        //    table[lo_orderdate[i]] = value;
                        //    cache.key = lo_orderdate[i];
                        //    cache.value = value;
                        }
                        // table[cache.key] = cache.value;

                     });

   for (auto &table: agg_tables) {
      for(auto &it: table) {
         global_agg[it.first] += it.second;
      }
   }

   // for (auto &it: global_agg) {
   //    std::cout << it.first << " | " << it.second << std::endl;
   // }
   leaveQuery(nrThreads);
   return std::move(resources.query);
}

 std::unique_ptr<runtime::Query> sum_group_int_func_phmap(Database& db,
                                                      size_t nrThreads) {
   auto resources = initQuery(nrThreads);
   auto& lo = db["lineorder"];
   auto lo_revenues = lo["lo_revenue"].data<types::Numeric<18, 2>>();
   auto lo_orderdate = lo["lo_orderdate"].data<types::Integer>();

   const size_t morselSize = 102400;

   // phmap::flat_hash_map<int32_t, int64_t> global_agg;
   // typedef tbb::enumerable_thread_specific<phmap::flat_hash_map<int32_t, int64_t>> maps;
   // maps agg_tables;


   runtime::HashmapSmall<int32_t, int64_t> global_agg;
   typedef tbb::enumerable_thread_specific<runtime::HashmapSmall<int32_t, int64_t>> maps;
   maps agg_tables;

   // preaggregation
   tbb::parallel_for(tbb::blocked_range<size_t>(0, lo.nrTuples, morselSize),
                     [&](const tbb::blocked_range<size_t>& r) {
                        maps::reference table = agg_tables.local();

                        vector<int32_t> func(1024);
                        vector<int32_t> date(1024);
                        vector<int64_t> revenues(1024);
                        int32_t* p_func = func.data();
                        int32_t* p_date = date.data();
                        int64_t* p_revenues = revenues.data();
                        size_t i = r.begin();
                        size_t end = r.end();
                        for (; i < end - 1024; i += 1024) {
                           for(int k = 0; k < 1024; k++) {
                              p_date[k] = lo_orderdate[i].value;
                              p_revenues[k] = lo_revenues[i].value;
                           }

                           for (int k = 0; k < 1024; k++) {
                              p_func[k] = ceil(abs(sin(pow(abs(floor(sqrt(p_date[k]))),2)) * 10));
                           }

                           for (int k = 0; k < 1024; k++) {
                              //table[p_func[k]] += p_revenues[k];
                              table.insert_or_update(p_func[k], p_func[k], p_revenues[k]);
                           }
                        }
                        for (;i < end; ++i) {
                           // --- aggregation
                           auto result = ceil(abs(sin(pow(abs(floor(sqrt(lo_orderdate[i].value))),2)) * 10));
                           //table[result] += lo_revenues[i].value;
                           table.insert_or_update(result, result, lo_revenues[i].value);
                        }



                        // vector<int32_t> tmp;
                        // for (size_t i = r.begin(), end = r.end(); i != end; i++) {
                        //    // --- aggregation
                        //    tmp.push_back(ceil(abs(sin(pow(abs(floor(sqrt(lo_orderdate[i].value))),2)) * 10)));
                        // }
                        // for (int i=0; i < r.size(); i++) {
                        //    // --- aggregation
                        //    table[tmp[i]] += lo_revenues[i];
                        // }


                        // for (size_t i = r.begin(), end = r.end(); i != end; ++i) {
                        //    // --- aggregation
                        //    auto result = ceil(abs(sin(pow(abs(floor(sqrt(lo_orderdate[i].value))),2)) * 10));
                        //    table[result] += lo_revenues[i].value;
                        // }
                     });

   // for (auto &table: agg_tables) {
   //    for(auto &it: table) {
   //       global_agg[it.first] += it.second;
   //    }
   // }

   for (auto &table: agg_tables) {
      for(auto &it: table) {
         //global_agg[it.first] += it.second;
         global_agg.insert_or_update(it.key, it.key, it.value);
      }
   }

   //--- output
   // for (auto &it: global_agg) {
   //    std::cout << it.first << " | " << it.second << std::endl;
   // }
   for (auto &it: global_agg) {
      std::cout << it.key << " | " << it.value << std::endl;
   }
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
		//return std::hash<T1>()(get<0>(keys)) ^ std::hash<T2>()(get<1>(keys));
      size_t hash = 17;
      hash = hash * 37 + std::hash<T1>()(get<0>(keys));
      hash = hash * 37 + std::hash<T2>()(get<1>(keys));
      return hash;
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
