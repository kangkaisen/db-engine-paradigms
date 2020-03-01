# Database Engines: Vectorization vs. Compilation
This repository contains a collection of experiments, conducted to carve out the differences between two types of query processing engines: Vectorizing (interpretation based) engines and compiling engines.

## Where to Start
Have a look at src/benchmarks/tpch/queries/ to see how query processing for Typer and Tectorwise works.

## How to Build
A configuration file is provided to build this project with CMake.
In the project directory run:
```
mkdir -p build/release
cd build/release
cmake -DCMAKE_BUILD_TYPE=Release ../..
make
```

This creates among others the main binaries test\_all and run\_tpch.
Use test\_all to run unit tests and check whether your compilation worked.
Our main binary run\_tpch requires TPC-H tables as generated by the TPC-H dbgen tool. With these our experimental queries can be run on arbitrary scale factors.
