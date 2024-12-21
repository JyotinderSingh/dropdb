# DropDB

## Overview
**DropDB** is a simple yet powerful database written in **Go**, designed as a personal learning project inspired by Edward Sciore's book, [Database Design and Implementation](https://link.springer.com/book/10.1007/978-3-030-33836-7). This project provides a practical approach to understanding core database concepts by building them from scratch.

The goal of DropDB is to implement a fully-featured database system while exploring the intricacies of database internals, including storage management, query processing, transaction handling, and optimization techniques.

## Features
DropDB is built incrementally, with the following features implemented or planned:


- [x] Disk and File Management  
  - Efficient storage and retrieval mechanisms with optimizations for performance.
- [x] Memory Management  
  - Intelligent use of memory to handle data and metadata efficiently.
- [x] Transaction Management  
  - Basic transaction handling with support for atomicity and durability.
- [x] Record Management  
  - Management of records with efficient access and updates.
- [ ] Metadata Management  
  - Systems to manage schema, table definitions, and data organization.
- [ ] Query Processing  
  - Execution of SQL-like queries with optimized operators.
- [ ] Query Parsing  
  - Translation of user queries into executable plans.
- [ ] Planning  
  - Logical and physical plan generation for efficient execution.
- [ ] JDBC Interfaces  
  - Standardized interfaces to interact with the database from Java-based applications.
- [ ] Indexing  
  - Support for indexes to improve query performance.
- [ ] Materialization and Sorting  
  - Temporary data storage and ordering for efficient query execution.
- [ ] Effective Buffer Utilization  
  - Smart buffer management to optimize disk I/O.
- [ ] Query Optimization  
  - Cost-based optimization for selecting the most efficient execution plans.

## Additional Features
- **Support for Multiple Data Types**  
  DropDB supports a variety of data types, including:
    - `int`, `short`, `long`, `string`, `bool`, and `date`.
- **On-Disk Layout Optimizations**  
  Optimized disk storage to ensure byte alignment and minimize padding, improving overall performance.

## Project Motivation
This project serves as a hands-on journey to deeply understand the principles of database design and implementation. By replicating the structure outlined in Sciore's book, DropDB allows for experimentation and learning about real-world database challenges and solutions.

## Contributions
While DropDB is primarily a personal learning project, contributions are welcome! If you'd like to contribute or share feedback, feel free to open an issue or submit a pull request.
