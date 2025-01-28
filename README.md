<img src="dropdb.png" alt="DropDB logo" width="200">

## Overview

DropDB is a fully-featured database written in Go, designed as an educational project inspired by Edward
Sciore's [Database Design and Implementation](https://link.springer.com/book/10.1007/978-3-030-33836-7). The project
extends beyond the book's implementation with enhanced features, optimizations, and additional capabilities.

The goal is to implement a fairly feature-complete database while exploring database internals, including storage
management, query processing, transaction handling, and optimization techniques.

## Core Features

### Implemented

- **Disk and File Management**
    - Efficient storage and retrieval with optimized disk layout
    - Byte alignment optimization for improved performance

- **Memory Management**
    - Intelligent memory allocation for data and metadata
    - Statistics tracking for query planning optimization

- **Transaction Management**
    - ACID compliance with atomicity and durability
    - Robust transaction processing and recovery

- **Record and Metadata Management**
    - Efficient record access and updates
    - Comprehensive schema and table definition management

- **Query Processing**
    - SQL parsing and execution
    - Materialization and sorting capabilities
    - Support for complex queries with multiple clauses

- **Indexing**
    - B-tree index implementation
    - Performance optimization for data retrieval

### In Development

- **Buffer Management**
    - Smart buffer pool management
    - I/O optimization strategies

- **Query Optimization**
    - Cost-based query planning
    - Execution plan optimization

## Data Types and Operations

### Supported Types

- `int`, `short`, `long`
- `string`
- `bool`
- `date`

### Query Capabilities

- **Comparison Operators**: `=`, `!=`, `>`, `<`, `>=`, `<=`
- **Aggregation Functions**:
    - `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`
    - Note: AVG and SUM results use integer casting due to current floating-point limitations
    - Precision issues may occur with 64-bit integers on 32-bit machines

### Query Features

- Aggregations
- `GROUP BY` clauses
- `HAVING` clauses
- `ORDER BY` (currently ascending only)

## SQL Support

### Supported Commands

#### SELECT

```sql
-- Basic query with conditions
SELECT name
FROM users
WHERE id = 1

-- Join query
SELECT name, dept_name
FROM users,
     departments
WHERE users_dept_id = dept_id

-- Aggregation with grouping
SELECT dept, avg(salary)
FROM employees
GROUP BY dept

-- Complex query
SELECT category, date, sum (amount)
FROM orders
WHERE amount > 500
GROUP BY category, date
HAVING sum (amount) > 2000
ORDER BY total asc
```

#### Data Definition

- `CREATE TABLE` - Define new tables with specified fields and types
- `CREATE VIEW` - Create stored queries
- `CREATE INDEX` - Build indexes for performance optimization

#### Data Manipulation

- `INSERT` - Add new records
- `UPDATE` - Modify existing records
- `DELETE` - Remove records based on conditions

## Project Goals

DropDB serves as both a learning platform and a practical implementation of database concepts. While primarily developed
for educational purposes, it aims to provide real-world database functionality and performance.

## Contributing

Contributions are welcome! Feel free to:

- Open issues for bugs or feature requests
- Submit pull requests for improvements
- Share feedback on implementation approaches

The project particularly welcomes contributions in areas like buffer management and query optimization.