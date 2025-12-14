# Mini SQL Database Engine

A simple in-memory SQL query engine built in Python for learning purposes.

## Features

- Load data from CSV files
- Supports:
  - `SELECT * FROM table`
  - `SELECT col1, col2 FROM table`
  - `SELECT COUNT(*) FROM table`
  - `SELECT COUNT(column) FROM table`
  - `WHERE` clause with `=`, `!=`, `>`, `<`, `>=`, `<=`
- Interactive CLI

## Supported SQL Grammar

SELECT _ FROM customers;
SELECT name, age FROM customers;
SELECT COUNT(_) FROM customers;
SELECT COUNT(age) FROM customers;
SELECT _ FROM customers WHERE age > 30;
SELECT name FROM customers WHERE country = 'USA';
SELECT _ FROM customers WHERE age != 25;

> Note: Queries are case-insensitive. Only single WHERE condition supported.

## How to Run

1. Clone the repo
2. Run:

```bash
python cli.py
```
