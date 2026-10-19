## 📘 Unleash the power of Apache Iceberg on AWS — Code Repository

This repository contains all the hands-on examples, notebooks, scripts, and reference architectures that accompany the book [Unleash the power of Apache Iceberg on AWS](https://a.co/d/gb5MNbx).

<img src="unleash_the_power_of_apache_iceberg_on_aws.jpg" alt="myimage" width="400">


### Repository Purpose

The book is designed to be deeply practical. Every chapter comes with runnable examples so that you can:

- Learn Iceberg from fundamentals to advanced features.
- Understand AWS-native integrations (Glue Catalog, EMR, Athena, MSK, Lambda, Fargate…).
- Reproduce the same pipelines described in the book.

This repository functions as a companion lab environment to the book.

### Book Editions

This repository holds the code for two editions of the book. The chapter order changed between them, so pick the version that matches your copy:

| Edition | Branch / Tag | Download |
|---|---|---|
| Second edition (current, default branch) | [`book_v2`](https://github.com/apache-iceberg-on-aws-book/code_samples/tree/book_v2) | [Browse](https://github.com/apache-iceberg-on-aws-book/code_samples/tree/book_v2) |
| First edition | [`book_v1`](https://github.com/apache-iceberg-on-aws-book/code_samples/tree/book_v1) | [Release `book_v1` (zip)](https://github.com/apache-iceberg-on-aws-book/code_samples/releases/tag/book_v1) |

If you own the first edition, use the [`book_v1`](https://github.com/apache-iceberg-on-aws-book/code_samples/tree/book_v1) tag. Its chapter folders follow the original numbering.

### Repository Structure

The repository mirrors the structure of the book (second edition).
Each chapter has its own directory containing the examples, scripts, configurations, or notebooks referenced in that chapter.

- [Chapter 2 - Working with Apache Iceberg Tables in AWS](chapter2_aws/)
- [Chapter 3 - Data Ingestion Patterns](chapter3_data_ingestion/)
- [Chapter 4 - Best Practices and Operations](chapter4_best_practices_operations/)
- [Chapter 5 - Apache Iceberg REST Catalog integration and Interoperability](chapter5_rest_catalog/)
- [Chapter 6 - Migrate to Apache Iceberg](chapter6_migration/)
- [Chapter 7 - Advanced Topics](chapter7_advanced/)

#### Chapter mapping between editions

| Second edition (`book_v2`) | First edition (`book_v1`) |
|---|---|
| `chapter2_aws` | `chapter3_aws` |
| `chapter3_data_ingestion` | `chapter4_data_ingestion` |
| `chapter4_best_practices_operations` | `chapter6_operations_monitoring` |
| `chapter5_rest_catalog` | `chapter8_rest_catalog` |
| `chapter6_migration` | `chapter9_migration` |
| `chapter7_advanced` | `chapter10_advanced` |

### Contributions

This repository is intentionally open so that practitioners can:

- Contribute additional examples
- Suggest improvements
- Add diagrams, troubleshooting notes, or performance benchmarks

PRs are welcome, especially around Iceberg engine integrations with AWS services.

### Trademark Notice

[Apache®](https://www.apache.org/), [Apache Iceberg™](https://iceberg.apache.org/), and all related marks and logos are trademarks of the Apache Software Foundation (ASF).
This repository reflects the author’s personal views and does not represent AWS or the ASF.
