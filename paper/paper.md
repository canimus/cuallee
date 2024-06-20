---
title: 'cuallee: A Python package for data quality checks across multiple DataFrame APIs'
tags:
  - python
  - data quality
  - data checks
  - data unit tests
  - data pipelines
  - data validation
  - data observability
  - data lake
  - pyspark
  - duckdb
  - pandas
  - snowpark
  - polars
  - big data
authors:
  - name: Herminio Vazquez
    orcid: 0000-0003-1937-8006
    equal-contrib: true
    affiliation: 1
  - name: Virginie Grosboillot
    orcid: 0000-0002-8249-7182
    equal-contrib: true
    affiliation: 2
affiliations:
 - name: Independent Researcher, Mexico
   index: 1
 - name: Swiss Federal Institute of Technology (ETH)
   index: 2
date: 11 December 2022
bibliography: paper.bib

---

# Summary

In today's world, where vast amounts of data are generated and collected daily, and where data heavily influence business, political, and societal decisions, it is crucial to evaluate the quality of the data used for analysis, decision-making, and reporting. This involves understanding how reliable and trustworthy the data are. To address this need, we have created `cuallee`, a Python package for assessing data quality. `cuallee` is designed to be dataframe-agnostic, offering an intuitive and user-friendly API for describing checks across the most popular dataframe implementations such as PySpark, Pandas, Snowpark, Polars, DuckDB, and BigQuery. Currently, `cuallee` offers over 50 checks to help users evaluate the quality of their data.


# Statement of need

For data engineers and data scientists, maintaining a consistent workflow involves operating in hybrid environments, where they develop locally before transitioning data pipelines and analyses to cloud-based environments. Whilst working in local environments typically allows them to fit data sets in memory, moving workloads to cloud environments involve operating with full scale data that requires a different computing framework [@10.14778/3229863.3229867], i.e. distributed computing, parallelization, and horizontal scaling. `cuallee` accomodates the testing activities required by this shift in computing frameworks, in both local and remote environments, without the need to rewrite test scenarios or employ different testing approaches for assessing various quality dimensions of the data [@10.1145/3603707].

An additional argument is related to the rapid evolution of the data ecosystem [@10.1145/3603706]. Organizations and data teams are constantly seeking ways to improve, whether through cost-effective solutions or by integrating new capabilities into their data operations. However, this pursuit presents new challenges when migrating workloads from one technology to another. As information technology and data strategies become more resilient against vendor lock-ins, they turn to technologies that enable seamless operation across platforms, avoiding the chaos of fully re-implementing data products. In essence, with `cuallee` no data testing strategy needs to be rewritten or reformulated due to platform changes.

One last argument in favor of using a quality tool such as `cuallee` is the need to integrate quality procedures into the early stages of data product development. Whether in industry or academia, there is often a tendency to prioritize functional aspects over quality, leading to less time being dedicated to quality activities. By providing a clear, easy-to-use, and adaptable programming interface for data quality, teams can incorporate quality into their development process, promoting a proactive approach of building quality in rather than relying solely on testing to ensure quality.


# Data Quality Frameworks
Data platforms have diversified from file systems and relational databases, to full ecosystems including the concept of data lakes [@10.3389/fdata.2020.564115]. Modern platforms host a variety of data formats, beyond traditional tabular data, including semi-structured like `JSON` [@10.1145/2872427.2883029] or unstructured like audio or images.

Operating with modern data platforms, requires a versatile data processing framework capable to handle structured and unstructured data, supports data operations in various programming languages, fulfills the imperative and declarative form to data operations from practitioners and does it reliably for any size of data. Apache Spark [@10.1145/2723372.2742797] represents an exemplar framework due to the wide range of data processing capabilities —batch processing, real-time streaming, machine learning, and graph processing—within a unified framework commended and adopted [@oreilly2023technology] by the data industry.

`cuallee` is powered by native data engines, including Apache Spark, and offers a robust structure that can be extended to new engines with fully open-source implementation guidelines and rigorous testing. `pydeequ` [@10.14778/3229863.3229867] is a pioneer in large-scale data quality frameworks and is fully open-source. However, its adoption is limited due to the smaller community of developers proficient in the `scala` programming language.

On the other hand, `great-expectations` [@Gong_Great_Expectations] and `soda` [@soda_core] additionaly to an open-source platform also offer commercial options that require registration and issuing of keys for cloud reporting capabilities. 

`cuallee` provides a fully open-source data quality framework designed for both academia and industry practitioners, offering unparalleled performance compared to the aforementioned alternatives.


## Performance Benchmark
A reproducible performance benchmark is available in the code repository [@cuallee_performance_tests].
It consists of `38` checks over an open sourced data set [@nyc_tlc_trip_record_data] made of `19.8 million rows`. The validation performs `19` checks for __completeness__ and `19` checks for __uniqueness__ for each column of the dataset.

The following table (\autoref{tab:performance}) provides a summary of the performance comparison:

Framework | Definitions | Time
 ------- | ----------- | ----
`great_expectations==0.18.13` | `python` | `▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇▇ 66s`
`soda==1.4.10` | `yaml` | `▇▇▇▇▇▇▇▇▇▇▇▇▇ 43s`
`pydeequ==1.3.0` | `python` | `▇▇▇ 11s`
`cuallee==0.10.3` | `python` | `▇▇ 7s`

Table: Performance comparison on popular data quality frameworks []{label="tab:performance"} \label{tab:performance}



# Methods

`cuallee`  employs a heuristic-based approach to define quality rules for each dataset. This prevents the inadvertent duplication of quality predicates, thus reducing the likelihood of human error in defining rules with identical predicates. Several studies have been conducted on the efficiency of these rules, including auto-validation and auto-definition using profilers [@10.1145/3580305.3599776].


# Checks
In `cuallee`, checks serve as the fundamental concept. These checks (\autoref{tab:checks}) are implemented by __rules__, which specify _quality predicates_. These predicates, when aggregated, form the criteria used to evaluate the quality of a dataset. Efforts to establish a universal quality metric [@10.1145/3529190.3529222] typically involve using statistics and combining dimensions to derive a single reference value that encapsulates overall quality attributes. 

Check | Description | DataType
 ------- | ----------- | ----
`is_complete` | Zero `nulls` | _agnostic_
`is_unique` | Zero `duplicates` | _agnostic_
`is_primary_key` | Zero `duplicates` | _agnostic_
`are_complete` | Zero `nulls` on group of columns | _agnostic_
`are_unique` | Composite primary key check | _agnostic_
`is_composite_key` | Zero duplicates on multiple columns | _agnostic_
`is_greater_than` | `col > x` | _numeric_
`is_positive` | `col > 0` | _numeric_
`is_negative` | `col < 0` | _numeric_
`is_greater_or_equal_than` | `col >= x` | _numeric_
`is_less_than` | `col < x` | _numeric_
`is_less_or_equal_than` | `col <= x` | _numeric_
`is_equal_than` | `col == x` | _numeric_
`is_contained_in` | `col in [a, b, c, ...]` | _agnostic_
`is_in` | Alias of `is_contained_in` | _agnostic_
`not_contained_in` | `col not in [a, b, c, ...]` | _agnostic_
`not_in` | Alias of `not_contained_in` | _agnostic_
`is_between` | `a <= col <= b` | _numeric, date_
`has_pattern` | Matching a pattern defined as a `regex` | _string_
`is_legit` | String not null & not empty `^\S$` | _string_
`has_min` | `min(col) == x` | _numeric_
`has_max` | `max(col) == x` | _numeric_
`has_std` | `σ(col) == x` | _numeric_
`has_mean` | `μ(col) == x` | _numeric_
`has_sum` | `Σ(col) == x` | _numeric_
`has_percentile` | `%(col) == x` | _numeric_
`has_cardinality` | `count(distinct(col)) == x` | _agnostic_
`has_max_by` | A utilitary predicate for `max(col_a) == x for max(col_b)`  | _agnostic_
`has_min_by` | A utilitary predicate for `min(col_a) == x for min(col_b)`  | _agnostic_
`has_correlation` | Finds correlation between `0..1` on `corr(col_a, col_b)` | _numeric_
`has_entropy` | Calculates the entropy of a column `entropy(col) == x` for classification problems | _numeric_
`is_inside_iqr` | Verifies column values reside inside limits of interquartile range `Q1 <= col <= Q3` used on anomalies.  | _numeric_
`is_in_millions` | `col >= 1e6` | _numeric_
`is_in_billions` | `col >= 1e9` | _numeric_
`is_t_minus_1` | For date fields confirms 1 day ago `t-1` | _date_
`is_t_minus_2` | For date fields confirms 2 days ago `t-2` | _date_
`is_t_minus_3` | For date fields confirms 3 days ago `t-3` | _date_
`is_t_minus_n` | For date fields confirms n days ago `t-n` | _date_
`is_today` | For date fields confirms day is current date `t-0` | _date_
`is_yesterday` | For date fields confirms 1 day ago `t-1` | _date_
`is_on_weekday` | For date fields confirms day is between `Mon-Fri` | _date_
`is_on_weekend` | For date fields confirms day is between `Sat-Sun` | _date_
`is_on_monday` | For date fields confirms day is `Mon` | _date_
`is_on_tuesday` | For date fields confirms day is `Tue` | _date_
`is_on_wednesday` | For date fields confirms day is `Wed` | _date_
`is_on_thursday` | For date fields confirms day is `Thu` | _date_
`is_on_friday` | For date fields confirms day is `Fri` | _date_
`is_on_saturday` | For date fields confirms day is `Sat` | _date_
`is_on_sunday` | For date fields confirms day is `Sun` | _date_
`is_on_schedule` | For date fields confirms time windows i.e. `9:00 - 17:00` | _timestamp_
`is_daily` | Can verify daily continuity on date fields by default. `[2,3,4,5,6]` which represents `Mon-Fri` in PySpark. However new schedules can be used for custom date continuity | _date_
`has_workflow` | Adjacency matrix validation on `3-column` graph, based on `group`, `event`, `order` columns.  | _agnostic_
`satisfies` | An open `SQL expression` builder to construct custom checks | _agnostic_
`validate` | The ultimate transformation of a check with a `dataframe` input for validation | _agnostic_
`iso.iso_4217` | currency compliant `ccy` | _string_
`iso.iso_3166` | country compliant `country` | _string_
`Control.completeness` | Zero `nulls` all columns| _agnostic_
`Control.percentage_fill` | `% rows` not empty | _agnostic_
`Control.percentage_empty` | `% rows` empty | _agnostic_

Table: List and description of the currently available checks []{label="tab:checks"} \label{tab:checks}

# References