---
title: 'Cuallee: A Python package for data quality across multiple DataFrame APIs'
tags:
  - python
  - data quality
  - data checks
  - data unit test
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

In today's world, where vast amounts of data are generated and collected daily, and where data heavily influence business, political, and societal decisions, it's crucial to evaluate the quality of the data used for analysis, decision-making, and reporting. This involves understanding how reliable and trustworthy the data are. To address this need, we've created `cuallee`, a Python package for assessing data quality. `cuallee` is designed to be dataframe-agnostic, offering an intuitive and user-friendly API for describing checks across the most popular dataframe implementations such as PySpark, Pandas, Snowpark, Polars, DuckDB and BigQuery. Currently, `cuallee` offers over 50 checks to help users evaluate the quality of their data.


# Statement of need

For data engineers and data scientists, maintaining a consistent workflow involves operating in hybrid environments, where they develop locally before transitioning data pipelines and analysis to cloud-based environments. Whilst working in local environments typically allows them to fit data sets in memory, moving workloads to cloud environments involve operating with full scale data that requires a different computing framework, i.e. distributed computing, parallelization, and horizontal scaling.

This shift in computing frameworks requires the adoption of testing strategies that can accommodate testing activities in both local and remote environments, without the need to rewrite test scenarios or employ different testing approaches for assessing various quality dimensions of the data.

An additional argument is related to the rapid evolution of the data ecosystem. Organizations and data teams are constantly seeking ways to improve, whether through cost-effective solutions or by integrating new capabilities into their data operations. However, this pursuit presents new challenges when migrating workloads from one technology to another. As information technology and data strategies become more resilient against vendor lock-ins, they turn to technologies that enable seamless operation across platforms, avoiding the chaos of fully re-implementing data products. In essence, no data testing strategy needs to be rewritten or reformulated due to platform changes.

A last argument is the need for such a quality tool, is the desire of moving quality procedures to the earliest phases of the data product development life-cycle. Whether in industry or academia, the reduction of time allocated for quality activities is unfortunately like a norm, due to the predominant focus on functional aspects. Enabling a declarative, intuitive and flexible programming interface to data quality, allows teams to embed quality into their development, adopting a mindset of building quality in, as opposed to testing quality out.


# Checks
[@Binney:2008] is the reference for the checks

# Controls
This are the controls 

# References