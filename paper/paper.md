---
title: 'Cuallee: A Python package for data quality across multiple DataFrame APIs'
tags:
  - Python
  - data quality
  - unit testing
  - pyspark
  - snowpark
  - pandas
  - polars
  - duckdb
  - big data
  - distributing computing
authors:
  - name: Herminio Vazquez
    orcid: 0000-0003-1937-8006
    equal-contrib: true
    affiliation: 1
  - name: Virginie Grosboillot Phd.
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

In an era in which large amount of data are daily generated and collected and in which data have become the driver of business (politic, societal) decisions, it is important to assess the quality of the data used for analysis, decision making and reporting, meaning to which degree they are relialable and trustworthy.
In this context we developped `cuallee`, a Python package for data quality assessment which is platform agnostic. `cuallee` provides an intuitive and user-friendly API to describe checks for PySpark, Pandas, Snowpark, Polars and DuckDB dataframes.
Today `cuallee` provides over 50 checks for assessing the quality of the users' data.


# Statement of need

$1Data are used for ... and ... and ...
$2 Why data quality is important... (ref, ref, ref).
$3Play of cuallee 
$4 summary of the main chara teristics of cuallee
$5 Other players (+ references), advantages of cuallee over other sources...
Not as many integration as other solutions on the market but integration to dagster checks. 


`Gala` is an Astropy-affiliated Python package for galactic dynamics. Python
enables wrapping low-level languages (e.g., C) for speed without losing
flexibility or ease-of-use in the user-interface. The API for `Gala` was
designed to provide a class-based and user-friendly interface to fast (C or
Cython-optimized) implementations of common operations such as gravitational
potential and force evaluation, orbit integration, dynamical transformations,
and chaos indicators for nonlinear dynamics. `Gala` also relies heavily on and
interfaces well with the implementations of physical units and astronomical
coordinate systems in the `Astropy` package [@astropy] (`astropy.units` and
`astropy.coordinates`).

`Gala` was designed to be used by both astronomical researchers and by
students in courses on gravitational dynamics or astronomy. It has already been
used in a number of scientific publications [@Pearson:2017] and has also been
used in graduate courses on Galactic dynamics to, e.g., provide interactive
visualizations of textbook material [@Binney:2008]. The combination of speed,
design, and support for Astropy functionality in `Gala` will enable exciting
scientific explorations of forthcoming data releases from the *Gaia* mission
[@gaia] by students and experts alike.

# Mathematics

Single dollars ($) are required for inline mathematics e.g. $f(x) = e^{\pi/x}$

Double dollars make self-standing equations:

$$\Theta(x) = \left\{\begin{array}{l}
0\textrm{ if } x < 0\cr
1\textrm{ else}
\end{array}\right.$$

You can also use plain \LaTeX for equations
\begin{equation}\label{eq:fourier}
\hat f(\omega) = \int_{-\infty}^{\infty} f(x) e^{i\omega x} dx
\end{equation}
and refer to \autoref{eq:fourier} from text.

# Citations

Citations to entries in paper.bib should be in
[rMarkdown](http://rmarkdown.rstudio.com/authoring_bibliographies_and_citations.html)
format.

If you want to cite a software repository URL (e.g. something on GitHub without a preferred
citation) then you can do it with the example BibTeX entry below for @fidgit.

For a quick reference, the following citation commands can be used:
- `@author:2001`  ->  "Author et al. (2001)"
- `[@author:2001]` -> "(Author et al., 2001)"
- `[@author1:2001; @author2:2001]` -> "(Author1 et al., 2001; Author2 et al., 2002)"

# Figures

Figures can be included like this:
![Caption for example figure.\label{fig:example}](figure.png)
and referenced from text using \autoref{fig:example}.

Figure sizes can be customized by adding an optional second parameter:
![Caption for example figure.](figure.png){ width=20% }

# Acknowledgements

We acknowledge contributions from Brigitta Sipocz, Syrtis Major, and Semyeong
Oh, and support from Kathryn Johnston during the genesis of this project.

In addition, many users have contributed features and bug fixes. Of particular note are Remco de Boer, and Samantha Hamilton who made contributions to documentation and code, and documentation respectively. A full list of coding contributors can be found here: https://github.com/mpl-extensions/mpl-interactions/graphs/contributors
Finally, many users have contributed in ways other than coding. For example by rais- ing issues either with the package or documentation. These users are recognized here:
https://github.com/mpl-extensions/mpl-interactions#contributors-

# References