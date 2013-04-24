SCAPE Characterisation Components Benchmarking Data
===================================================

Holds machine readable test data for the initial SCAPE Characterisation Components 

Overview
--------

* title: SCAPE Characterisation Components Benchmarking Data
* creator-name: SCAPE Project
* creator-url: http://scape-project.eu
* dataset-url (text/csv): https://raw.github.com/openplanets/cc-benchmark-tests/master/tika-1.2-test.csv
* dataset-url (application/json): https://raw.github.com/openplanets/cc-benchmark-tests/master/tika-1.2-test.json
* original format: text/csv

Data Release Type
-----------------

- [] a one-off release of a single dataset
- [x] a one-off release of a set of related datasets
- [] ongoing release of a series of related datasets
- [] a service or API for accessing open data

Dataset Type 
--------------------

- [] human-readable documents
- [x] statistical data, such as counts, averages and percentages
- [] geographic information, such as points and boundaries
- [] other kinds of structured data

Description
-----------

A record of Tika 1.2 identification results and benchmark timing, tested on the GovDocs corpora.
The data is a CSV file, each record consists of five fields:

1. the name of the input file
2. the MIME type returned by Tika
3. the MIME type(s) from the GovDocs groundtruth
4. time in milliseconds taken to perform Tika identification
5. boolean OK/FAIL indicating whether Tika successfully identified the file type

Copyright
---------

All content and data are © 2013 [Scape Project](http://www.scape-project.eu/) and licensed under a [Creative Commons Attribution-ShareAlike 3.0 Unported License](http://creativecommons.org/licenses/by-sa/3.0/deed.en_GB).

Personally Identifiable Data 
----------------------------

This dataset does not contain any personally identifiable data other than that relating to the curators, maintainers and publishers of the dataset and related information.

Using the Data
--------------
The CSV (text/csv) file can be opened in Excel and other spreadsheet applications.

Both CSV and JSON are machine readable formats and more information about each can be found at the following locations:

CSV: http://en.wikipedia.org/wiki/Comma-separated_values 
JSON: http://en.wikipedia.org/wiki/JSON

Findability
-----------

- [] Data can be found within 3 clicks of the organisation's home page
- [] Is the data listed somewhere, alongside data from the wider sector

Applicability
-------------

This dataset does not contain any time sensitive information

Quality
-------

* Issues : https://github.com/openplanets/cc-benchmark-tests/issues
* Policy : 

Guarantees
----------
This data is available experimentally but should be around until July 2014
