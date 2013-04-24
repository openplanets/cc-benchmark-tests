cc-benchmark-tests
==================

Holds machine readable test data for the initial SCAPE Characterisation Components 

Overview
--------

* title: SCAPE Characterisation Components Benchmarking Data
* creator-name: SCAPE Project
* creator-url: http://scape-project.eu
* release-type: one-off set

Description
-----------
A record of Tika identification results and benchmark timing, tested on the GovDocs corpora.
The data is a CSV file, each record consists of five fields:

1) the name of the input file
2) the MIME type returned by Tika
3) the MIME type(s) from the GovDocs groundtruth
4) time in milliseconds taken to perform Tika identification
5) boolean OK/FAIL indicating whether Tika successfully identified the file type
