ke2mongo
========

task hierarchy
--------------

1. Data is imported from KE EMu exports into mongo with: MongoCatalogueTask, MongoTaxonomyTask
2. MongoDeleteTask parses the eaudit export and removes any deleted files
3. Data is exported from mongo into CSV via CSVTasks (SpecimenCSVTask, ...)
4. Dataset tasks (SpecimenDatasetTask, ...) copy the CSV file into the CKAN datastore


bulk mongo update
-----------------

These tasks will be run weekly, processing the weekly KE EMu exports.
If the process fails / KE EMu delivers files on the wrong date, the process will fail
As this checks if there are export files for multiple dates.

In this situation, after checking and if there is no problem with the files etc.,
The outstanding files, along with the most recent, can be processed by running the command:

python bulk.py


ke export files
---------------

Prior to 23/01/2014 the exports included all public KE EMu records.

From 23/01/2014 the exports only include records updated since the last export ran.

The exports of the 23/01/12 do still contain ALL records, as the last export date was essentially null.
(The eaudit dump is a dump of all deleted records ever, so is not required)


A new dump was produced 20140814, containing all records. 

Updates prior to that point do not include records updated on the date the report runs.


INSTALL
-------

Requires postgres CITEXT extension

CREATE EXTENSION IF NOT EXISTS citext;

And restart postgresql



















