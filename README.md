ke2mongo
========

task hierarchy
--------------

1. Data is imported from KE EMu exports into mongo with: MongoCatalogueTask, MongoTaxonomyTask
2. MongoDeleteTask parses the eaudit export and removes any deleted files
3. Data is exported from mongo into CSV via CSVTasks (SpecimenCSVTask, ...) 
    OR:
   Data imported directly into CKAN via API method 


BULK IMPORT
-----------

To recreate from scratch, there's a bulk import. This script loops through all files
in the export directory, and imports them one by one into mongo. 

Run with command:

python bulk.py

It does not import into CKAN. After running bulk import, you can call the
CKAN import task with no date parameter to import all into CKAN.


CKAN
----

You can import into CKAN in two ways, via the CKAN API or by exporting to CSV and importing.

CSV is faster - but does not populate _full_text index. There is a command in ckanext-nhm for
building the _full_text index - but can be slow. Use CSV import if you want to quickly check
data. Otherwise API method is preferable.

API method uses CKAN api, and imports directly there.


KE EMu EXPORTS
--------------

Prior to 23/01/2014 the exports included all public KE EMu records.

From 23/01/2014 the exports only include records updated since the last export ran.

The exports of the 23/01/12 do still contain ALL records, as the last export date was essentially null.
(The eaudit dump is a dump of all deleted records ever, so is not required)

test
A new dump was produced 20140814, containing all records. 

Updates prior to that point do not include records updated on the date the report runs.


INSTALL
-------

Requires postgres CITEXT extension

CREATE EXTENSION IF NOT EXISTS citext;

And restart postgresql