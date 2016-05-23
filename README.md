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
in the export directory, and imports them one by one into mongo.  It checks past imports,
and will not rerun an import a file. If you want to rerun delete table_updates in KE EMu
mongo collection.

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

A new dump was produced 20140814, containing all records. 

Updates prior to that point do not include records updated on the date the report runs.


IMPORT CRITERIA
---------------

The import process filters out records not needed on the portal. The criteria are:
 
Specimens
---------

(These are filtered out before adding to the mongo collection - see MongoCatalogueTask)

Record type != [
    'Acquisition',
    'Bound Volume',
    'Bound Volume Page',
    'Collection Level Description',
    'DNA Card',  
    'Field Notebook',
    'Field Notebook (Double Page)',
    'Image',
    'Image (electronic)',
    'Image (non-digital)',
    'Image (digital)',
    'Incoming Loan',
    'L&A Catalogue',
    'Missing',
    'Object Entry',
    'object entry', 
    'Object entry',  
    'PEG Specimen',
    'PEG Catalogue',
    'Preparation',
    'Rack File',
    'Tissue', 
    'Transient Lot'
 ]

Record status != [
    "DELETE",
    "DELETE-MERGED",
    "DUPLICATION",
    "Disposed of",
    "FROZEN ARK",
    "INVALID",
    "POSSIBLE TYPE",
    "PROBLEM",
    "Re-registered in error",
    "Reserved",
    "Retired",
    "Retired (see Notes)",
    "Retired (see Notes)Retired (see Notes)",
    "SCAN_cat",
    "See Notes",
    "Specimen missing - see notes",
    "Stub",
    "Stub Record",
    "Stub record"
]

Web publishable != 'N'

Preferred GUID value exists
 
Index lots
----------

Record type = 'Index Lot'
Web publishable != 'N'
Preferred GUID value exists

Artefacts
---------

Record type = 'Artefacts'
Web publishable != 'N'
Preferred GUID value exists


Luigi
-----

Luigi tasks can be run either from the command line



INSTALL
-------

Requires postgres CITEXT extension

CREATE EXTENSION IF NOT EXISTS citext;

And restart postgresql


MANUAL GIS
----------

From Scratch:

UPDATE "a0aa9450-6d48-43c6-9836-18156fa1ff5f" SET "_geom"=st_setsrid(st_makepoint("decimalLongitude"::float8, "decimalLatitude"::float8), 4326),
"_the_geom_webmercator" = st_transform(st_setsrid(st_makepoint("decimalLongitude"::float8, "decimalLatitude"::float8), 4326), 3857)
WHERE "decimalLatitude" <= 90 AND "decimalLatitude" >= -90 AND "decimalLongitude" <= 180 AND "decimalLongitude" >= -180


For updates, add:
AND (
  ("_geom" IS NULL AND "decimalLatitude" IS NOT NULL OR ST_Y("_geom") <> "decimalLatitude")
  OR
  ("_geom" IS NULL AND "decimalLongitude" IS NOT NULL OR ST_X("_geom") <> "decimalLongitude")
)    

