ke2mongo
========

USAGE
-----

For the Data Portal, we have three primary tasks:

SpecimenDatasetAPITask - Writes specimens records to CKAN via the API
IndexLotDatasetAPITask - Writes index lot records to CKAN via the API
ArtefactDatasetAPITask - Writes artefact records to CKAN via the API

These are dependent on a series of tasks responsible for writing each of the KE EMu module exports to MongoDB, which are in turn dependent on tasks responsible for reading the export files. For example, the SpecimenDatasetAPITask has the following dependency graph:  

Tasks can be run individually - taking the export file date as a parameter:

python tasks/specimen.py SpecimenDatasetAPITask --local-scheduler --date 20160303 

This is useful for development.  However a main run file is provided, which examines the import log and the export files to work out which is the oldest group of export files that need running, and schedules them all to run.

python run.py



INSTALL
-------

Requires postgres CITEXT extension

CREATE EXTENSION IF NOT EXISTS citext;

And restart postgresql


MANUAL GIS
----------

If you want to add the GIS fields

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

