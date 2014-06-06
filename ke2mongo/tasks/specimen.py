#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.

python run.py SpecimenDatasetTask --local-scheduler --date 20140123

"""

from ke2mongo.tasks.dataset import DatasetTask
from ke2mongo.tasks.csv import CSVTask
from ke2mongo.tasks import PARENT_TYPES, PART_TYPES, ARTEFACT_TYPE, INDEX_LOT_TYPE
from operator import itemgetter
from collections import OrderedDict

class SpecimenCSVTask(CSVTask):

    columns = [

        # Specimen column tuples have an extra value, denoting if the field is inheritable by part records
        # ([KE EMu field], [new field], [field type], Inheritable False|False)

        ('_id', '_id', 'int32', False),

        # Identifier
        ('DarGlobalUniqueIdentifier', 'occurrenceID', 'string:100', False),

        # Record level
        ('AdmDateModified', 'modified', 'string:100', False),
        # This isn't actually in DwC - but I'm going to use dcterms:created
        ('AdmDateInserted', 'created', 'string:100', False,),
        ('DarInstitutionCode', 'institutionCode', 'string:100', True),
        ('DarCollectionCode', 'collectionCode', 'string:100', True),
        ('DarBasisOfRecord', 'basisOfRecord', 'string:100', True),

        # Taxonomy
        ('DarScientificName', 'scientificName', 'string:100', True),
        # Rather than using the two darwin core fields DarScientificNameAuthorYear and ScientificNameAuthor
        # It's easier to just use IdeFiledAsAuthors which has them both concatenated
        ('IdeFiledAsAuthors', 'scientificNameAuthorship', 'string:100', True),
        ('DarKingdom', 'kingdom', 'string:100', True),
        ('DarPhylum', 'phylum', 'string:100', True),
        ('DarClass', 'class', 'string:100', True),
        ('DarOrder', 'order', 'string:100', True),
        ('DarFamily', 'family', 'string:100', True),
        ('DarGenus', 'genus', 'string:100', True),
        ('DarSubgenus', 'subgenus', 'string:100', True),
        ('DarSpecies', 'specificEpithet', 'string:100', True),
        ('DarSubspecies', 'infraspecificEpithet', 'string:100', True),
        ('DarHigherTaxon', 'higherTaxon', 'string:100', True),
        ('DarInfraspecificRank', 'taxonRank', 'string:100', True),

        # Location
        ('DarDecimalLongitude', 'decimalLongitude', 'float32', True),
        ('DarDecimalLatitude', 'decimalLatitude', 'float32', True),
        ('DarGeodeticDatum', 'geodeticDatum', 'string:100', True),
        ('DarGeorefMethod', 'georeferenceProtocol', 'string:100', True),

        ('DarMinimumElevationInMeters', 'minimumElevationInMeters', 'string:100', True),
        ('DarMaximumElevationInMeters', 'maximumElevationInMeters', 'string:100', True),
        ('DarMinimumDepthInMeters', 'minimumDepthInMeters', 'string:100', True),
        ('DarMaximumDepthInMeters', 'maximumDepthInMeters', 'string:100', True),

        ('DarIsland', 'island', 'string:100', True),
        ('DarIslandGroup', 'islandGroup', 'string:100', True),
        ('DarContinentOcean', 'continentOcean', 'string:100', True),
        ('DarWaterBody', 'waterBody', 'string:100', True),

        ('DarLocality', 'locality', 'string:100', True),
        ('DarStateProvince', 'stateProvince', 'string:100', True),
        ('DarCountry', 'country', 'string:100', True),
        ('DarContinent', 'continent', 'string:100', True),
        ('DarHigherGeography', 'higherGeography', 'string:100', True),

        # Occurrence
        ('DarCatalogNumber', 'catalogNumber', 'string:100', True),
        ('DarOtherCatalogNumbers', 'otherCatalogNumbers', 'string:100', True),
        ('DarCollector', 'recordedBy', 'string:100', True),
        ('DarCollectorNumber', 'recordNumber', 'string:100', True),
        ('DarIndividualCount', 'individualCount', 'string:100', True),
        ('DarLifeStage', 'lifeStage', 'string:100', True),
        #  According to docs, ageClass has been superseded by lifeStage. We have both, but ageClass duplicates
        # And for the ~200 it has extra data, the data isn't good
        # ('DarAgeClass', 'ageClass', 'string:100', True),
        ('DarSex', 'sex', 'string:100', True),
        ('DarPreparations', 'preparations', 'string:100', True),

        # Identification
        ('DarIdentifiedBy', 'identifiedBy', 'string:100', True),
        # KE Emu has 3 fields for identification date: DarDayIdentified, DarMonthIdentified and DarYearIdentified
        # But EntIdeDateIdentified holds them all - which is what we want for dateIdentified
        ('EntIdeDateIdentified', 'dateIdentified', 'string:100', True),
        ('DarIdentificationQualifier', 'identificationQualifier', 'string:100', True),
        ('DarTypeStatus', 'typeStatus', 'string:100', True),

        # Collection event
        ('DarFieldNumber', 'fieldNumber', 'string:100', True),
        # Merge into eventTime (DarStartTimeOfDay & DarEndTimeOfDay not used when eventTime is empty)
        ('DarTimeOfDay', 'eventTime', 'string:100', True),
        # KE EMu uses 3 fields: DarDayCollected, DarStartDayCollected and DarEndDayCollected
        # However DarStartDayCollected & DarEndDayCollected is never populated when DarDayCollected isn't
        # So lets just use DarDayCollected
        ('DarDayCollected', 'day', 'string:100', True),
        # As day: DarStartMonthCollected + DarEndMonthCollected => DarMonthCollected
        ('DarMonthCollected', 'month', 'string:100', True),
        # Merge into year
        ('DarYearCollected', 'year', 'string:100', True),

        # TODO: Test this and rerun with new downloads
        # TODO: New download has failed.

        # Geo
        ('DarEarliestEon', 'earliestEonOrLowestEonothem', 'string:100', True),  # Eon
        ('DarLatestEon', 'latestEonOrHighestEonothem', 'string:100', True),
        ('DarEarliestEra', 'earliestEraOrLowestErathem', 'string:100', True),  # Era
        ('DarLatestEra', 'latestEraOrHighestErathem', 'string:100', True),
        ('DarEarliestPeriod', 'earliestPeriodOrLowestSystem', 'string:100', True),  # Period
        ('DarLatestPeriod', 'latestPeriodOrHighestSystem', 'string:100', True),
        ('DarEarliestEpoch', 'earliestEpochOrLowestSeries', 'string:100', True),  # Epoch
        ('DarLatestEpoch', 'latestEpochOrHighestSeries', 'string:100', True),
        ('DarEarliestAge', 'earliestAgeOrLowestStage', 'string:100', True),  # Age
        ('DarLatestAge', 'latestAgeOrHighestStage', 'string:100', True),
        ('DarLowestBiostrat', 'lowestBiostratigraphicZone', 'string:100', True),  # Biostratigraphy
        ('DarHighestBiostrat', 'highestBiostratigraphicZone', 'string:100', True),
        ('DarGroup', 'group', 'string:100', True),
        ('DarFormation', 'formation', 'string:100', True),
        ('DarMember', 'member', 'string:100', True),
        ('DarBed', 'bed', 'string:100', True),

        # Resource relationship
        ('DarRelatedCatalogItem', 'relatedResourceID', 'string:100', True),

        # TODO: I've removed this. Check it's in dynamic props
        # ('ColRecordType', 'colRecordType', 'string:100', False),

        ('dynamicProperties', 'dynamicProperties', 'string:400', False),

        # Removed: We do not want notes, could contain anything
        # ('DarNotes', 'DarNotes', 'string:100', True),
        # ('DarLatLongComments', 'latLongComments', 'string:100', True),
    ]

    # Dynamic properties - these will map into one dynamicProperties field
    # They are use in the aggregator, not the monary query so specifying type isn't required
    dynamic_property_columns = [
        ('ColRecordType', 'colRecordType', False),
        ('ColSubDepartment', 'colSubDepartment', True),
        ('PrtType', 'prtType', False),
        ('RegCode', 'regCode', False),
        ('CatKindOfObject', 'kindOfObject', False),
        ('CatKindOfCollection', 'kindOfCollection', False),
        ('CatPreservative', 'catPreservative', False),
        ('ColKind', 'collectionKind', False),
        ('EntPriCollectionName', 'collectionName', False),
        ('PartRefStr', 'partRefs', True),
        ('DarPreparationType', 'preparationType', True),
        ('DarObservedWeight', 'observedWeight', True),
    ]

    def get_columns(self, keys=[0, 1, 2]):
        """
        Return list of columns
        You can pass in the keys of the tuples you want to return - 0,1,2 are the default for the CSV parser
        @param keys:
        @return:
        """
        return [itemgetter(*keys)(c) for c in self.columns]

    def part_parent_aggregator_query(self):
        """
        Part / Parents using an aggregator which needs to be initiated before running
        @return: status dict
        """
        query = list()

        # Exclude all types except Parent and Part types
        query.append({'$match': {"ColRecordType": {"$in": PARENT_TYPES + PART_TYPES}}})

        # Columns has field type, but we do not use that here, and need to ensure it has the
        # Same dimensions as dynamic_property_columns
        columns = self.get_columns([0, 1, 3])

        columns += self.dynamic_property_columns

        # Select all fields
        project = {col[0]: 1 for col in columns}
        # Add the PartRef field so we can unwind it
        project['part_id'] = {"$ifNull": ["$PartRef", [None]]}
        project['DarCatalogNumber'] = {"$ifNull": ["$DarCatalogNumber", "$RegRegistrationNumber"]}
        # Explicitly add ColRecordType & PartRef - this process will break they do not exist
        project['ColRecordType'] = 1
        project['PartRef'] = 1
        # We cannot rely on the DarGlobalUniqueIdentifier field, as parts do not have it, so build manually
        project['DarGlobalUniqueIdentifier'] = {"$concat": ["NHMUK:ecatalogue:", "$irn"]}

        query.append({'$project': project})

        # # Unwind based on part ID
        query.append({'$unwind': "$part_id"})

        # Add all fields to the group
        #  If col[3] is True (inheritable) use {$first: "$col"} to get the parent record value
        # Otherwise use {$last: "$col"} to use the part record value for that field
        # Due to the way unwind works, part records are always after the parent record
        group = {col[0]: {"%s" % '$first' if col[2] else '$last': "$%s" % col[0]} for col in columns}

        # Add the group key
        group['_id'] = {"$ifNull": ["$part_id", "$_id"]}

        # Add part refs
        group['PartRef'] = {"$first": "$PartRef"}

        query.append({'$group': group})

        query.append({'$match': {"ColRecordType": {"$nin": PARENT_TYPES}}})

        query.append({'$project': self._get_columns_projection()})

        # Output to DwC collection
        query.append({'$out': 'agg_%s_parts' % self.collection_name})

        return query

    def specimen_aggregator_query(self):
        """
        Aggregator for non part specimen records
        @return: aggregation list query
        """
        query = list()
        query.append({'$match': {"ColRecordType": {"$nin": PARENT_TYPES + PART_TYPES + [ARTEFACT_TYPE, INDEX_LOT_TYPE]}}})
        # query.append({'$match': {"ColRecordType": {"$in": ['specimen']}}})
        query.append({'$project': self._get_columns_projection()})
        query.append({'$out': 'agg_%s_specimens' % self.collection_name})

        return query

    def _get_columns_projection(self):
        """
        Get a list of column projections, to use in an aggregated query
        @return: list
        """

        #  All non-dynamic property columns
        project = {col[0]: 1 for col in self.columns}

        # Create an array of dynamicProperties to use in an aggregation projection
        # In the format {dynamicProperties : {$concat: [{$cond: {if: "$ColRecordType", then: {$concat: ["ColRecordType=","$ColRecordType", ";"]}, else: ''}}
        dynamic_properties = [{"$cond": OrderedDict([("if", "${}".format(col[0])), ("then", {"$concat": ["{}=".format(col[1]), "${}".format(col[0]), ";"]}), ("else", '')])} for col in self.dynamic_property_columns]
        project['dynamicProperties'] = {"$concat": dynamic_properties}

        return project

    @property
    def query(self):
        """
        Return list of query objects - either an aggregation query or query dict
        @return: list of queries
        """
        return [
            self.specimen_aggregator_query(),
            self.part_parent_aggregator_query()
        ]

class SpecimenDatasetTask(DatasetTask):
    """
    Class for creating specimens DwC dataset
    """
    name = 'Specimens'
    description = 'Specimen records'
    format = 'dwc'  # Darwin Core format

    package = {
        'name': u'nhm-collection5',
        'notes': u'The Natural History Museum\'s collection',
        'title': "Collection5",
        'author': None,
        'author_email': None,
        'license_id': u'other-open',
        'maintainer': None,
        'maintainer_email': None,
        'resources': [],
    }

    csv_class = SpecimenCSVTask