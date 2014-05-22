#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

import numpy as np
import pandas as pd
import sys
from ke2mongo.tasks.collection import CollectionDatasetTask
from ke2mongo.log import log
from ke2mongo.tasks import PARENT_TYPES, PART_TYPES, ARTEFACT_TYPE, INDEX_LOT_TYPE
from operator import itemgetter

class DarwinCoreDatasetTask(CollectionDatasetTask):
    """
    Class for creating specimens DwC dataset
    """
    name = 'Specimens'
    description = 'Specimen records'
    format = 'dwc'  # Darwin Core format

    columns = [
        ('_id', '_id', 'int32', False),

        # Identifier
        ('DarGlobalUniqueIdentifier', 'globalUniqueIdentifier', 'string:100', False),

        # Record level
        ('AdmDateModified', 'dateLastModified', 'string:100', False),
        # This isn't actually in DwC - but I'm going to use dcterms:created
        ('AdmDateInserted', 'created', 'string:100', False,),
        ('DarInstitutionCode', 'institutionCode', 'string:100', True),
        ('DarCollectionCode', 'collectionCode', 'string:100', True),
        ('DarBasisOfRecord', 'basisOfRecord', 'string:100', True),

        # Taxonomy
        ('DarScientificName', 'scientificName', 'string:100', True),
        ('DarScientificNameAuthor', 'scientificNameAuthor', 'string:100', True),
        ('DarScientificNameAuthorYear', 'scientificNameAuthorYear', 'string:100', True),
        ('DarKingdom', 'kingdom', 'string:100', True),
        ('DarPhylum', 'phylum', 'string:100', True),
        ('DarClass', 'class', 'string:100', True),
        ('DarOrder', 'order', 'string:100', True),
        ('DarFamily', 'family', 'string:100', True),
        ('DarGenus', 'genus', 'string:100', True),
        ('DarSubgenus', 'subgenus', 'string:100', True),
        ('DarSpecies', 'species', 'string:100', True),
        ('DarSubspecies', 'subspecies', 'string:100', True),
        ('DarHigherTaxon', 'higherTaxon', 'string:100', True),
        ('DarInfraspecificRank', 'infraspecificRank', 'string:100', True),

        # Location
        ('DarDecimalLongitude', 'decimalLongitude', 'float32', True),
        ('DarDecimalLatitude', 'decimalLatitude', 'float32', True),
        ('DarGeodeticDatum', 'geodeticDatum', 'string:100', True),
        ('DarGeorefMethod', 'georefMethod', 'string:100', True),

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
        ('DarHigherGeography', 'DarHigherGeography', 'string:100', True),

        # Occurrence
        ('DarCatalogNumber', 'catalogNumber', 'string:100', True),
        ('DarOtherCatalogNumbers', 'otherCatalogNumbers', 'string:100', True),
        ('DarCatalogNumberText', 'catalogNumberText', 'string:100', True),
        ('DarCollector', 'collector', 'string:100', True),
        ('DarCollectorNumber', 'collectorNumber', 'string:100', True),
        ('DarIndividualCount', 'DarIndividualCount', 'string:100', True),
        ('DarLifeStage', 'lifeStage', 'string:100', True),
        ('DarAgeClass', 'ageClass', 'string:100', True),  # According to docs, ageClass has been superseded by lifeStage. We have both
        ('DarSex', 'sex', 'string:100', True),
        ('DarPreparations', 'preparations', 'string:100', True),
        ('DarPreparationType', 'preparationType', 'string:100', True),
        ('DarObservedWeight', 'observedWeight', 'string:100', True), # This has moved to dynamicProperties

        # Identification
        ('DarIdentifiedBy', 'identifiedBy', 'string:100', True),
        ('DarDayIdentified', 'dayIdentified', 'string:100', True),
        ('DarMonthIdentified', 'monthIdentified', 'string:100', True),
        ('DarYearIdentified', 'yearIdentified', 'string:100', True),
        ('DarIdentificationQualifier', 'identificationQualifier', 'string:100', True),
        ('DarTypeStatus', 'typeStatus', 'string:100', True),

        # Collection event
        ('DarFieldNumber', 'fieldNumber', 'string:100', True),
        ('DarStartTimeOfDay', 'startTimeOfDay', 'string:100', True),
        ('DarStartDayCollected', 'startDayCollected', 'string:100', True),
        ('DarStartMonthCollected', 'startMonthCollected', 'string:100', True),
        ('DarStartYearCollected', 'startYearCollected', 'string:100', True),
        ('DarTimeOfDay', 'timeOfDay', 'string:100', True),
        ('DarDayCollected', 'dayCollected', 'string:100', True),
        ('DarMonthCollected', 'monthCollected', 'string:100', True),
        ('DarYearCollected', 'yearCollected', 'string:100', True),
        ('DarEndTimeOfDay', 'endTimeOfDay', 'string:100', True),
        ('DarEndDayCollected', 'endDayCollected', 'string:100', True),
        ('DarEndMonthCollected', 'endMonthCollected', 'string:100', True),
        ('DarEndYearCollected', 'endYearCollected', 'string:100', True),

        # Resource relationship
        ('DarRelatedCatalogItem', 'relatedCatalogItem', 'string:100', True),

        # Extra fields we need to map - TODO: location irn?
        ('ColRecordType', 'colRecordType', 'string:100', False),

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
    ]

    def get_columns(self, keys=[0, 1, 2]):
        """
        Return list of columns
        You can pass in the keys of the tuples you want to return - 0,1,2 are the default for the CSV parser
        @param keys:
        @return:
        """
        return [itemgetter(*keys)(c) for c in self.columns]

    def part_parent_aggregator(self):
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
        columns + self.dynamic_property_columns

        # Select all fields
        project = {col[0]: 1 for col in columns}
        # Add the PartRef field so we can unwind it
        project['part_id'] = {"$ifNull": ["$PartRef", [None]]}
        project['DarCatalogNumber'] = {"$ifNull": ["$DarCatalogNumber", "$RegRegistrationNumber"]}
        # Manually add this field, as this process will break if it doesn't exist
        project['ColRecordType'] = 1
        project['PartRef'] = 1
        # We cannot rely on the DarGlobalUniqueIdentifier field, as parts do not have it, so build manually
        project['DarGlobalUniqueIdentifier'] = {"$concat": ["NHMUK:ecatalogue:", "$irn"]}

        # TODO: Associated part IDs

        query.append({'$project': project})

        # Unwind based on part ID
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

        query.append({'$project': self.get_columns_projection()})

        # Output to DwC collection
        query.append({'$out': 'agg_%s_parts' % self.collection_name})

        return query

    def specimen_aggregator(self):
        """
        Aggregator for non part specimen records
        @return: aggregation list query
        """
        query = list()
        query.append({'$match': {"ColRecordType": {"$nin": PARENT_TYPES + PART_TYPES + [ARTEFACT_TYPE, INDEX_LOT_TYPE]}}})
        # query.append({'$match': {"ColRecordType": {"$in": ['specimen']}}})
        query.append({'$project': self.get_columns_projection()})
        query.append({'$out': 'agg_%s_specimens' % self.collection_name})

        return query

    def get_columns_projection(self):
        """
        Get a list of column projections, to use in an aggregated query
        @return: list
        """

        #  All non-dynamic property columns
        project = {col[0]: 1 for col in self.columns}

        # Create an array of dynamicProperties to use in an aggregation projection
        # In the format {dynamicProperties : {$concat: [{$cond: {if: "$ColRecordType", then: {$concat: ["ColRecordType=","$ColRecordType", ";"]}, else: ''}}
        dynamic_properties = [{"$cond": {"if": "${}".format(col[0]), "then": {"$concat": ["{}=".format(col[1]), "${}".format(col[0]), ";"]}, "else": ''}} for col in self.dynamic_property_columns]
        project['dynamicProperties'] = {"$concat": dynamic_properties}

        return project


    @property
    def query(self):
        """
        Return list of query objects - either an aggregation query or query dict
        @return: list of queries
        """
        return [
            self.specimen_aggregator(),
            self.part_parent_aggregator()
        ]