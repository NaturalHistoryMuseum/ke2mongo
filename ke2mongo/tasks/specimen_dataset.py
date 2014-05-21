#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

from random import randint
import numpy as np
import pandas as pd
import sys
from ke2mongo.tasks.dataset import DatasetTask
from ke2mongo.log import log
from ke2mongo.tasks import PARENT_TYPES, PART_TYPES


class SpecimenDatasetTask(DatasetTask):
    """
    Class for creating specimens dataset
    """
    name = 'Specimens'
    description = 'Specimen records'
    format = 'dwc'  # Darwin Core format

    package = {
        'name': u'nhm2-collection%s' % randint(1,1000),
        'notes': u'The Natural History Museum\'s collection',
        'title': "Collection",
        'author': None,
        'author_email': None,
        'license_id': u'other-open',
        'maintainer': None,
        'maintainer_email': None,
        'resources': [],
    }

    # Define the collection name - for other datasets this just uses the specimen collection
    # But we use a reference to the collection built by the aggregation query
    collection_name = 'ecatalogue'

    # Prefix for denoting fields to use as DynamicProperties
    dynamic_property_prefix = '_dynamicProperties.'

    columns = [
        ('_id', '_id', False, 'int32'),

        # Identifier
        ('DarGlobalUniqueIdentifier', 'globalUniqueIdentifier', False, 'string:100'),

        # Record level
        ('AdmDateModified', 'dateLastModified', False, 'string:100'),
        # This isn't actually in DwC - but I'm going to use dcterms:created
        ('AdmDateInserted', 'created', False, 'string:100',),
        ('DarInstitutionCode', 'institutionCode', True, 'string:100'),
        ('DarCollectionCode', 'collectionCode', True, 'string:100'),
        ('DarBasisOfRecord', 'basisOfRecord', True, 'string:100'),

        # Taxonomy
        ('DarScientificName', 'scientificName', True, 'string:100'),
        ('DarScientificNameAuthor', 'scientificNameAuthor', True, 'string:100'),
        ('DarScientificNameAuthorYear', 'scientificNameAuthorYear', True, 'string:100'),
        ('DarKingdom', 'kingdom', True, 'string:100'),
        ('DarPhylum', 'phylum', True, 'string:100'),
        ('DarClass', 'class', True, 'string:100'),
        ('DarOrder', 'order', True, 'string:100'),
        ('DarFamily', 'family', True, 'string:100'),
        ('DarGenus', 'genus', True, 'string:100'),
        ('DarSubgenus', 'subgenus', True, 'string:100'),
        ('DarSpecies', 'species', True, 'string:100'),
        ('DarSubspecies', 'subspecies', True, 'string:100'),
        ('DarHigherTaxon', 'higherTaxon', True, 'string:100'),
        ('DarInfraspecificRank', 'infraspecificRank', True, 'string:100'),

        # Location
        ('DarDecimalLongitude', 'decimalLongitude', True, 'float32'),
        ('DarDecimalLatitude', 'decimalLatitude', True, 'float32'),
        ('DarGeodeticDatum', 'geodeticDatum', True, 'string:100'),
        ('DarGeorefMethod', 'georefMethod', True, 'string:100'),

        ('DarMinimumElevationInMeters', 'minimumElevationInMeters', True, 'string:100'),
        ('DarMaximumElevationInMeters', 'maximumElevationInMeters', True, 'string:100'),
        ('DarMinimumDepthInMeters', 'minimumDepthInMeters', True, 'string:100'),
        ('DarMaximumDepthInMeters', 'maximumDepthInMeters', True, 'string:100'),

        ('DarIsland', 'island', True, 'string:100'),
        ('DarIslandGroup', 'islandGroup', True, 'string:100'),
        ('DarContinentOcean', 'continentOcean', True, 'string:100'),
        ('DarWaterBody', 'waterBody', True, 'string:100'),

        ('DarLocality', 'locality', True, 'string:100'),
        ('DarStateProvince', 'stateProvince', True, 'string:100'),
        ('DarCountry', 'country', True, 'string:100'),
        ('DarContinent', 'continent', True, 'string:100'),
        ('DarHigherGeography', 'DarHigherGeography', True, 'string:100'),

        # Occurrence
        ('DarCatalogNumber', 'catalogNumber', True, 'string:100'),
        ('DarOtherCatalogNumbers', 'otherCatalogNumbers', True, 'string:100'),
        ('DarCatalogNumberText', 'catalogNumberText', True, 'string:100'),
        ('DarCollector', 'collector', True, 'string:100'),
        ('DarCollectorNumber', 'collectorNumber', True, 'string:100'),
        ('DarIndividualCount', 'DarIndividualCount', True, 'string:100'),
        ('DarLifeStage', 'lifeStage', True, 'string:100'),
        ('DarAgeClass', 'ageClass', True, 'string:100'),  # According to docs, ageClass has been superseded by lifeStage. We have both
        ('DarSex', 'sex', True, 'string:100'),
        ('DarPreparations', 'preparations', True, 'string:100'),
        ('DarPreparationType', 'preparationType', True, 'string:100'),
        ('DarObservedWeight', 'observedWeight', True, 'string:100'), # This has moved to dynamicProperties

        # Identification
        ('DarIdentifiedBy', 'identifiedBy', True, 'string:100'),
        ('DarDayIdentified', 'dayIdentified', True, 'string:100'),
        ('DarMonthIdentified', 'monthIdentified', True, 'string:100'),
        ('DarYearIdentified', 'yearIdentified', True, 'string:100'),
        ('DarIdentificationQualifier', 'identificationQualifier', True, 'string:100'),
        ('DarTypeStatus', 'typeStatus', True, 'string:100'),

        # Collection event
        ('DarFieldNumber', 'fieldNumber', True, 'string:100'),
        ('DarStartTimeOfDay', 'startTimeOfDay', True, 'string:100'),
        ('DarStartDayCollected', 'startDayCollected', True, 'string:100'),
        ('DarStartMonthCollected', 'startMonthCollected', True, 'string:100'),
        ('DarStartYearCollected', 'startYearCollected', True, 'string:100'),
        ('DarTimeOfDay', 'timeOfDay', True, 'string:100'),
        ('DarDayCollected', 'dayCollected', True, 'string:100'),
        ('DarMonthCollected', 'monthCollected', True, 'string:100'),
        ('DarYearCollected', 'yearCollected', True, 'string:100'),
        ('DarEndTimeOfDay', 'endTimeOfDay', True, 'string:100'),
        ('DarEndDayCollected', 'endDayCollected', True, 'string:100'),
        ('DarEndMonthCollected', 'endMonthCollected', True, 'string:100'),
        ('DarEndYearCollected', 'endYearCollected', True, 'string:100'),

        # Resource relationship
        # TODO: Need to do this one properly? Parts?
        ('DarRelatedCatalogItem', 'relatedCatalogItem', True, 'string:100'),

        # Extra fields we need to map - TODO: location irn?

        ('dynamicProperties', 'dynamicProperties', False, 'string:300'),

        # Removed: We do not want notes, could contain anything
        # ('DarNotes', 'DarNotes', True, 'string:100'),
        # ('DarLatLongComments', 'latLongComments', True, 'string:100'),
    ]

    # Dynamic properties - these will map into one dynamicProperties field
    dynamic_property_columns = [
        ('ColRecordType', 'colRecordType', False),
        ('ColSubDepartment', 'colSubDepartment', True),
        ('PrtType', 'prtType', False),
        ('RegCode', 'regCode', False),
        ('CatKindOfObject', 'kindOfObject', False),
        ('CatKindOfCollection', 'kindOfCollection', False),
        ('CatPreservative', 'catPreservative', False),
        ('ColKind', 'collectionKind', False),
    ]

    def part_parent_aggregator(self):
        """
        Part / Parents using an aggregator which needs to be initiated before running
        @return: status dict
        """
        query = list()

        # Exclude all types except Parent and Part types
        query.append({'$match': {"ColRecordType": {"$in": PARENT_TYPES + PART_TYPES}}})

        # Select all fields
        project = {col[0]: 1 for col in self.columns + self.dynamic_property_columns}
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
        group = {col[0]: {"%s" % '$first' if col[2] else '$last': "$%s" % col[0]} for col in self.columns + self.dynamic_property_columns}

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
        query.append({'$match': {"ColRecordType": {"$nin": PARENT_TYPES + PART_TYPES}}})
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


    def process_dataframe(self, m, df):

        pass

        # parent_irns = pd.unique(df._parentRef.values.ravel()).tolist()

        # self.get_parents(m, parent_irns)

    # def requires(self):
    #
    #     sys.exit()

    # def get_parents(self, m, irns):
    #
    #     ke_cols, df_cols, types = zip(*self.columns)
    #
    #     q = {'_id': {'$in': irns}, "ColRecordType": {"$in": ['Bird Group Parent', 'Mammal Group Parent']}}
    #
    #     query = m.query(self.database, self.collection_name, q, ke_cols, types)
    #     df = pd.DataFrame(np.matrix(query).transpose(), columns=df_cols)


