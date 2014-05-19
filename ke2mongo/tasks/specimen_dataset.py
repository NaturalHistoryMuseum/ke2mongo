#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

from ke2mongo.tasks.dataset import DatasetTask
from random import randint
import sys
from collections import OrderedDict

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
    # But we use a reference to the collection build by the aggregation query
    collection_name = 'agg_dwc'

    columns = [
        ('_id', '_id', 'int32'),

        # Identifier
        ('DarGlobalUniqueIdentifier', 'globalUniqueIdentifier', 'string:100'),

        # Record level
        ('AdmDateModified', 'dateLastModified', 'string:100'),
        # This isn't actually in DwC - but I'm going to use dcterms:created
        ('AdmDateInserted', 'created', 'string:100'),
        ('DarInstitutionCode', 'institutionCode', 'string:100'),
        ('DarCollectionCode', 'collectionCode', 'string:100'),
        ('DarBasisOfRecord', 'basisOfRecord', 'string:100'),

        # Taxonomy
        ('DarScientificName', 'scientificName', 'string:100'),
        ('DarScientificNameAuthor', 'scientificNameAuthor', 'string:100'),
        ('DarScientificNameAuthorYear', 'scientificNameAuthorYear', 'string:100'),
        ('DarKingdom', 'kingdom', 'string:100'),
        ('DarPhylum', 'phylum', 'string:100'),
        ('DarClass', 'class', 'string:100'),
        ('DarOrder', 'order', 'string:100'),
        ('DarFamily', 'family', 'string:100'),
        ('DarGenus', 'genus', 'string:100'),
        ('DarSubgenus', 'subgenus', 'string:100'),
        ('DarSpecies', 'species', 'string:100'),
        ('DarSubspecies', 'subspecies', 'string:100'),
        ('DarHigherTaxon', 'higherTaxon', 'string:100'),
        ('DarInfraspecificRank', 'infraspecificRank', 'string:100'),

        # Location
        ('DarDecimalLongitude', 'decimalLongitude', 'float32', True),
        ('DarDecimalLatitude', 'decimalLatitude', 'float32'),
        ('DarGeodeticDatum', 'geodeticDatum', 'string:100'),
        ('DarGeorefMethod', 'georefMethod', 'string:100'),

        ('DarMinimumElevationInMeters', 'minimumElevationInMeters', 'string:100'),
        ('DarMaximumElevationInMeters', 'maximumElevationInMeters', 'string:100'),
        ('DarMinimumDepthInMeters', 'minimumDepthInMeters', 'string:100'),
        ('DarMaximumDepthInMeters', 'maximumDepthInMeters', 'string:100'),

        ('DarIsland', 'island', 'string:100'),
        ('DarIslandGroup', 'islandGroup', 'string:100'),
        ('DarContinentOcean', 'continentOcean', 'string:100'),
        ('DarWaterBody', 'waterBody', 'string:100'),

        ('DarLocality', 'locality', 'string:100'),
        ('DarStateProvince', 'stateProvince', 'string:100'),
        ('DarCountry', 'country', 'string:100'),
        ('DarContinent', 'continent', 'string:100'),
        ('DarHigherGeography', 'DarHigherGeography', 'string:100'),

        # Occurrence
        ('DarCatalogNumber', 'catalogNumber', 'string:100'),
        ('DarOtherCatalogNumbers', 'otherCatalogNumbers', 'string:100'),
        ('DarCatalogNumberText', 'catalogNumberText', 'string:100'),
        ('DarCollector', 'collector', 'string:100'),
        ('DarCollectorNumber', 'collectorNumber', 'string:100'),
        ('DarIndividualCount', 'DarIndividualCount', 'string:100'),
        ('DarLifeStage', 'lifeStage', 'string:100'),
        ('DarAgeClass', 'ageClass', 'string:100'),  # According to docs, ageClass has been superseded by lifeStage. We have both
        ('DarSex', 'sex', 'string:100'),
        ('DarPreparations', 'preparations', 'string:100'),
        ('DarPreparationType', 'preparationType', 'string:100'),
        ('DarObservedWeight', 'observedWeight', 'string:100'), # This has moved to dynamicProperties

        # Identification
        ('DarIdentifiedBy', 'identifiedBy', 'string:100'),
        ('DarDayIdentified', 'dayIdentified', 'string:100'),
        ('DarMonthIdentified', 'monthIdentified', 'string:100'),
        ('DarYearIdentified', 'yearIdentified', 'string:100'),
        ('DarIdentificationQualifier', 'identificationQualifier', 'string:100'),
        ('DarTypeStatus', 'typeStatus', 'string:100'),

        # Collection event
        ('DarFieldNumber', 'fieldNumber', 'string:100'),
        ('DarStartTimeOfDay', 'startTimeOfDay', 'string:100'),
        ('DarStartDayCollected', 'startDayCollected', 'string:100'),
        ('DarStartMonthCollected', 'startMonthCollected', 'string:100'),
        ('DarStartYearCollected', 'startYearCollected', 'string:100'),
        ('DarTimeOfDay', 'timeOfDay', 'string:100'),
        ('DarDayCollected', 'dayCollected', 'string:100'),
        ('DarMonthCollected', 'monthCollected', 'string:100'),
        ('DarYearCollected', 'yearCollected', 'string:100'),
        ('DarEndTimeOfDay', 'endTimeOfDay', 'string:100'),
        ('DarEndDayCollected', 'endDayCollected', 'string:100'),
        ('DarEndMonthCollected', 'endMonthCollected', 'string:100'),
        ('DarEndYearCollected', 'endYearCollected', 'string:100'),

        # Resource relationship
        # TODO: Need to do this one properly
        ('DarRelatedCatalogItem', 'relatedCatalogItem', 'string:100'),

        # Extra fields we need to map
        # TODO: Know, we want this in dynamic properties
        # ('ColRecordType', '_colRecordType', 'string:100'),
        # ('ColSubDepartment', '_colSubDepartment', 'string:100'),
        # ('RegCode', 'RegCode', 'string:100')

        # Removed: We do not want notes, could contain anything
        # ('DarNotes', 'DarNotes', 'string:100'),
        # ('DarLatLongComments', 'latLongComments', 'string:100'),
    ]


    @property
    def query(self):

        # TODO: This is very slow. Try pandas approach.
        # TODO: Order by ColRecordType - Less calls to mongo.

        # Prior to returning the query, build the aggregation object
        collection = self.mongo.get_collection()

        query = list()

        # Exclude Index Lots and Artefacts
        query.append({'$match': {"ColRecordType": {"$nin": ["Index Lot", "Artefact"]}}})

        # Select all fields
        project = {col[0]:1 for col in self.columns}
        # Add the PartRef field so we can unwind it
        project['part_id'] = {"$ifNull": ["$PartRef", [None]]}
        project['DarCatalogNumber'] = {"$ifNull": ["$DarCatalogNumber", "$RegRegistrationNumber"]}
        # Manually add this field, as this process will break if it doesn't exist
        project['ColRecordType'] = 1
        # We cannot rely on the DarGlobalUniqueIdentifier field, as parts do not have it, so build manually
        project['DarGlobalUniqueIdentifier'] = {"$concat": ["NHMUK", "$irn"]}

        # TODO: dynamicProperties
        # project['dynamicProperties'] = {"$concat": [{"$last": "$ColRecordType"}, ";"]}

        query.append({'$project': project})

        # Unwind based on part ID
        query.append({'$unwind': "$part_id"})

        # And these fields use the parent
        group = {col[0]:{"$first": "$%s" % col[0]} for col in self.columns}
        group['_id'] = {"$ifNull": ["$part_id", "$_id"]}
        # These fields should always use the Part data.
        # If they aren't populated, then use the
        group['DarGlobalUniqueIdentifier'] = {"$last": "$DarGlobalUniqueIdentifier"}

        group['DarCatalogNumber'] = {"$last": "$DarCatalogNumber"}
        group['AdmDateInserted'] = {"$last": "$AdmDateInserted"}
        group['AdmDateModified'] = {"$last": "$AdmDateModified"}
        group['DarDateLastModified'] = {"$last": "$DarDateLastModified"}

        query.append({'$group': group})

        query.append({'$match': {"ColRecordType": {"$nin": self.mongo.parent_types}}})

        # Output to DwC collection
        query.append({'$out': self.collection_name})

        collection.aggregate(query, allowDiskUse=True)

        # We're querying against the collection we've just made, so no query parameters are necessary
        return {}



    # "_id" :  {"$in": [1751672, 1751675, 1751681, 1751684, 1751687, 1751690, 1751693, 1751696, 1751699, 1751702]}



    # query = {"ColRecordType": {"$nin": ["Index Lot", "Artefact"]}, "_id" :  {"$in": [467777,475975,478089,4080610,1920482,2494268,271212,1953121,1751693,1953125,1953129,1953130,1112389,1181206,661581,1291338,443462,3255480,3255496,3255497,3255498,3255499,3255500,3255506,3255507,3255509,3255512,3255513,3255514,3255515,3255516,3255523,3255529,3255530,3255531,3255532,3255533,3255535,3255536,3255542,3255544,3255545,3255546,3255553,3255554,3255555,3255556,3255557,3255561,439233,3255558,3255559,3255562,3255563,3255564,3255565,3255566,3255567,3255570,3255571,3255573,3255574,3255575,3255576,3255577,3255578,3255579,3255580,3255581,3255582,3255587,3255592,3255600,3255601,3255602,3255603,3255604,3255605,3255607,3255608,3255609,3255611]}}

    # query = {"ColRecordType": {"$nin": ["Index Lot", "Artefact"]}}

    # def requires(self):
    #
    #     sys.exit()

