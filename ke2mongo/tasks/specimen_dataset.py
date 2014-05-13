#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

from ke2mongo.tasks.dataset import DatasetTask
from random import randint

class SpecimenDatasetTask(DatasetTask):
    """
    Class for creating specimens dataset
    """
    name = 'Specimens'
    description = 'Specimen records'
    format = 'dwc'  # Darwin Core format

    package = {
        'name': u'nhm-collection%s' % randint(1,1000),
        'notes': u'The Natural History Museum\'s collection',
        'title': "Collection",
        'author': None,
        'author_email': None,
        'license_id': u'other-open',
        'maintainer': None,
        'maintainer_email': None,
        'resources': [],
    }

    columns = [
        ('_id', '_id', 'int32'),
        ('DarPreparations', 'DarPreparations', 'string:100'),
        ('DarPreparationType', 'DarPreparationType', 'string:100'),
        ('DarRelatedCatalogItem', 'DarRelatedCatalogItem', 'string:100'),
        ('DarObservedWeight', 'DarObservedWeight', 'string:100'),
        ('DarEndTimeOfDay', 'DarEndTimeOfDay', 'string:100'),
        ('DarAgeClass', 'DarAgeClass', 'string:100'),
        ('DarStartTimeOfDay', 'DarStartTimeOfDay', 'string:100'),
        ('DarTimeOfDay', 'DarTimeOfDay', 'string:100'),
        ('DarMaximumElevationInMeters', 'DarMaximumElevationInMeters', 'string:100'),
        ('DarInfraspecificRank', 'DarInfraspecificRank', 'string:100'),
        ('DarGeodeticDatum', 'DarGeodeticDatum', 'string:100'),
        ('DarMaximumDepthInMeters', 'DarMaximumDepthInMeters', 'string:100'),
        ('DarMinimumElevationInMeters', 'DarMinimumElevationInMeters', 'string:100'),
        ('DarMinimumDepthInMeters', 'DarMinimumDepthInMeters', 'string:100'),
        ('DarDayIdentified', 'DarDayIdentified', 'string:100'),
        ('DarMonthIdentified', 'DarMonthIdentified', 'string:100'),
        ('DarIdentificationQualifier', 'DarIdentificationQualifier', 'string:100'),
        ('DarIsland', 'DarIsland', 'string:100'),
        ('DarSubgenus', 'DarSubgenus', 'string:100'),
        ('DarIslandGroup', 'DarIslandGroup', 'string:100'),
        ('DarGeorefMethod', 'DarGeorefMethod', 'string:100'),
        ('DarYearIdentified', 'DarYearIdentified', 'string:100'),
        ('DarContinentOcean', 'DarContinentOcean', 'string:100'),
        ('DarWaterBody', 'DarWaterBody', 'string:100'),
        ('DarFieldNumber', 'DarFieldNumber', 'string:100'),
        ('DarSubspecies', 'DarSubspecies', 'string:100'),
        ('DarIdentifiedBy', 'DarIdentifiedBy', 'string:100'),
        ('DarTypeStatus', 'DarTypeStatus', 'string:100'),
        ('DarNotes', 'DarNotes', 'string:100'),
        ('DarLifeStage', 'DarLifeStage', 'string:100'),
        ('DarDateLastModified', 'DarDateLastModified', 'string:100'),
        ('DarScientificNameAuthorYear', 'DarScientificNameAuthorYear', 'string:100'),
        ('DarOtherCatalogNumbers', 'DarOtherCatalogNumbers', 'string:100'),
        ('DarLatLongComments', 'DarLatLongComments', 'string:100'),
        ('DarSex', 'DarSex', 'string:100'),
        ('DarKingdom', 'DarKingdom', 'string:100'),
        ('DarStateProvince', 'DarStateProvince', 'string:100'),
        ('DarDecimalLongitude', 'DarDecimalLongitude', 'float32', True),
        ('DarDecimalLatitude', 'DarDecimalLatitude', 'float32'),
        ('DarStartDayCollected', 'DarStartDayCollected', 'string:100'),
        ('DarEndDayCollected', 'DarEndDayCollected', 'string:100'),
        ('DarDayCollected', 'DarDayCollected', 'string:100'),
        ('DarCollectorNumber', 'DarCollectorNumber', 'string:100'),
        ('DarStartMonthCollected', 'DarStartMonthCollected', 'string:100'),
        ('DarEndMonthCollected', 'DarEndMonthCollected', 'string:100'),
        ('DarMonthCollected', 'DarMonthCollected', 'string:100'),
        ('DarScientificNameAuthor', 'DarScientificNameAuthor', 'string:100'),
        ('DarPhylum', 'DarPhylum', 'string:100'),
        ('DarOrder', 'DarOrder', 'string:100'),
        ('DarLocality', 'DarLocality', 'string:100'),
        ('DarStartYearCollected', 'DarStartYearCollected', 'string:100'),
        ('DarEndYearCollected', 'DarEndYearCollected', 'string:100'),
        ('DarYearCollected', 'DarYearCollected', 'string:100'),
        ('DarClass', 'DarClass', 'string:100'),
        ('DarContinent', 'DarContinent', 'string:100'),
        ('DarIndividualCount', 'DarIndividualCount', 'string:100'),
        ('DarCollector', 'DarCollector', 'string:100'),
        ('DarFamily', 'DarFamily', 'string:100'),
        ('DarCountry', 'DarCountry', 'string:100'),
        ('DarHigherTaxon', 'DarHigherTaxon', 'string:100'),
        ('DarHigherGeography', 'DarHigherGeography', 'string:100'),
        ('DarSpecies', 'DarSpecies', 'string:100'),
        ('DarGenus', 'DarGenus', 'string:100'),
        ('DarCatalogNumberText', 'DarCatalogNumberText', 'string:100'),
        ('DarCatalogNumber', 'DarCatalogNumber', 'string:100'),
        ('DarScientificName', 'DarScientificName', 'string:100'),
        ('DarBasisOfRecord', 'DarBasisOfRecord', 'string:100'),
        ('DarCollectionCode', 'DarCollectionCode', 'string:100'),
        ('DarGlobalUniqueIdentifier', 'DarGlobalUniqueIdentifier', 'string:100'),
        ('DarInstitutionCode', 'DarInstitutionCode', 'string:100')
    ]

    query = {"ColRecordType": {"$nin": ["Index Lot", "Artefact"]}}



