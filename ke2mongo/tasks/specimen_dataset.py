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

    # TODO: Merge string + true etc., if has output format, it'll run

    columns = [
        ('_id', '_id', 'int32', True),
        ('DarPreparations', 'DarPreparations', 'string:100', True),
        ('DarPreparationType', 'DarPreparationType', 'string:100', True),
        ('DarRelatedCatalogItem', 'DarRelatedCatalogItem', 'string:100', True),
        ('DarObservedWeight', 'DarObservedWeight', 'string:100', True),
        ('DarEndTimeOfDay', 'DarEndTimeOfDay', 'string:100', True),
        ('DarAgeClass', 'DarAgeClass', 'string:100', True),
        ('DarStartTimeOfDay', 'DarStartTimeOfDay', 'string:100', True),
        ('DarTimeOfDay', 'DarTimeOfDay', 'string:100', True),
        ('DarMaximumElevationInMeters', 'DarMaximumElevationInMeters', 'string:100', True),
        ('DarInfraspecificRank', 'DarInfraspecificRank', 'string:100', True),
        ('DarGeodeticDatum', 'DarGeodeticDatum', 'string:100', True),
        ('DarMaximumDepthInMeters', 'DarMaximumDepthInMeters', 'string:100', True),
        ('DarMinimumElevationInMeters', 'DarMinimumElevationInMeters', 'string:100', True),
        ('DarMinimumDepthInMeters', 'DarMinimumDepthInMeters', 'string:100', True),
        ('DarDayIdentified', 'DarDayIdentified', 'string:100', True),
        ('DarMonthIdentified', 'DarMonthIdentified', 'string:100', True),
        ('DarIdentificationQualifier', 'DarIdentificationQualifier', 'string:100', True),
        ('DarIsland', 'DarIsland', 'string:100', True),
        ('DarSubgenus', 'DarSubgenus', 'string:100', True),
        ('DarIslandGroup', 'DarIslandGroup', 'string:100', True),
        ('DarGeorefMethod', 'DarGeorefMethod', 'string:100', True),
        ('DarYearIdentified', 'DarYearIdentified', 'string:100', True),
        ('DarContinentOcean', 'DarContinentOcean', 'string:100', True),
        ('DarWaterBody', 'DarWaterBody', 'string:100', True),
        ('DarFieldNumber', 'DarFieldNumber', 'string:100', True),
        ('DarSubspecies', 'DarSubspecies', 'string:100', True),
        ('DarIdentifiedBy', 'DarIdentifiedBy', 'string:100', True),
        ('DarTypeStatus', 'DarTypeStatus', 'string:100', True),
        ('DarNotes', 'DarNotes', 'string:100', True),
        ('DarLifeStage', 'DarLifeStage', 'string:100', True),
        ('DarDateLastModified', 'DarDateLastModified', 'string:100', True),
        ('DarScientificNameAuthorYear', 'DarScientificNameAuthorYear', 'string:100', True),
        ('DarOtherCatalogNumbers', 'DarOtherCatalogNumbers', 'string:100', True),
        ('DarLatLongComments', 'DarLatLongComments', 'string:100', True),
        ('DarSex', 'DarSex', 'string:100', True),
        ('DarKingdom', 'DarKingdom', 'string:100', True),
        ('DarStateProvince', 'DarStateProvince', 'string:100', True),
        ('DarDecimalLongitude', 'DarDecimalLongitude', 'string:100', True),
        ('DarDecimalLatitude', 'DarDecimalLatitude', 'string:100', True),
        ('DarStartDayCollected', 'DarStartDayCollected', 'string:100', True),
        ('DarEndDayCollected', 'DarEndDayCollected', 'string:100', True),
        ('DarDayCollected', 'DarDayCollected', 'string:100', True),
        ('DarCollectorNumber', 'DarCollectorNumber', 'string:100', True),
        ('DarStartMonthCollected', 'DarStartMonthCollected', 'string:100', True),
        ('DarEndMonthCollected', 'DarEndMonthCollected', 'string:100', True),
        ('DarMonthCollected', 'DarMonthCollected', 'string:100', True),
        ('DarScientificNameAuthor', 'DarScientificNameAuthor', 'string:100', True),
        ('DarPhylum', 'DarPhylum', 'string:100', True),
        ('DarOrder', 'DarOrder', 'string:100', True),
        ('DarLocality', 'DarLocality', 'string:100', True),
        ('DarStartYearCollected', 'DarStartYearCollected', 'string:100', True),
        ('DarEndYearCollected', 'DarEndYearCollected', 'string:100', True),
        ('DarYearCollected', 'DarYearCollected', 'string:100', True),
        ('DarClass', 'DarClass', 'string:100', True),
        ('DarContinent', 'DarContinent', 'string:100', True),
        ('DarIndividualCount', 'DarIndividualCount', 'string:100', True),
        ('DarCollector', 'DarCollector', 'string:100', True),
        ('DarFamily', 'DarFamily', 'string:100', True),
        ('DarCountry', 'DarCountry', 'string:100', True),
        ('DarHigherTaxon', 'DarHigherTaxon', 'string:100', True),
        ('DarHigherGeography', 'DarHigherGeography', 'string:100', True),
        ('DarSpecies', 'DarSpecies', 'string:100', True),
        ('DarGenus', 'DarGenus', 'string:100', True),
        ('DarCatalogNumberText', 'DarCatalogNumberText', 'string:100', True),
        ('DarCatalogNumber', 'DarCatalogNumber', 'string:100', True),
        ('DarScientificName', 'DarScientificName', 'string:100', True),
        ('DarBasisOfRecord', 'DarBasisOfRecord', 'string:100', True),
        ('DarCollectionCode', 'DarCollectionCode', 'string:100', True),
        ('DarGlobalUniqueIdentifier', 'DarGlobalUniqueIdentifier', 'string:100', True),
        ('DarInstitutionCode', 'DarInstitutionCode', 'string:100', True)
    ]

    query = {"ColRecordType": {"$nin": ["Index Lot", "Artefact"]}}



