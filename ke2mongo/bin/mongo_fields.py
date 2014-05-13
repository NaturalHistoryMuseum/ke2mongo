import sys
from pymongo import MongoClient

#!/usr/bin/env python
# encoding: utf-8
"""
untitled.py

Created by Ben Scott on 2013-08-23.
Copyright (c) 2013 __MyCompanyName__. All rights reserved.
"""

import sys
import os

def main():
    
    # Setup MongoDB
    client = MongoClient()
    mongo_db = client['keemu']
    
    fields = [
        'DarLocality',
        'DarVerbatimElevation',
        'DarInfraspecificRank',
        'DarDayIdentified',
        'DarMinimumDepthInMeters',
        'DarMonthIdentified',
        'DarMaximumDepthInMeters',
        'DarIndividualCount',
        'DarMaximumDepth',
        'DarVerbatimCollectingDate',
        'DarTissues',
        'DarScientificNameAuthorYear',
        'DarVerbatimLongitude',
        'DarNotes',
        'DarCollectorNumber',
        'DarGenBankNum',
        'DarIdentificationModifier',
        'DarMinimumDepth',
        'DarLatLongComments',
        'DarIsland',
        'DarPreviousCatalogNumber',
        'DarEndTimeOfDay',
        'DarYearCollected',
        'DarVerbatimDepth',
        'DarCatalogNumber',
        'DarOriginalCoordinateSystem',
        'DarScientificNameAuthor',
        'DarOtherCatalogNumbers',
        'DarSubgenus',
        'DarFieldNumber',
        'DarYearIdentified',
        'DarRelationshipType',
        'DarEndMonthCollected',
        'DarInfraspecificEpithet',
        'DarAgeClass',
        'DarRemarks',
        'DarGeodeticDatum',
        'DarKingdom',
        'DarStart_EndCoordinatePrecision',
        'DarCoordinatePrecision',
        'DarStartTimeOfDay',
        'DarSpecificEpithet',
        'DarDecimalLongitude',
        'DarLatitude',
        'DarCitation',
        'DarLifeStage',
        'DarFamily',
        'DarStartYearCollected',
        'DarEndLatitude',
        'DarBasisOfRecord',
        'DarMaximumElevation',
        'DarStartLatitude',
        'DarCounty',
        'DarRelatedInformation',
        'DarObservedIndividualCount',
        'DarSource',
        'DarRecordURL',
        'DarIslandGroup',
        'DarWaterBody',
        'DarCoordinateUncertaintyInMeter',
        'DarSex',
        'DarStartDayCollected',
        'DarVerbatimLatitude',
        'DarGenus',
        'DarTimeOfDay',
        'DarImageURL',
        'DarDecimalLatitude',
        'DarTypeStatus',
        'DarStateProvince',
        'DarBoundingBox',
        'DarGeorefMethod',
        'DarScientificName',
        'DarCollectionCode',
        'DarLongitude',
        'DarGlobalUniqueIdentifier',
        'DarInstitutionCode',
        'DarRelatedCatalogItem',
        'DarTimeCollected',
        'DarPreparations',
        'DarContinent',
        'DarEndJulianDay',
        'DarGMLFeature',
        'DarCountry',
        'DarJulianDay',
        'DarSubspecies',
        'DarFieldNotes',
        'DarMaximumElevationInMeters',
        'DarContinentOcean',
        'DarIdentificationQualifier',
        'DarTimeZone',
        'DarEndLongitude',
        'DarHorizontalDatum',
        'DarClass',
        'DarRelatedCatalogItems',
        'DarPhylum',
        'DarStartMonthCollected',
        'DarHigherGeography',
        'DarDepthRange',
        'DarDateLastModified',
        'DarCollector',
        'DarObservedWeight',
        'DarMinimumElevationInMeters',
        'DarHigherTaxon',
        'DarStartJulianDay',
        'DarDayCollected',
        'DarTemperature',
        'DarEndDayCollected',
        'DarStartLongitude',
        'DarCatalogNumberNumeric',
        'DarOrder',
        'DarMinimumElevation',
        'DarPreparationType',
        'DarEndYearCollected',
        'DarMonthCollected',
        'DarIdentifiedBy',
        'DarCatalogNumberText',
        'DarSpecies'
    ]
    
    for field in fields:
        results = mongo_db.ecatalogue.find({field: {'$exists': 1}})
        print '{0}:\t{1}\r'.format(field,  results.count())

#

if __name__ == '__main__':
	main()
