#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.

python specimen.py SpecimenDatasetToCKANTask --local-scheduler --date 20140731
python specimen.py SpecimenDatasetToCSVTask --local-scheduler --date 20140821

"""

import os
import sys
import luigi
import itertools
import pandas as pd
from pymongo import MongoClient

import numpy as np
from ke2mongo import config
from ke2mongo.tasks import PARENT_TYPES, PART_TYPES, MULTIMEDIA_URL, MULTIMEDIA_FORMATS
from ke2mongo.tasks.dataset import DatasetTask, DatasetToCSVTask, DatasetToCKANTask
from ke2mongo.tasks.target import CSVTarget, CKANTarget
from ke2mongo.tasks.artefact import ArtefactDatasetTask
from ke2mongo.tasks.indexlot import IndexLotDatasetTask
from ke2mongo.log import log
from collections import OrderedDict


class SpecimenDatasetTask(DatasetTask):

    columns = [
        # List of columns
        # ([KE EMu field], [new field], [field type], indexed)

        # Identifier
        ('DarGlobalUniqueIdentifier', 'occurrenceID', 'string:100', True),

        # Record level
        ('AdmDateModified', 'modified', 'string:100', True),
        # This isn't actually in DwC - but I'm going to use dcterms:created
        ('AdmDateInserted', 'created', 'string:100', True),
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
        ('DarHigherTaxon', 'higherClassification', 'string:100', True),
        ('DarInfraspecificRank', 'taxonRank', 'string:100', True),

        # Location
        # The encoding of DarLocality is buggered - see ecatalogue.1804973
        # So better to use the original field with the correct encoding
        ('sumPreciseLocation', 'locality', 'string:100', True),
        ('DarStateProvince', 'stateProvince', 'string:100', True),
        ('DarCountry', 'country', 'string:100', True),
        ('DarContinent', 'continent', 'string:100', True),
        ('DarIsland', 'island', 'string:100', True),
        ('DarIslandGroup', 'islandGroup', 'string:100', True),
        # Removed: continentOcean is not in current DwC standard, replaced by waterBody and continent
        # ('DarContinentOcean', 'continentOcean', 'string:100', True),
        ('DarWaterBody', 'waterBody', 'string:100', True),
        ('DarHigherGeography', 'higherGeography', 'string:100', True),
        ('ColHabitatVerbatim', 'habitat', 'string:100', True),

        ('DarDecimalLongitude', 'decimalLongitude', 'float32', True),
        ('DarDecimalLatitude', 'decimalLatitude', 'float32', True),
        ('DarGeodeticDatum', 'geodeticDatum', 'string:100', True),
        ('DarGeorefMethod', 'georeferenceProtocol', 'string:100', True),

        ('DarMinimumElevationInMeters', 'minimumElevationInMeters', 'string:100', True),
        ('DarMaximumElevationInMeters', 'maximumElevationInMeters', 'string:100', True),
        ('DarMinimumDepthInMeters', 'minimumDepthInMeters', 'string:100', True),
        ('DarMaximumDepthInMeters', 'maximumDepthInMeters', 'string:100', True),

        # Occurrence
        ('DarCatalogNumber', 'catalogNumber', 'string:100', True),
        ('DarOtherCatalogNumbers', 'otherCatalogNumbers', 'string:100', True),
        ('DarCollector', 'recordedBy', 'string:100', True),
        ('DarCollectorNumber', 'recordNumber', 'string:100', True),
        ('DarIndividualCount', 'individualCount', 'string:100', True),
        ('DarLifeStage', 'lifeStage', 'string:100', True),
        # According to docs, ageClass has been superseded by lifeStage. We have both, but ageClass duplicates
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
        ('DarRelatedCatalogItem', 'relatedResourceID', 'string:100', False),
        # Dynamic properties
        ('dynamicProperties', 'dynamicProperties', 'string:400', False),
        # Multimedia
        ('MulMultiMediaRef', 'associatedMedia', 'string:100', False),

        # Private, internal-only fields
        ('RegRegistrationParentRef', '_parentRef', 'int32', False),
        ('_id', '_id', 'int32', False),


        # Removed: We do not want notes, could contain anything
        # ('DarNotes', 'DarNotes', 'string:100'),
        # ('DarLatLongComments', 'latLongComments', 'string:100'),
    ]

    # Dynamic properties - these will map into one dynamicProperties field
    # They are use in the aggregator, not the monary query so specifying type isn't required
    dynamic_property_columns = [
        ('ColRecordType', 'recordType'),
        ('ColSubDepartment', 'subDepartment'),
        ('PrtType', 'partType'),
        ('RegCode', 'registrationCode'),
        ('CatKindOfObject', 'kindOfObject'),
        ('CatKindOfCollection', 'kindOfCollection'),
        ('CatPreservative', 'preservative'),
        ('ColKind', 'collectionKind'),
        ('EntPriCollectionName', 'collectionName'),
        ('PartRefStr', 'partRefs'),
        ('PalAcqAccLotDonorFullName', 'donorName'),
        ('DarPreparationType', 'preparationType'),
        ('DarObservedWeight', 'observedWeight'),
        # Extra fields from specific KE EMu record types
        # No need to inherit these properties - not parts etc.,
        # DNA
        ('DnaExtractionMethod', 'extractionMethod'),
        ('DnaReSuspendedIn', 'resuspendedIn'),
        ('DnaTotalVolume', 'totalVolume'),
        # Parasite card
        ('CardBarcode', 'barcode'),
        # Egg
        ('EggClutchSize', 'clutchSize'),
        ('EggSetMark', 'setMark'),
        # Nest
        ('NesShape', 'nestShape'),
        ('NesSite', 'nestSite'),
        # Silica gel
        ('SilPopulationCode', 'populationCode'),
        # Botany
        ('CollExsiccati', 'exsiccati'),
        ('ColExsiccatiNumber', 'exsiccatiNumber'),
        ('ColSiteDescription', 'siteDescription'),  # This is called "Label locality" in existing NHM online DBs
        ('ColPlantDescription', 'plantDescription'),
        ('FeaCultivated', 'cultivated'),
        ('FeaPlantForm', 'plantForm'),
        # Paleo
        ('PalDesDescription', 'catalogueDescription'),
        ('PalStrChronostratLocal', 'chronostratigraphy'),
        ('PalStrLithostratLocal', 'lithostratigraphy'),
        # Mineralogy
        ('MinDateRegistered', 'dateRegistered'),
        ('MinIdentificationAsRegistered', 'identificationAsRegistered'),
        ('MinIdentificationDescription', 'identificationDescription'),
        ('MinPetOccurance', 'occurrence'),
        ('MinOreCommodity', 'commodity'),
        ('MinOreDepositType', 'depositType'),
        ('MinTextureStructure', 'texture'),
        ('MinIdentificationVariety', 'identificationVariety'),
        ('MinIdentificationOther', 'identificationOther'),
        ('MinHostRock', 'hostRock'),
        ('MinAgeDataAge', 'age'),
        ('MinAgeDataType', 'ageType'),
        # Mineralogy location
        ('MinNhmTectonicProvinceLocal', 'tectonicProvince'),
        ('MinNhmStandardMineLocal', 'mine'),
        ('MinNhmMiningDistrictLocal', 'miningDistrict'),
        ('MinNhmComplexLocal', 'mineralComplex'),
        ('MinNhmRegionLocal', 'geologyRegion'),
        # Meteorite
        ('MinMetType', 'meteoriteType'),
        ('MinMetGroup', 'meteoriteGroup'),
        ('MinMetChondriteAchondrite', 'chondriteAchondrite'),
        ('MinMetClass', 'meteoriteClass'),
        ('MinMetPetType', 'petType'),
        ('MinMetPetSubtype', 'petSubType'),
        ('MinMetRecoveryFindFall', 'recovery'),
        ('MinMetRecoveryDate', 'recoveryDate'),
        ('MinMetRecoveryWeight', 'recoveryWeight'),
        ('MinMetWeightAsRegistered', 'registeredWeight'),
        ('MinMetWeightAsRegisteredUnit', 'registeredWeightUnit'),
    ]

    query = {}  # Selecting data is moved to aggregation query

    def run(self):

        # Before running, build aggregation query
        self.build_aggregation_query()

        super(SpecimenDatasetTask, self).run()


        # # Do we have an aggregation query?
        # if self.aggregation_query:
        #     # Monary cannot use an aggregator query, so we'll output to another collection
        #     # and then query against that for everything {}
        #     collection_name = 'agg_%s' % self.collection_name
        #
        #     q = self.aggregation_query
        #
        #     # Add the output collection the query
        #     q.append({'$out': collection_name})
        #
        #     # Run the aggregation query
        #     log.info("Building aggregated collection: %s", collection_name)
        #     result = db[self.collection_name].aggregate(q, allowDiskUse=True)
        #
        #     # Ensure the aggregation process succeeded
        #     assert result['ok'] == 1.0

    def build_aggregation_query(self):
        """
        Build aggregation query
        The specimen dataset is too complicated, and we need to use MongoDB's aggregation queries
        Monary cannot handle aggregation queries however, so we'll build the aggregator, and then use {} for the query

        @return: Boolean, denoting success
        """

        mongo = MongoClient()
        db_collection = mongo[self.mongo_db][self.collection_name]

        # Update collection name to use agg_
        # Monary will query against the collection name
        self.collection_name = 'agg_%s' % self.collection_name

        # Build list of columns to select
        projection = {col[0]: 1 for col in self.columns}

        # Create an array of dynamicProperties to use in an aggregation projection
        # In the format {dynamicProperties : {$concat: [{$cond: {if: "$ColRecordType", then: {$concat: ["ColRecordType=","$ColRecordType", ";"]}, else: ''}}
        dynamic_properties = [{"$cond": OrderedDict([("if", "${}".format(col[0])), ("then", {"$concat": ["{}=".format(col[1]), "${}".format(col[0]), ";"]}), ("else", '')])} for col in self.dynamic_property_columns]

        projection['dynamicProperties'] = {"$concat": dynamic_properties}

        # We cannot rely on some DwC fields, as they are missing / incomplete for some records
        # So we manually add them based on other fields

        # If $DarCatalogNumber does not exist, we'll try use $GeneralCatalogueNumber
        # GeneralCatalogueNumber has min bm number - RegRegistrationNumber does not
        projection['DarCatalogNumber'] = {"$ifNull": ["$DarCatalogNumber", "$GeneralCatalogueNumber"]}
        # We cannot rely on the DarGlobalUniqueIdentifier field, as parts do not have it, so build manually
        projection['DarGlobalUniqueIdentifier'] = {"$concat": ["NHMUK:ecatalogue:", "$irn"]}

        # As above, need to manually build DarCollectionCode and DarInstitutionCode
        # These do need to be defined as columns, so the inheritance / new field name is used
        # But we are over riding the default behaviour (just selecting the column)
        projection['DarInstitutionCode'] = {"$literal": "NHMUK"}
        projection['DarBasisOfRecord'] = {"$literal": "Specimen"}
        # If an entom record collection code = BMNH(E), otherwise use PAL, MIN etc.,
        projection['DarCollectionCode'] = {
            "$cond": {
                "if": {"$eq": ["$ColDepartment", "Entomology"]},
                "then": "BMNH(E)",
                "else": {"$toUpper": {"$substr": ["$ColDepartment", 0, 3]}}
            }
        }

        # Build an aggregation query for parent records
        # Uses the same projection, so we can easily merge into part records when we're processing the dataframe

        parent_aggregation_query = list()

        # We do not want parent types - these will be merged in the DF

        parent_aggregation_query.append({'$match': {"ColRecordType": {"$in": PARENT_TYPES}}})
        parent_aggregation_query.append({'$project': projection})

        self.agg_parent_collection_name = '%s_parent' % self.collection_name

        # Add the output collection the query
        parent_aggregation_query.append({'$out': self.agg_parent_collection_name})

        log.info("Building parent aggregated collection: %s", self.agg_parent_collection_name)

        # TEMP: Put this back in
        # result = db_collection.aggregate(parent_aggregation_query, allowDiskUse=True)
        #
        # # Ensure the aggregation process succeeded
        # assert result['ok'] == 1.0

        # And now build the main aggregation query we'll use with Monary

        aggregation_query = list()

        # We do not want parent types - these will be merged in the DF
        match = {'$match': {"ColRecordType": {"$nin": PARENT_TYPES + [ArtefactDatasetTask.record_type, IndexLotDatasetTask.record_type]}}}

        # If we have a date. we're only going to get specimens imported on that date
        if self.date:
            match['$match']['exportFileDate'] = self.date

        aggregation_query.append(match)

        aggregation_query.append({'$limit': 500})

        aggregation_query.append({'$project': projection})

           # Add the output collection the query
        aggregation_query.append({'$out': self.collection_name})

        # Run the aggregation query
        log.info("Building aggregated collection: %s", self.collection_name)
        result = db_collection.aggregate(aggregation_query, allowDiskUse=True)

        # Ensure the aggregation process succeeded
        assert result['ok'] == 1.0


    @staticmethod
    def ensure_multimedia(m, df):

        # The multimedia field contains IRNS of all items - not just images
        # So we need to look up the IRNs against the multimedia record to get the mime type
        # And filter out non-image mimetypes we do not support

        # Convert associatedMedia field to a list
        df['associatedMedia'] = df['associatedMedia'].apply(lambda x: list(int(z.strip()) for z in x.split(';') if z.strip()))

        def get_valid_multimedia(multimedia_irns):
            """
            Get a data frame of taxonomy records
            @param m: Monary instance
            @param irns: taxonomy IRNs to retrieve
            @return:
            """
            q = {'_id': {'$in': multimedia_irns}, 'MulMimeFormat': {'$in': MULTIMEDIA_FORMATS}}
            ('_id', '_taxonomy_irn', 'int32'),
            query = m.query('keemu', 'emultimedia', q, ['_id'], ['int32'])
            return query[0].view()

        # Get a unique list of IRNS
        unique_multimedia_irns = list(set(itertools.chain(*[irn for irn in df.associatedMedia.values])))

        # Get a list of multimedia irns with valid mimetypes
        valid_multimedia = get_valid_multimedia(unique_multimedia_irns)

        # And finally update the associatedMedia field, so formatting with the IRN with MULTIMEDIA_URL, if the IRN is in valid_multimedia
        df['associatedMedia'] = df['associatedMedia'].apply(lambda irns: '; '.join(MULTIMEDIA_URL % irn for irn in irns if irn in valid_multimedia))

    def process_dataframe(self, m, df):
        """
        Process the dataframe, updating multimedia irns => URIs
        @param m: monary
        @param df: dataframe
        @return: dataframe
        """

        self.ensure_multimedia(m, df)

        # Process part parents
        parent_irns = pd.unique(df._parentRef.values.ravel()).tolist()

        if parent_irns:

            parent_df = self.get_dataframe(m, self.agg_parent_collection_name, self.columns, parent_irns, '_id')

            # Ensure the parent multimedia images are usable
            self.ensure_multimedia(m, parent_df)

            # Assign parentRef as the index to allow us to combine with parent_df
            df.index = df['_parentRef']

            # Convert empty strings to NaNs
            df = df.applymap(lambda x: np.nan if isinstance(x, basestring) and x == '' else x)

            # Which allows us to use combine_first() to replace NaNs with value from parent df
            df = df.combine_first(parent_df)

        return df


class SpecimenDatasetToCSVTask(SpecimenDatasetTask, DatasetToCSVTask):
    pass


class SpecimenDatasetToCKANTask(SpecimenDatasetTask, DatasetToCKANTask):
    package = {
        'name': 'specimens',
        'notes': u'The Natural History Museum\'s collection',
        'title': "NHM Collection",
        'author': 'Natural History Museum',
        'license_id': u'cc-by',
        'resources': [],
        'dataset_type': 'Specimen',
        'spatial': '{"type":"Polygon","coordinates":[[[-180,82],[180,82],[180,-82],[-180,-82],[-180,82]]]}',
        'owner_org': config.get('ckan', 'owner_org')
    }

    # And now save to the datastore
    datastore = {
        'resource': {
            'name': 'Test data',
            'description': 'Test data',
            'format': 'dwc'  # Darwin core
        },
    }

    primary_key = 'occurrenceID'

    geospatial_fields = {
        'latitude_field': 'decimalLatitude',
        'longitude_field': 'decimalLongitude'
    }