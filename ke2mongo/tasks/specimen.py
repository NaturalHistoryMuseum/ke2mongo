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
from ke2mongo.tasks import PARENT_TYPES, PART_TYPES, MULTIMEDIA_URL, MULTIMEDIA_FORMATS
from ke2mongo.tasks.dataset import DatasetTask, DatasetToCSVTask, DatasetToCKANTask
from ke2mongo.tasks.target import CSVTarget, CKANTarget
from ke2mongo.tasks.artefact import ArtefactDatasetTask
from ke2mongo.tasks.indexlot import IndexLotDatasetTask, IndexLotDatasetToCKANTask
from ke2mongo.log import log
from collections import OrderedDict

# TODO: CSV task should check field order
# TODO: 

class SpecimenDatasetTask(DatasetTask):

    columns = [
        # List of columns
        # ([KE EMu field], [new field], [field type])

        # Identifier
        ('DarGlobalUniqueIdentifier', 'Occurrence ID', 'string:100'),

        # Record level
        ('AdmDateModified', 'Modified', 'string:100'),
        # This isn't actually in DwC - but I'm going to use dcterms:created
        ('AdmDateInserted', 'Created', 'string:100'),
        ('DarInstitutionCode', 'Institution code', 'string:100'),
        ('DarCollectionCode', 'Collection code', 'string:100'),
        ('DarBasisOfRecord', 'Basis of record', 'string:100'),

        # Taxonomy
        ('DarScientificName', 'Scientific name', 'string:100'),
        # Rather than using the two darwin core fields DarScientificNameAuthorYear and ScientificNameAuthor
        # It's easier to just use IdeFiledAsAuthors which has them both concatenated
        ('IdeFiledAsAuthors', 'Scientific name authorship', 'string:100'),
        ('DarKingdom', 'Kingdom', 'string:100'),
        ('DarPhylum', 'Phylum', 'string:100'),
        ('DarClass', 'Class', 'string:100'),
        ('DarOrder', 'Order', 'string:100'),
        ('DarFamily', 'Family', 'string:100'),
        ('DarGenus', 'Genus', 'string:100'),
        ('DarSubgenus', 'Subgenus', 'string:100'),
        ('DarSpecies', 'Specific epithet', 'string:100'),
        ('DarSubspecies', 'Infraspecific epithet', 'string:100'),
        ('DarHigherTaxon', 'Higher classification', 'string:100'),
        ('DarInfraspecificRank', 'Taxon rank', 'string:100'),

        # Location
        # The encoding of DarLocality is buggered - see ecatalogue.1804973
        # So better to use the original field with the correct encoding
        ('sumPreciseLocation', 'Locality', 'string:100'),
        ('DarStateProvince', 'State province', 'string:100'),
        ('DarCountry', 'Country', 'string:100'),
        ('DarContinent', 'Continent', 'string:100'),
        ('DarIsland', 'Island', 'string:100'),
        ('DarIslandGroup', 'Island group', 'string:100'),
        # Removed: continentOcean is not in current DwC standard, replaced by waterBody and continent
        # ('DarContinentOcean', 'continentOcean', 'string:100'),
        ('DarWaterBody', 'Water body', 'string:100'),
        ('DarHigherGeography', 'Higher geography', 'string:100'),
        ('ColHabitatVerbatim', 'Habitat', 'string:100'),
        ('DarDecimalLongitude', 'Decimal longitude', 'float32'),
        ('DarDecimalLatitude', 'Decimal latitude', 'float32'),
        ('DarGeodeticDatum', 'Geodetic datum', 'string:100'),
        ('DarGeorefMethod', 'Georeference protocol', 'string:100'),

        # Occurrence
        ('DarMinimumElevationInMeters', 'Minimum elevation in meters', 'string:100'),
        ('DarMaximumElevationInMeters', 'Maximum elevation in meters', 'string:100'),
        ('DarMinimumDepthInMeters', 'Minimum depth in meters', 'string:100'),
        ('DarMaximumDepthInMeters', 'Maximum depth in meters', 'string:100'),
        ('DarCatalogNumber', 'Catalog number', 'string:100'),
        ('DarOtherCatalogNumbers', 'Other catalog numbers', 'string:100'),
        ('DarCollector', 'Recorded by', 'string:100'),
        ('DarCollectorNumber', 'Record number', 'string:100'),
        ('DarIndividualCount', 'Individual count', 'string:100'),
        # According to docs, ageClass has been superseded by lifeStage. We have both, but ageClass duplicates
        # And for the ~200 it has extra data, the data isn't good
        # ('DarAgeClass', 'ageClass', 'string:100'),
        ('DarLifeStage', 'Life stage', 'string:100'),
        ('DarSex', 'Sex', 'string:100'),
        ('DarPreparations', 'Preparations', 'string:100'),

        # Identification
        ('DarIdentifiedBy', 'Identified by', 'string:100'),
        # KE Emu has 3 fields for identification date: DarDayIdentified, DarMonthIdentified and DarYearIdentified
        # But EntIdeDateIdentified holds them all - which is what we want for dateIdentified
        ('EntIdeDateIdentified', 'Date identified', 'string:100'),
        ('DarIdentificationQualifier', 'Identification qualifier', 'string:100'),
        ('DarTypeStatus', 'Type status', 'string:100'),
        ('DarFieldNumber', 'Field number', 'string:100'),
        ('DarTimeOfDay', 'Event time', 'string:100'),
        ('DarDayCollected', 'Day', 'string:100'),
        ('DarMonthCollected', 'Month', 'string:100'),
        ('DarYearCollected', 'Year', 'string:100'),

        # Geo
        ('DarEarliestEon', 'Earliest eon or lowest eonothem', 'string:100'),
        ('DarLatestEon', 'Latest eon or highest eonothem', 'string:100'),
        ('DarEarliestEra', 'Earliest era or lowest erathem', 'string:100'),
        ('DarLatestEra', 'Latest era or highest erathem', 'string:100'),
        ('DarEarliestPeriod', 'Earliest period or lowest system', 'string:100'),
        ('DarLatestPeriod', 'Latest period or highest system', 'string:100'),
        ('DarEarliestEpoch', 'Earliest epoch or lowest series', 'string:100'),
        ('DarLatestEpoch', 'Latest epoch or highest series', 'string:100'),
        ('DarEarliestAge', 'Earliest age or lowest stage', 'string:100'),
        ('DarLatestAge', 'Latest age or highest stage', 'string:100'),
        ('DarLowestBiostrat', 'Lowest biostratigraphic zone', 'string:100'),
        ('DarHighestBiostrat', 'Highest biostratigraphic zone', 'string:100'),
        ('DarGroup', 'Group', 'string:100'),
        ('DarFormation', 'Formation', 'string:100'),
        ('DarMember', 'Member', 'string:100'),
        ('DarBed', 'Bed', 'string:100'),

        # Resource relationship
        ('DarRelatedCatalogItem', 'Related resource id', 'string:100'),

        # Multimedia
        ('MulMultiMediaRef', 'Associated media', 'string:100'),

        # Dynamic properties
        # These fields do not map to DwC, but are still very useful
        ('ColRecordType', 'Record type', 'string:100'),
        ('ColSubDepartment', 'Sub department', 'string:100'),
        ('PrtType', 'Part type', 'string:100'),
        ('RegCode', 'Registration code', 'string:100'),
        ('CatKindOfObject', 'Kind of object', 'string:100'),
        ('CatKindOfCollection', 'Kind of collection', 'string:100'),
        ('CatPreservative', 'Preservative', 'string:100'),
        ('ColKind', 'Collection kind', 'string:100'),
        ('EntPriCollectionName', 'Collection name', 'string:100'),
        ('PartRefStr', 'Part refs', 'string:100'),
        ('PalAcqAccLotDonorFullName', 'Donor name', 'string:100'),
        ('DarPreparationType', 'Preparation type', 'string:100'),
        ('DarObservedWeight', 'Observed weight', 'string:100'),
        # DNA
        ('DnaExtractionMethod', 'Extraction method', 'string:100'),
        ('DnaReSuspendedIn', 'Resuspended in', 'string:100'),
        ('DnaTotalVolume', 'Total volume', 'string:100'),
        # Parasite card
        ('CardBarcode', 'Barcode', 'string:100'),
        # Egg
        ('EggClutchSize', 'Clutch size', 'string:100'),
        ('EggSetMark', 'Set mark', 'string:100'),
        # Nest
        ('NesShape', 'Nest shape', 'string:100'),
        ('NesSite', 'Nest site', 'string:100'),
        # Silica gel
        ('SilPopulationCode', 'Population code', 'string:100'),
        # Botany
        ('CollExsiccati', 'Exsiccati', 'string:100'),
        ('ColExsiccatiNumber', 'Exsiccati number', 'string:100'),
        ('ColSiteDescription', 'Site description', 'string:100'),
        ('ColPlantDescription', 'Plant description', 'string:100'),
        ('FeaCultivated', 'Cultivated', 'string:100'),
        ('FeaPlantForm', 'Plant form', 'string:100'),
        # Paleo
        ('PalDesDescription', 'Catalogue description', 'string:100'),
        ('PalStrChronostratLocal', 'Chronostratigraphy', 'string:100'),
        ('PalStrLithostratLocal', 'Lithostratigraphy', 'string:100'),
        # Mineralogy
        ('MinDateRegistered', 'Date registered', 'string:100'),
        ('MinIdentificationAsRegistered', 'Identification as registered', 'string:100'),
        ('MinIdentificationDescription', 'Identification description', 'string:100'),
        ('MinPetOccurance', 'Occurrence', 'string:100'),
        ('MinOreCommodity', 'Commodity', 'string:100'),
        ('MinOreDepositType', 'Deposit type', 'string:100'),
        ('MinTextureStructure', 'Texture', 'string:100'),
        ('MinIdentificationVariety', 'Identification variety', 'string:100'),
        ('MinIdentificationOther', 'Identification other', 'string:100'),
        ('MinHostRock', 'Host rock', 'string:100'),
        ('MinAgeDataAge', 'Age', 'string:100'),
        ('MinAgeDataType', 'Age type', 'string:100'),
        # Mineralogy location
        ('MinNhmTectonicProvinceLocal', 'Tectonic province', 'string:100'),
        ('MinNhmStandardMineLocal', 'Mine', 'string:100'),
        ('MinNhmMiningDistrictLocal', 'Mining district', 'string:100'),
        ('MinNhmComplexLocal', 'Mineral complex', 'string:100'),
        ('MinNhmRegionLocal', 'Geology region', 'string:100'),
        # Meteorite
        ('MinMetType', 'Meteorite type', 'string:100'),
        ('MinMetGroup', 'Meteorite group', 'string:100'),
        ('MinMetChondriteAchondrite', 'Chondrite achondrite', 'string:100'),
        ('MinMetClass', 'Meteorite class', 'string:100'),
        ('MinMetPetType', 'Pet type', 'string:100'),
        ('MinMetPetSubtype', 'Pet sub type', 'string:100'),
        ('MinMetRecoveryFindFall', 'Recovery', 'string:100'),
        ('MinMetRecoveryDate', 'Recovery date', 'string:100'),
        ('MinMetRecoveryWeight', 'Recovery weight', 'string:100'),
        ('MinMetWeightAsRegistered', 'Registered weight', 'string:100'),
        ('MinMetWeightAsRegisteredUnit', 'Registered weight unit', 'string:100'),

        # Internal
        ('RegRegistrationParentRef', '_parentRef', 'int32'),
        ('_id', '_id', 'int32'),

        # Removed: We do not want notes, could contain anything
        # ('DarNotes', 'DarNotes', 'string:100'),
        # ('DarLatLongComments', 'latLongComments', 'string:100'),
    ]

    query = {}  # Selecting data is moved to aggregation query

    def run(self):

        # Before running, build aggregation query
        self.build_aggregation_query()
        super(SpecimenDatasetTask, self).run()

    def build_aggregation_query(self):
        """
        Build aggregation query
        The specimen dataset is too complicated, and we need to use MongoDB's aggregation queries
        Monary cannot handle aggregation queries however, so we'll build the aggregator, and then use {} for the query

        @return: None
        """
        mongo = MongoClient()
        db_collection = mongo[self.mongo_db][self.collection_name]

        # Update collection name to use agg_
        # Monary will query against the collection name
        self.collection_name = 'agg_%s' % self.collection_name

        # Build list of columns to select
        projection = {col[0]: 1 for col in self.columns}

        # We cannot rely on some DwC fields, as they are missing / incomplete for some records
        # So we manually add them based on other fields

        # If $DarCatalogNumber does not exist, we'll try use $RegRegistrationNumber
        # BUGFIX 141016: Using RegRegistrationNumber rather than GeneralCatalogueNumber
        # GeneralCatalogueNumber is not populated for bird part records & DarCatalogNumber has MinBmNumber
        projection['DarCatalogNumber'] = {"$ifNull": ["$DarCatalogNumber", "$RegRegistrationNumber"]}
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
        # TBH, the aggregation query is so simple now dynamic properties are their own fields, this is probably unnecessary
        # But lets keep in until after we've got all fields in place

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

        # match['$match']['_id'] = {"$in": [480206, 433477]}

        aggregation_query.append(match)

        # TEMP: Limit
        aggregation_query.append({'$limit': 1000})

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
        df['Associated media'] = df['Associated media'].apply(lambda x: list(int(z.strip()) for z in x.split(';') if z.strip()))

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
        unique_multimedia_irns = list(set(itertools.chain(*[irn for irn in df['Associated media'].values])))

        # Get a list of multimedia irns with valid mimetypes
        valid_multimedia = get_valid_multimedia(unique_multimedia_irns)

        # And finally update the associatedMedia field, so formatting with the IRN with MULTIMEDIA_URL, if the IRN is in valid_multimedia
        df['Associated media'] = df['Associated media'].apply(lambda irns: '; '.join(MULTIMEDIA_URL % irn for irn in irns if irn in valid_multimedia))

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
            # Which allows us to use combine_first() to replace NaNs with value from parent df
            df = df.applymap(lambda x: np.NaN if isinstance(x, basestring) and x == '' else x)

            # There is a annoying bug that coerces string columns to integers in combine_first
            # Hack: ensure there's always a string value that cannot be coerced in every column
            # So will create a dummy row, that gets deleted after combine_first is called
            dummy_index = len(df) + 1
            parent_df.loc[dummy_index] = ['-' for _ in parent_df]
            df = df.combine_first(parent_df)
            df = df.drop([dummy_index])

        return df


class SpecimenDatasetToCSVTask(SpecimenDatasetTask, DatasetToCSVTask):
    pass


class SpecimenDatasetToCKANTask(SpecimenDatasetTask, DatasetToCKANTask):

    package = IndexLotDatasetToCKANTask.package

    # And now save to the datastore
    datastore = {
        'resource': {
            'name': 'Test data9',
            'description': 'Test data',
            'format': 'dwc'  # Darwin core
        },
        'primary_key': 'Occurrence ID'
    }

    geospatial_fields = {
        'latitude_field': 'Decimal latitude',
        'longitude_field': 'Decimal longitude'
    }