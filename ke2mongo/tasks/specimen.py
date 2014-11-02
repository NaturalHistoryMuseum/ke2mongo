#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.

python specimen.py SpecimenDatasetToCKANTask --local-scheduler --date 20140731
python specimen.py SpecimenDatasetToCSVTask --local-scheduler --date 20140821

"""
import re
import pandas as pd
import numpy as np
import luigi
import itertools
from collections import OrderedDict
from ke2mongo.tasks import PARENT_TYPES, COLLECTION_DATASET
from ke2mongo.tasks.dataset import DatasetTask, DatasetCSVTask, DatasetAPITask
from ke2mongo.tasks.artefact import ArtefactDatasetTask
from ke2mongo.tasks.indexlot import IndexLotDatasetTask

class SpecimenDatasetTask(DatasetTask):

    # CKAN Dataset params
    package = COLLECTION_DATASET

    # And now save to the datastore
    datastore = {
        'resource': {
            'name': 'Specimens13',
            'description': 'Specimens',
            'format': 'dwc'  # Darwin core
        },
        'primary_key': 'Occurrence ID'
    }

    primary_key_prefix = 'NHMUK:ecatalogue:'

    geospatial_fields = {
        'latitude_field': 'Decimal latitude',
        'longitude_field': 'Decimal longitude'
    }

    columns = [
        # List of columns
        # ([KE EMu field], [new field], [field type])

        # Identifier
        ('irn', 'Occurrence ID', 'string:100'),

        # Record level
        ('AdmDateModified', 'Modified', 'string:100'),
        # This isn't actually in DwC - but I'm going to use dcterms:created
        ('AdmDateInserted', 'Created', 'string:100'),
        ('ColDepartment', 'Collection code', 'string:100'),

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
        # Use nearest name place rather than precise locality https://github.com/NaturalHistoryMuseum/ke2mongo/issues/29
        ('PalNearestNamedPlaceLocal', 'Locality', 'string:100'),
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
        ('DarDecimalLongitude', 'Decimal longitude', 'float64'),
        ('DarDecimalLatitude', 'Decimal latitude', 'float64'),
        ('DarGeodeticDatum', 'Geodetic datum', 'string:100'),
        ('DarGeorefMethod', 'Georeference protocol', 'string:100'),

        # Occurrence
        ('DarMinimumElevationInMeters', 'Minimum elevation in meters', 'string:100'),
        ('DarMaximumElevationInMeters', 'Maximum elevation in meters', 'string:100'),
        ('DarMinimumDepthInMeters', 'Minimum depth in meters', 'string:100'),
        ('DarMaximumDepthInMeters', 'Maximum depth in meters', 'string:100'),
        ('DarCatalogNumber', 'Catalog number', 'string:100'),
        ('DarOtherCatalogNumbers', 'Other catalog numbers', 'string:100'),
        # DarCollector doesn't have multiple collectors NHMUK:ecatalogue:1751715 - Switched to using ecollectionevents.ColParticipantLocal
        # ('DarCollector', 'Recorded by', 'string:100'),
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
        # ('DarFieldNumber', 'Field number', 'string:100'),  Removed as mostly duplicates DarCollectorNumber (JW - feedback)
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
        ('PalAcqAccLotDonorFullName', 'Donor name', 'string:100'),
        ('DarPreparationType', 'Preparation type', 'string:100'),
        ('DarObservedWeight', 'Observed weight', 'string:100'),

        # Location
        ('EntLocExpeditionNameLocal', 'Expedition', 'string:100'),
        ('sumViceCountry', 'Vice country', 'string:100'),

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
        ('ColSiteDescription', 'Label locality', 'string:100'),  # JW asked for this to be renamed from Site Description => Label locality
        ('ColPlantDescription', 'Plant description', 'string:100'),
        ('FeaCultivated', 'Cultivated', 'string:100'),
        # ('FeaPlantForm', 'Plant form', 'string:100'),  # JW asked for this to be removed
        # Paleo
        ('PalDesDescription', 'Catalogue description', 'string:100'),
        ('PalStrChronostratLocal', 'Chronostratigraphy', 'string:100'),
        ('PalStrLithostratLocal', 'Lithostratigraphy', 'string:100'),
        # Mineralogy
        ('MinDateRegistered', 'Date registered', 'string:100'),
        ('MinIdentificationAsRegistered', 'Identification as registered', 'string:100'),
        ('MinIdentificationDescription', 'Identification description', 'string:100'),
        ('MinPetOccurance', 'Occurrence', 'string:100'),
        ('MinOreCommodity', 'Commodity', 'string:200'),
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
        ('sumSiteRef', '_siteRef', 'int32'),
        ('sumCollectionEventRef', '_collectionEventRef', 'int32'),
        ('CardParasiteRef', '_cardParasiteRef', 'int32'),
        ('_id', '_id', 'int32'),
        # Used if DarCatalogueNumber is empty
        ('RegRegistrationNumber', '_regRegistrationNumber', 'string:100'),

        # Used if CatPreservative is empty
        ('EntCatPreservation', '_entCatPreservation', 'string:100'),

        # Used to build previous determinations for Botany
        ('DetTypeofType', '_determinationTypes', 'string:100'),
        ('EntIdeScientificNameLocal', '_determinationNames', 'string:100'),
        ('EntIdeFiledAs', '_determinationFiledAs', 'string:100'),
        # If DarTypeStatus is empty, we'll use sumTypeStatus which has previous determinations
        ('sumTypeStatus', '_sumTypeStatus', 'string:100'),

        # Locality if nearest named place is empty
        # The encoding of DarLocality is buggered - see ecatalogue.1804973
        # So better to use the original field with the correct encoding
        ('sumPreciseLocation', '_preciseLocation', 'string:100'),

        # CITES specimens
        ('cites', '_cites', 'bool'),

        # Parasite cards use a different field for life stage
        ('CardParasiteStage', '_parasite_stage', 'string:100'),

        # Removed: We do not want notes, could contain anything
        # ('DarNotes', 'DarNotes', 'string:100'),
        # ('DarLatLongComments', 'latLongComments', 'string:100'),
    ]

    # Additional columns to merge in from the sites collection
    sites_columns = [
        ('_id', '_irn', 'int32'),
        ('LatDeriveCentroid', 'Centroid', 'bool'),
        ('GeorefMaxErrorDist', 'Max error', 'string:100'),
        ('GeorefMaxErrorDistUnits', '_errorUnit', 'string:100'),
        ('LatLongitude', 'Verbatim longitude', 'string:100'),
        ('LatLatitude', 'Verbatim latitude', 'string:100'),
    ]

    # Additional columns to merge in from the taxonomy collection
    collection_event_columns = [
        ('_id', '_irn', 'int32'),
        ('ColParticipantLocal', 'Recorded by', 'string:100'),
    ]

    # Used to merge in data from parasite cards, which do not have taxonomic data
    taxonomy_columns = [
        ('_id', '_irn', 'int32'),
        ('ClaScientificNameBuilt', 'Scientific name', 'string:100'),
        ('ClaKingdom', 'Kingdom', 'string:60'),
        ('ClaPhylum', 'Phylum', 'string:100'),
        ('ClaClass', 'Class', 'string:100'),
        ('ClaOrder', 'Order', 'string:100'),
        ('ClaFamily', 'Family', 'string:100'),
        ('ClaGenus', 'Genus', 'string:100'),
        ('ClaSubgenus', 'Subgenus', 'string:100'),
        ('ClaSpecies', 'Specific epithet', 'string:100'),
        ('ClaSubspecies', 'Infraspecific epithet', 'string:100'),
        ('ClaRank', 'Taxon rank', 'string:10'),  # NB: CKAN uses rank internally
    ]

    @property
    def query(self):
        """
        Query object for selecting data from mongoDB

        To test encoding, use query = {'_id': 42866}

        @return: dict
        """

        query = super(SpecimenDatasetTask, self).query

        # Override the default ColRecordType
        query['ColRecordType'] = {
            "$nin": PARENT_TYPES + [ArtefactDatasetTask.record_type, IndexLotDatasetTask.record_type]
        }

        # We only want Botany records if they have a catalogue number starting with BM
        # And only for Entom, Min, Pal & Zoo departments.
        query['$or'] = [
                {"ColDepartment": 'Botany', "DarCatalogNumber": re.compile("^BM")},
                {"ColDepartment":
                    {
                        "$in": ["Entomology", "Mineralogy", "Palaeontology", "Zoology"]
                    }
                }
            ]

        # To test: Order by ID, and put into batches of 2 with site / without
        # 1229
        # query['_id'] = {'$in' : [4235726, 619589, 619588, 1229, 619587, 619586, 619590]}

        return query

    def get_output_columns(self):
        """
        Override default get_output_columns and add in literal columns (not retrieved from mongo)
        @return:
        """
        output_columns = OrderedDict((col[1], col[2]) for col in itertools.chain(self.columns, self.sites_columns, self.collection_event_columns) if self._is_output_field(col[1]))

        # Add the literal columns
        output_columns['Institution code'] = 'string:100'
        output_columns['Basis of record'] = 'string:100'
        output_columns['Determinations'] = 'string:200'

        return output_columns

    def process_dataframe(self, m, df):
        """
        Process the dataframe, updating multimedia irns => URIs
        @param m: monary
        @param df: dataframe
        @return: dataframe
        """
        df = super(SpecimenDatasetTask, self).process_dataframe(m, df)

        # Is getting the same value as before????? If missing????
        # print df['Occurrence ID']
        # print df['_siteRef']
        # print df['_cites']

        # Added literal columns
        df['Institution code'] = 'NHMUK'
        df['Basis of record'] = 'Specimen'

        # Convert collection code to PAL, MIN etc.,
        df['Collection code'] = df['Collection code'].str.upper().str[0:3]
        # Entom record collection code = BMNH(E)
        df['Collection code'][df['Collection code'] == 'ENT'] = "BMNH(E)"

        # Ensure multimedia resources are suitable (jpeg rather than tiff etc.,)
        self.ensure_multimedia(df, 'Associated media')

        # Assign determination name, type and field as to Determinations to show determination history
        determinations = [
            ('name', '_determinationNames'),
            ('type', '_determinationTypes'),
            ('filedAs', '_determinationFiledAs')
        ]

        # Loop through all the determination fields, adding the field name
        for field_name, determination in determinations:
            df[determination][df[determination] != ''] = field_name + '=' + df[determination]

        df['Determinations'] = df['_determinationNames'].str.cat(df['_determinationTypes'].values.astype(str), sep='\n').str.cat(df['_determinationFiledAs'].values.astype(str), sep='\n')

        # Convert all blank strings to NaN so we can use fillna & combine_first() to replace NaNs with value from parent df
        df = df.applymap(lambda x: np.nan if isinstance(x, basestring) and x == '' else x)

        df['Catalog number'].fillna(df['_regRegistrationNumber'], inplace=True)

        # If PalNearestNamedPlaceLocal is missing, use sumPreciseLocation
        df['Locality'].fillna(df['_preciseLocation'], inplace=True)

        # Replace missing DarTypeStatus
        df['Type status'].fillna(df['_sumTypeStatus'], inplace=True)

        # Replace missing CatPreservative
        df['Preservative'].fillna(df['_entCatPreservation'], inplace=True)

        # Cultivated should only be set on Botany records - but is actually on everything
        df['Cultivated'][df['Collection code'] != 'BOT'] = np.nan

        # Process part parents
        parent_irns = self._get_irns(df, '_parentRef')

        if parent_irns:

            parent_df = self.get_dataframe(m, 'ecatalogue', self.columns, parent_irns, '_id')

            # Ensure the parent multimedia images are usable
            self.ensure_multimedia(parent_df, 'Associated media')

            # Assign parentRef as the index to allow us to combine with parent_df
            df.index = df['_parentRef']

            # There is a annoying bug that coerces string columns to integers in combine_first
            # Hack: ensure there's always a string value that cannot be coerced in every column
            # So will create a dummy row, that gets deleted after combine_first is called
            dummy_index = len(df) + 1
            parent_df.loc[dummy_index] = ['-' for _ in parent_df]
            df = df.combine_first(parent_df)
            df = df.drop([dummy_index])

        # Load extra sites info (if this a centroid and error radius + unit)
        site_irns = self._get_irns(df, '_siteRef')

        sites_df = self.get_dataframe(m, 'esites', self.sites_columns, site_irns, '_irn')
        # Append the error unit to the max error value
        sites_df['Max error'] = sites_df['Max error'].astype(str) + ' ' + sites_df['_errorUnit'].astype(str)
        df = pd.merge(df, sites_df, how='outer', left_on=['_siteRef'], right_on=['_irn'])

        # For CITES species, we need to hide Lat/Lon and Locality data - and label images
        for i in ['Locality', 'Label locality', 'Decimal longitude', 'Decimal latitude', 'Verbatim longitude', 'Verbatim latitude', 'Centroid', 'Max error', 'Higher geography', 'Associated media']:
            df[i][df['_cites'] == 'True'] = np.nan

        # Load collection event data
        collection_event_irns = self._get_irns(df, '_collectionEventRef')

        # if collection_event_irns:
        collection_event_df = self.get_dataframe(m, 'ecollectionevents', self.collection_event_columns, collection_event_irns, '_irn')
        df = pd.merge(df, collection_event_df, how='outer', left_on=['_collectionEventRef'], right_on=['_irn'])

        # Add parasite life stage
        df['Life stage'].fillna(df['_parasite_stage'], inplace=True)

        # Add parasite card
        parasite_taxonomy_irns = self._get_irns(df, '_cardParasiteRef')

        # if parasite_taxonomy_irns:
        parasite_df = self.get_dataframe(m, 'etaxonomy', self.taxonomy_columns, parasite_taxonomy_irns, '_irn')
        df.index = df['_cardParasiteRef']
        df = df.combine_first(parasite_df)

        return df


class SpecimenDatasetCSVTask(SpecimenDatasetTask, DatasetCSVTask):
    pass


class SpecimenDatasetAPITask(SpecimenDatasetTask, DatasetAPITask):
    pass


if __name__ == "__main__":
    luigi.run()