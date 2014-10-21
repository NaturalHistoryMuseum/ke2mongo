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
from collections import OrderedDict
from ke2mongo.tasks import PARENT_TYPES, COLLECTION_DATASET
from ke2mongo.tasks.dataset import DatasetTask, DatasetCSVTask, DatasetAPITask
from ke2mongo.tasks.artefact import ArtefactDatasetTask
from ke2mongo.tasks.indexlot import IndexLotDatasetTask

# TODO: Add new fields
# TODO: UPDATES!!!

class SpecimenDatasetTask(DatasetTask):

    # CKAN Dataset params
    package = COLLECTION_DATASET

    # And now save to the datastore
    datastore = {
        'resource': {
            'name': 'Test data19',
            'description': 'Test data',
            'format': 'dwc'  # Darwin core
        },
        'primary_key': 'Occurrence ID'
    }

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
        ('EntLocExpeditionNameLocal', 'Expedition', 'string:100'),

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
        ('ColSiteRef', '_sites_irn', 'int32'),
        ('_id', '_id', 'int32'),
        # Used if DarCatalogueNumber is empty
        ('RegRegistrationNumber', '_regRegistrationNumber', 'string:100'),
        # Used to build previous determinations for Botany
        ('DetTypeofType', '_determinationTypes', 'string:100'),
        ('EntIdeScientificNameLocal', '_determinationNames', 'string:100'),
        ('EntIdeFiledAs', '_determinationFiledAs', 'string:100'),
        # If DarTypeStatus is empty, we'll use sumTypeStatus which has previous determinations
        ('sumTypeStatus', '_sumTypeStatus', 'string:100'),
        # CITES specimens
        ('cites', '_cites', 'bool'),

        # Removed: We do not want notes, could contain anything
        # ('DarNotes', 'DarNotes', 'string:100'),
        # ('DarLatLongComments', 'latLongComments', 'string:100'),
    ]

    # Additional columns to merge in from the taxonomy collection
    sites_columns = [
        ('_id', '_sites_irn', 'int32'),
        ('LatDeriveCentroid', 'Centroid', 'bool'),
        ('GeorefMaxErrorDist', 'Max error', 'int32'),
        ('GeorefMaxErrorDistUnits', '_errorUnit', 'string:100'),

    ]

    # query = {
    #     "ColRecordType": {
    #         "$nin": PARENT_TYPES + [ArtefactDatasetTask.record_type, IndexLotDatasetTask.record_type]
    #     },
    #     # We only want Botany records if they have a catalogue number starting with BM
    #     # And only for Entom, Min, Pal & Zoo depts.
    #     "$or":
    #         [
    #             {"ColDepartment": 'Botany', "DarCatalogNumber": re.compile("^BM")},
    #             {"ColDepartment":
    #                 {
    #                     "$in": ["Entomology", "Mineralogy", "Palaeontology", "Zoology"]
    #                 }
    #             }
    #         ]
    # }

    query = {
        'EntLocExpeditionNameLocal': {'$exists': True}
    }

    def get_output_columns(self):
        """
        Override default get_output_columns and add in literal columns (not retrieved from mongo)
        @return:
        """
        output_columns = OrderedDict((col[1], col[2]) for col in self.columns + self.sites_columns if self._is_output_field(col[1]))

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
        df['Occurrence ID'] = 'NHMUK:ecatalogue:' + df['Occurrence ID']
        # Added literal columns
        df['Institution code'] = 'NHMUK'
        df['Basis of record'] = 'Specimen'

        # Convert collection code to PAL, MIN etc.,
        df['Collection code'] = df['Collection code'].str.upper().str[0:3]
        # Entom record collection code = BMNH(E)
        df['Collection code'][df['Collection code'] == 'ENT'] = "BMNH(E)"

        # Ensure multimedia resources are suitable (jpeg rather than tiff etc.,)
        self.ensure_multimedia(m, df, 'Associated media')

        # For CITES species, we need to hide Lat/Lon and Locality data
        for i in ['Locality', 'Label locality', 'Decimal longitude', 'Decimal latitude', 'Higher geography']:
            df[i][df['_cites'] == 'True'] = np.nan

        # Assign determination name, type and field as to Determinations to show determination history
        df['Determinations'] = 'name=' + df['_determinationNames'].astype(str) + ';type=' + df['_determinationTypes'].astype(str) + ';filedAs=' + df['_determinationFiledAs'].astype(str)

        # Convert all blank strings to NaN so we can use fillna & combine_first() to replace NaNs with value from parent df
        df = df.applymap(lambda x: np.nan if isinstance(x, basestring) and x == '' else x)

        df['Catalog number'].fillna(df['_regRegistrationNumber'], inplace=True)

        # Replace missing DarTypeStatus
        df['Type status'].fillna(df['_sumTypeStatus'], inplace=True)

        # Cultivated should only be set on Botany records - but is actually on everything
        df['Cultivated'][df['Collection code'] != 'BOT'] = np.nan

        # Process part parents
        parent_irns = pd.unique(df._parentRef.values.ravel()).tolist()

        if parent_irns:

            parent_df = self.get_dataframe(m, 'ecatalogue', self.columns, parent_irns, '_id')

            # Ensure the parent multimedia images are usable
            self.ensure_multimedia(m, parent_df, 'Associated media')

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
        site_irns = pd.unique(df._sites_irn.values.ravel()).tolist()
        sites_df = self.get_dataframe(m, 'esites', self.sites_columns, site_irns, '_sites_irn')

        # Append the error unit to the max error value
        sites_df['Max error'] = sites_df['Max error'].astype(str) + ' ' + sites_df['_errorUnit'].astype(str)
        df = pd.merge(df, sites_df, how='outer', left_on=['_sites_irn'], right_on=['_sites_irn'])

        return df


class SpecimenDatasetCSVTask(SpecimenDatasetTask, DatasetCSVTask):
    pass


class SpecimenDatasetAPITask(SpecimenDatasetTask, DatasetAPITask):
    pass
