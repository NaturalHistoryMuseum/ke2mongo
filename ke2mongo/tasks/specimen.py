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
from ke2mongo import config
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
            'name': 'Specimens2',
            'description': 'Specimens',
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
        ('ecatalogue.DarCatalogNumber', 'Catalog number', 'string:100'),
        # Taxonomy
        ('ecatalogue.DarScientificName', 'Scientific name', 'string:100'),
        # Rather than using the two darwin core fields DarScientificNameAuthorYear and ScientificNameAuthor
        # It's easier to just use IdeFiledAsAuthors which has them both concatenated
        ('ecatalogue.IdeFiledAsAuthors', 'Scientific name authorship', 'string:100'),
        ('ecatalogue.DarTypeStatus', 'Type status', 'string:100'),
        # Use nearest name place rather than precise locality https://github.com/NaturalHistoryMuseum/ke2mongo/issues/29
        ('ecatalogue.PalNearestNamedPlaceLocal', 'Locality', 'string:100'),
        ('ecatalogue.DarCountry', 'Country', 'string:100'),
        ('ecatalogue.DarWaterBody', 'Water body', 'string:100'),
        ('ecatalogue.EntLocExpeditionNameLocal', 'Expedition', 'string:100'),
        ('ecollectionevents.ColParticipantLocal', 'Recorded by', 'string:100'),
        ('ecatalogue.ColDepartment', 'Collection code', 'string:100'),

        ('ecatalogue.DarKingdom', 'Kingdom', 'string:100'),
        ('ecatalogue.DarPhylum', 'Phylum', 'string:100'),
        ('ecatalogue.DarClass', 'Class', 'string:100'),
        ('ecatalogue.DarOrder', 'Order', 'string:100'),
        ('ecatalogue.DarFamily', 'Family', 'string:100'),
        ('ecatalogue.DarGenus', 'Genus', 'string:100'),
        ('ecatalogue.DarSubgenus', 'Subgenus', 'string:100'),
        ('ecatalogue.DarSpecies', 'Specific epithet', 'string:100'),
        ('ecatalogue.DarSubspecies', 'Infraspecific epithet', 'string:100'),
        ('ecatalogue.DarHigherTaxon', 'Higher classification', 'string:100'),
        ('ecatalogue.DarInfraspecificRank', 'Taxon rank', 'string:100'),

        # Location
        ('ecatalogue.DarStateProvince', 'State province', 'string:100'),
        ('ecatalogue.DarContinent', 'Continent', 'string:100'),
        ('ecatalogue.DarIsland', 'Island', 'string:100'),
        ('ecatalogue.DarIslandGroup', 'Island group', 'string:100'),
        # Removed: continentOcean is not in current DwC standard, replaced by waterBody and continent
        # ('ecatalogue.DarContinentOcean', 'continentOcean', 'string:100'),
        ('ecatalogue.DarHigherGeography', 'Higher geography', 'string:100'),
        ('ecatalogue.ColHabitatVerbatim', 'Habitat', 'string:100'),
        ('ecatalogue.DarDecimalLongitude', 'Decimal longitude', 'float64'),
        ('ecatalogue.DarDecimalLatitude', 'Decimal latitude', 'float64'),
        ('ecatalogue.DarGeodeticDatum', 'Geodetic datum', 'string:100'),
        ('ecatalogue.DarGeorefMethod', 'Georeference protocol', 'string:100'),

        ('esites.LatDeriveCentroid', 'Centroid', 'bool'),
        ('esites.GeorefMaxErrorDist', 'Max error', 'string:100'),
        ('esites.GeorefMaxErrorDistUnits', '_errorUnit', 'string:100'),
        ('esites.LatLongitude', 'Verbatim longitude', 'string:100'),
        ('esites.LatLatitude', 'Verbatim latitude', 'string:100'),

        # Occurrence
        ('ecatalogue.DarMinimumElevationInMeters', 'Minimum elevation in meters', 'string:100'),
        ('ecatalogue.DarMaximumElevationInMeters', 'Maximum elevation in meters', 'string:100'),
        ('ecatalogue.DarMinimumDepthInMeters', 'Minimum depth in meters', 'string:100'),
        ('ecatalogue.DarMaximumDepthInMeters', 'Maximum depth in meters', 'string:100'),
        ('ecatalogue.DarOtherCatalogNumbers', 'Other catalog numbers', 'string:100'),
        # DarCollector doesn't have multiple collectors NHMUK:ecatalogue:1751715 - Switched to using ecollectionevents.ColParticipantLocal
        # ('ecatalogue.DarCollector', 'Recorded by', 'string:100'),
        ('ecatalogue.DarCollectorNumber', 'Record number', 'string:100'),
        ('ecatalogue.DarIndividualCount', 'Individual count', 'string:100'),
        # According to docs, ageClass has been superseded by lifeStage. We have both, but ageClass duplicates
        # And for the ~200 it has extra data, the data isn't good
        # ('ecatalogue.DarAgeClass', 'ageClass', 'string:100'),
        ('ecatalogue.DarLifeStage', 'Life stage', 'string:100'),
        ('ecatalogue.DarSex', 'Sex', 'string:100'),
        ('ecatalogue.DarPreparations', 'Preparations', 'string:100'),

        # Identification
        ('ecatalogue.DarIdentifiedBy', 'Identified by', 'string:100'),
        # KE Emu has 3 fields for identification date: DarDayIdentified, DarMonthIdentified and DarYearIdentified
        # But EntIdeDateIdentified holds them all - which is what we want for dateIdentified
        ('ecatalogue.EntIdeDateIdentified', 'Date identified', 'string:100'),
        ('ecatalogue.DarIdentificationQualifier', 'Identification qualifier', 'string:100'),
        # ('ecatalogue.DarFieldNumber', 'Field number', 'string:100'),  Removed as mostly duplicates DarCollectorNumber (JW - feedback)
        ('ecatalogue.DarTimeOfDay', 'Event time', 'string:100'),
        ('ecatalogue.DarDayCollected', 'Day', 'string:100'),
        ('ecatalogue.DarMonthCollected', 'Month', 'string:100'),
        ('ecatalogue.DarYearCollected', 'Year', 'string:100'),

        # Geo
        ('ecatalogue.DarEarliestEon', 'Earliest eon or lowest eonothem', 'string:100'),
        ('ecatalogue.DarLatestEon', 'Latest eon or highest eonothem', 'string:100'),
        ('ecatalogue.DarEarliestEra', 'Earliest era or lowest erathem', 'string:100'),
        ('ecatalogue.DarLatestEra', 'Latest era or highest erathem', 'string:100'),
        ('ecatalogue.DarEarliestPeriod', 'Earliest period or lowest system', 'string:100'),
        ('ecatalogue.DarLatestPeriod', 'Latest period or highest system', 'string:100'),
        ('ecatalogue.DarEarliestEpoch', 'Earliest epoch or lowest series', 'string:100'),
        ('ecatalogue.DarLatestEpoch', 'Latest epoch or highest series', 'string:100'),
        ('ecatalogue.DarEarliestAge', 'Earliest age or lowest stage', 'string:100'),
        ('ecatalogue.DarLatestAge', 'Latest age or highest stage', 'string:100'),
        ('ecatalogue.DarLowestBiostrat', 'Lowest biostratigraphic zone', 'string:100'),
        ('ecatalogue.DarHighestBiostrat', 'Highest biostratigraphic zone', 'string:100'),
        ('ecatalogue.DarGroup', 'Group', 'string:100'),
        ('ecatalogue.DarFormation', 'Formation', 'string:100'),
        ('ecatalogue.DarMember', 'Member', 'string:100'),
        ('ecatalogue.DarBed', 'Bed', 'string:100'),

        # Resource relationship
        # ('ecatalogue.DarRelatedCatalogItem', 'Related resource id', 'string:100'), Only 34 records have this field populated
        # So it's better to build automatically from part / parent records

        # Multimedia
        ('ecatalogue.MulMultiMediaRef', 'Associated media', 'string:100'),

        # Dynamic properties
        # These fields do not map to DwC, but are still very useful
        ('ecatalogue.ColRecordType', 'Record type', 'string:100'),
        ('ecatalogue.ColSubDepartment', 'Sub department', 'string:100'),
        ('ecatalogue.PrtType', 'Part type', 'string:100'),
        ('ecatalogue.RegCode', 'Registration code', 'string:100'),
        ('ecatalogue.CatKindOfObject', 'Kind of object', 'string:100'),
        ('ecatalogue.CatKindOfCollection', 'Kind of collection', 'string:100'),
        ('ecatalogue.CatPreservative', 'Preservative', 'string:100'),
        ('ecatalogue.ColKind', 'Collection kind', 'string:100'),
        ('ecatalogue.EntPriCollectionName', 'Collection name', 'string:100'),
        ('ecatalogue.PalAcqAccLotDonorFullName', 'Donor name', 'string:100'),
        ('ecatalogue.DarPreparationType', 'Preparation type', 'string:100'),
        ('ecatalogue.DarObservedWeight', 'Observed weight', 'string:100'),

        # Location
        ('ecatalogue.sumViceCountry', 'Vice country', 'string:100'),

        # DNA
        ('ecatalogue.DnaExtractionMethod', 'Extraction method', 'string:100'),
        ('ecatalogue.DnaReSuspendedIn', 'Resuspended in', 'string:100'),
        ('ecatalogue.DnaTotalVolume', 'Total volume', 'string:100'),
        # Parasite card
        ('ecatalogue.CardBarcode', 'Barcode', 'string:100'),
        # Egg
        ('ecatalogue.EggClutchSize', 'Clutch size', 'string:100'),
        ('ecatalogue.EggSetMark', 'Set mark', 'string:100'),
        # Nest
        ('ecatalogue.NesShape', 'Nest shape', 'string:100'),
        ('ecatalogue.NesSite', 'Nest site', 'string:100'),
        # Silica gel
        ('ecatalogue.SilPopulationCode', 'Population code', 'string:100'),
        # Botany
        ('ecatalogue.CollExsiccati', 'Exsiccati', 'string:100'),
        ('ecatalogue.ColExsiccatiNumber', 'Exsiccati number', 'string:100'),
        ('ecatalogue.ColSiteDescription', 'Label locality', 'string:100'),  # JW asked for this to be renamed from Site Description => Label locality
        ('ecatalogue.ColPlantDescription', 'Plant description', 'string:100'),
        ('ecatalogue.FeaCultivated', 'Cultivated', 'string:100'),
        # ('ecatalogue.FeaPlantForm', 'Plant form', 'string:100'),  # JW asked for this to be removed
        # Paleo
        ('ecatalogue.PalDesDescription', 'Catalogue description', 'string:100'),
        ('ecatalogue.PalStrChronostratLocal', 'Chronostratigraphy', 'string:100'),
        ('ecatalogue.PalStrLithostratLocal', 'Lithostratigraphy', 'string:100'),
        # Mineralogy
        ('ecatalogue.MinDateRegistered', 'Date registered', 'string:100'),
        ('ecatalogue.MinIdentificationAsRegistered', 'Identification as registered', 'string:100'),
        ('ecatalogue.MinIdentificationDescription', 'Identification description', 'string:100'),
        ('ecatalogue.MinPetOccurance', 'Occurrence', 'string:100'),
        ('ecatalogue.MinOreCommodity', 'Commodity', 'string:200'),
        ('ecatalogue.MinOreDepositType', 'Deposit type', 'string:100'),
        ('ecatalogue.MinTextureStructure', 'Texture', 'string:100'),
        ('ecatalogue.MinIdentificationVariety', 'Identification variety', 'string:100'),
        ('ecatalogue.MinIdentificationOther', 'Identification other', 'string:100'),
        ('ecatalogue.MinHostRock', 'Host rock', 'string:100'),
        ('ecatalogue.MinAgeDataAge', 'Age', 'string:100'),
        ('ecatalogue.MinAgeDataType', 'Age type', 'string:100'),
        # Mineralogy location
        ('ecatalogue.MinNhmTectonicProvinceLocal', 'Tectonic province', 'string:100'),
        ('ecatalogue.MinNhmStandardMineLocal', 'Mine', 'string:100'),
        ('ecatalogue.MinNhmMiningDistrictLocal', 'Mining district', 'string:100'),
        ('ecatalogue.MinNhmComplexLocal', 'Mineral complex', 'string:100'),
        ('ecatalogue.MinNhmRegionLocal', 'Geology region', 'string:100'),
        # Meteorite
        ('ecatalogue.MinMetType', 'Meteorite type', 'string:100'),
        ('ecatalogue.MinMetGroup', 'Meteorite group', 'string:100'),
        ('ecatalogue.MinMetChondriteAchondrite', 'Chondrite achondrite', 'string:100'),
        ('ecatalogue.MinMetClass', 'Meteorite class', 'string:100'),
        ('ecatalogue.MinMetPetType', 'Petrology type', 'string:100'),
        ('ecatalogue.MinMetPetSubtype', 'Petrology subtype', 'string:100'),
        ('ecatalogue.MinMetRecoveryFindFall', 'Recovery', 'string:100'),
        ('ecatalogue.MinMetRecoveryDate', 'Recovery date', 'string:100'),
        ('ecatalogue.MinMetRecoveryWeight', 'Recovery weight', 'string:100'),
        ('ecatalogue.MinMetWeightAsRegistered', 'Registered weight', 'string:100'),
        ('ecatalogue.MinMetWeightAsRegisteredUnit', 'Registered weight unit', 'string:100'),

        # Identifier
        ('ecatalogue.irn', 'Occurrence ID', 'string:100'),
        # Record level
        ('ecatalogue.AdmDateModified', 'Modified', 'string:100'),
        # This isn't actually in DwC - but I'm going to use dcterms:created
        ('ecatalogue.AdmDateInserted', 'Created', 'string:100'),

        # Internal
        ('ecatalogue.RegRegistrationParentRef', '_parentRef', 'int32'),
        ('ecatalogue.sumSiteRef', '_siteRef', 'int32'),
        ('ecatalogue.sumCollectionEventRef', '_collectionEventRef', 'int32'),
        ('ecatalogue.CardParasiteRef', '_cardParasiteRef', 'int32'),
        # Used if DarCatalogueNumber is empty
        ('ecatalogue.RegRegistrationNumber', '_regRegistrationNumber', 'string:100'),

        # Used if CatPreservative is empty
        ('ecatalogue.EntCatPreservation', '_entCatPreservation', 'string:100'),

        # Used to build previous determinations for Botany
        ('ecatalogue.DetTypeofType', '_determinationTypes', 'string:100'),
        ('ecatalogue.EntIdeScientificNameLocal', '_determinationNames', 'string:100'),
        ('ecatalogue.EntIdeFiledAs', '_determinationFiledAs', 'string:100'),
        # If DarTypeStatus is empty, we'll use sumTypeStatus which has previous determinations
        ('ecatalogue.sumTypeStatus', '_sumTypeStatus', 'string:100'),

        # Locality if nearest named place is empty
        # The encoding of DarLocality is buggered - see ecatalogue.1804973
        # So better to use the original field with the correct encoding
        ('ecatalogue.sumPreciseLocation', '_preciseLocation', 'string:100'),

        # CITES specimens
        ('ecatalogue.cites', '_cites', 'bool'),

        # Parasite cards use a different field for life stage
        ('ecatalogue.CardParasiteStage', '_parasite_stage', 'string:100'),

        # Join keys
        ('ecollectionevents._id', '_ecollectionevents_irn', 'int32'),
        ('esites._id', '_esites_irn', 'int32'),

        # Removed: We do not want notes, could contain anything
        # ('ecatalogue.DarNotes', 'DarNotes', 'string:100'),
        # ('ecatalogue.DarLatLongComments', 'latLongComments', 'string:100'),

    ]

    # Used to merge in data from parasite cards, which do not have taxonomic data
    parasite_taxonomy_fields = [
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

    # Columns not selected from the database
    # In the format (field_name, field_type, default_value)
    literal_columns = [
        ('Institution code', 'string:100', 'NHMUK'),
        ('Basis of record', 'string:100', 'Specimen'),
        ('Determinations', 'string:100', np.NaN),
        # This is set dynamically if this is a part record (with parent Ref)
        ('Related resource ID', 'string:100', np.NaN),
        ('Relationship of resource', 'string:100', np.NaN)
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
        query['_id'] = {'$in': [1]}

        return query

    def get_output_columns(self):
        """
        Override default get_output_columns and add in literal columns (not retrieved from mongo)
        @return:
        """
        output_columns = super(SpecimenDatasetTask, self).get_output_columns()

        # Add the literal columns
        for (field_name, field_type, _) in self.literal_columns:
            output_columns[field_name] = field_type

        return output_columns

    def process_dataframe(self, m, df):
        """
        Process the dataframe, updating multimedia irns => URIs
        @param m: monary
        @param df: dataframe
        @return: dataframe
        """
        df = super(SpecimenDatasetTask, self).process_dataframe(m, df)

        # Added literal columns
        for (field_name, _, default_value) in self.literal_columns:
            df[field_name] = default_value

        # Convert collection code to PAL, MIN etc.,
        df['Collection code'] = df['Collection code'].str.upper().str[0:3]
        # Entom record collection code = BMNH(E)
        df['Collection code'][df['Collection code'] == 'ENT'] = "BMNH(E)"

        # Ensure multimedia resources are suitable (jpeg rather than tiff etc.,)
        self.ensure_multimedia(df, 'Associated media')

        # Assign determination name, type and field as to Determinations to show determination history
        determinations = [
            ('Name', '_determinationNames'),
            ('Type', '_determinationTypes'),
            ('Filed as', '_determinationFiledAs')
        ]

        # Loop through all the determination fields, adding the field name
        for field_name, determination in determinations:
            df[determination][df[determination] != ''] = field_name + '=' + df[determination]

        df['Determinations'] = df['_determinationNames'].str.cat(df['_determinationTypes'].values.astype(str), sep='|').str.cat(df['_determinationFiledAs'].values.astype(str), sep='|')

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
        parent_irns = self._get_unique_irns(df, '_parentRef')

        if parent_irns:
            # We want to get all parts associated to one parent record, so we can provide them as associated records
            # So select all records matching the parent IRN

            # Use the same query, so we filter out any unwanted records
            # But use a copy just in case, as we'll be changing it
            q = dict(self.query)

            # Delete _id if it's set - need this for testing
            if '_id' in q:
                del q['_id']

            # Get all records with the same parent
            q['RegRegistrationParentRef'] = {'$in': parent_irns}

            monary_query = m.query(config.get('mongo', 'database'), 'ecatalogue', q, ['RegRegistrationParentRef', '_id'], ['int32'] * 2)
            part_df = pd.DataFrame(np.matrix(monary_query).transpose(), columns=['RegRegistrationParentRef', '_id'])

            #  Add primary key prefix
            part_df['_id'] = self.primary_key_prefix + part_df['_id'].astype(np.str)

            # Group by parent ref
            parts = part_df.groupby('RegRegistrationParentRef')['_id'].apply(lambda x: "%s" % ';'.join(x))

            # And update the main date frame with the group parts, merged on _parentRef
            df['Related resource ID'] = df.apply(lambda row: parts[row['_parentRef']] if row['_parentRef'] in parts else np.NaN, axis=1)

            df['Relationship of resource'][df['Related resource ID'].notnull()] = 'Parts'

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

        # Ensure our geo fields are floats
        df['Decimal longitude'] = df['Decimal longitude'].astype('float64')
        df['Decimal latitude'] = df['Decimal latitude'].astype('float64')

        # Get all collection columns
        collection_columns = self.get_collection_columns()

        # Load extra sites info (if this a centroid and error radius + unit)
        site_irns = self._get_unique_irns(df, '_siteRef')

        sites_df = self.get_dataframe(m, 'esites', collection_columns['esites'], site_irns, '_esites_irn')
        # Append the error unit to the max error value
        # Error unit can be populated even when Max error is not, so need to check max error first
        sites_df['Max error'][sites_df['Max error'] != ''] = sites_df['Max error'].astype(str) + ' ' + sites_df['_errorUnit'].astype(str)

        df = pd.merge(df, sites_df, how='outer', left_on=['_siteRef'], right_on=['_esites_irn'])

        # For CITES species, we need to hide Lat/Lon and Locality data - and label images
        for i in ['Locality', 'Label locality', 'Decimal longitude', 'Decimal latitude', 'Verbatim longitude', 'Verbatim latitude', 'Centroid', 'Max error', 'Higher geography', 'Associated media']:
            df[i][df['_cites'] == 'True'] = np.NaN

        # Some records are being assigned a Centroid even if they have no lat/lon fields.
        # Ensure it's NaN is latitude is null
        df['Centroid'][df['Decimal latitude'].isnull()] = np.NaN

        # Load collection event data
        collection_event_irns = self._get_unique_irns(df, '_collectionEventRef')

        # if collection_event_irns:
        collection_event_df = self.get_dataframe(m, 'ecollectionevents', collection_columns['ecollectionevents'], collection_event_irns, '_ecollectionevents_irn')
        # print collection_event_df
        df = pd.merge(df, collection_event_df, how='outer', left_on=['_collectionEventRef'], right_on=['_ecollectionevents_irn'])

        # Add parasite life stage
        # Parasite cards use a different field for life stage
        df['Life stage'].fillna(df['_parasite_stage'], inplace=True)

        # Add parasite card
        parasite_taxonomy_irns = self._get_unique_irns(df, '_cardParasiteRef')

        if parasite_taxonomy_irns:
            parasite_df = self.get_dataframe(m, 'etaxonomy', self.parasite_taxonomy_fields, parasite_taxonomy_irns, '_irn')
            df.index = df['_cardParasiteRef']
            df = df.combine_first(parasite_df)

        return df

class SpecimenDatasetCSVTask(SpecimenDatasetTask, DatasetCSVTask):
    pass


class SpecimenDatasetAPITask(SpecimenDatasetTask, DatasetAPITask):
    pass


if __name__ == "__main__":
    luigi.run()