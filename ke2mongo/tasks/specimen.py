#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.

python tasks/specimen.py SpecimenDatasetAPITask --local-scheduler
python tasks/specimen.py SpecimenDatasetCSVTask --local-scheduler --date 20140821

NOTE: This started failing on my dev box, I think because indexes got corrupted
Running rebuildIndexes() has fixed the problem


"""
import re
import pandas as pd
import numpy as np
import luigi
import json
from ke2mongo import config
from ke2mongo.tasks import PARENT_TYPES, DATASET_LICENCE, DATASET_AUTHOR, DATASET_TYPE
from ke2mongo.tasks.dataset import DatasetTask, DatasetCSVTask, DatasetAPITask
from ke2mongo.tasks.artefact import ArtefactDatasetTask
from ke2mongo.tasks.indexlot import IndexLotDatasetTask

DQI_UNKNOWN = 'Unknown'
DQI_NOT_APPLICABLE = 'N/A'

class SpecimenDatasetTask(DatasetTask):

    # CKAN Dataset params
    package = {
        'name': 'collection-specimens-150618',
        'notes': u'Specimen records from the Natural History Museum\'s collection',
        'title': "Collection specimens",
        'author': DATASET_AUTHOR,
        'license_id': DATASET_LICENCE,
        'resources': [],
        'dataset_category': DATASET_TYPE,
        'spatial': '{"type":"Polygon","coordinates":[[[-180,82],[180,82],[180,-82],[-180,-82],[-180,82]]]}',
        'owner_org': config.get('ckan', 'owner_org')
    }

    # And now save to the datastore
    datastore = {
        'resource': {
            'name': 'Specimens',
            'description': 'Specimen records',
            'format': 'dwc'  # Darwin core
        },
        'primary_key': 'occurrenceID'
    }

    geospatial_fields = {
        'latitude_field': 'decimalLatitude',
        'longitude_field': 'decimalLongitude'
    }

    columns = [
        # List of columns
        # ([KE EMu field], [new field], [field type])
        ('ecatalogue._id', '_id', 'int32'),  # Used for logging, joins and the old stable identifier
        ('ecatalogue.AdmGUIDPreferredValue', 'occurrenceID', 'uuid'),
        ('ecatalogue.DarCatalogNumber', 'catalogNumber', 'string:100'),
        # Taxonomy
        ('ecatalogue.DarScientificName', 'scientificName', 'string:100'),
        # Rather than using the two darwin core fields DarScientificNameAuthorYear and ScientificNameAuthor
        # It's easier to just use IdeFiledAsAuthors which has them both concatenated
        ('ecatalogue.IdeFiledAsAuthors', 'scientificNameAuthorship', 'string:100'),
        ('ecatalogue.DarTypeStatus', 'typeStatus', 'string:100'),
        # Use nearest name place rather than precise locality https://github.com/NaturalHistoryMuseum/ke2mongo/issues/29
        ('ecatalogue.PalNearestNamedPlaceLocal', 'locality', 'string:100'),
        ('ecatalogue.DarCountry', 'country', 'string:100'),
        ('ecatalogue.DarWaterBody', 'waterBody', 'string:100'),
        ('ecatalogue.EntLocExpeditionNameLocal', 'expedition', 'string:100'),
        ('ecollectionevents.ColParticipantLocal', 'recordedBy', 'string:100'),
        ('ecatalogue.ColDepartment', 'collectionCode', 'string:100'),

        ('ecatalogue.DarKingdom', 'kingdom', 'string:100'),
        ('ecatalogue.DarPhylum', 'phylum', 'string:100'),
        ('ecatalogue.DarClass', 'class', 'string:100'),
        ('ecatalogue.DarOrder', 'order', 'string:100'),
        ('ecatalogue.DarFamily', 'family', 'string:100'),
        ('ecatalogue.DarGenus', 'genus', 'string:100'),
        ('ecatalogue.DarSubgenus', 'subgenus', 'string:100'),
        ('ecatalogue.DarSpecies', 'specificEpithet', 'string:100'),
        ('ecatalogue.DarSubspecies', 'infraspecificEpithet', 'string:100'),
        ('ecatalogue.DarHigherTaxon', 'higherClassification', 'string:100'),
        ('ecatalogue.DarInfraspecificRank', 'taxonRank', 'string:100'),

        # Location
        ('ecatalogue.DarStateProvince', 'stateProvince', 'string:100'),
        ('ecatalogue.DarContinent', 'continent', 'string:100'),
        ('ecatalogue.DarIsland', 'island', 'string:100'),
        ('ecatalogue.DarIslandGroup', 'islandGroup', 'string:100'),
        # Removed: continentOcean is not in current DwC standard, replaced by waterBody and continent
        # ('ecatalogue.DarContinentOcean', 'continentOcean', 'string:100'),
        ('ecatalogue.DarHigherGeography', 'higherGeography', 'string:100'),
        ('ecatalogue.ColHabitatVerbatim', 'habitat', 'string:100'),
        ('ecatalogue.DarLatLongComments', '_latLongComments', 'string:100'),
        ('ecatalogue.DarDecimalLongitude', 'decimalLongitude', 'float64'),
        ('ecatalogue.DarDecimalLatitude', 'decimalLatitude', 'float64'),
        ('ecatalogue.DarGeodeticDatum', 'geodeticDatum', 'string:100'),
        ('ecatalogue.DarGeorefMethod', 'georeferenceProtocol', 'string:100'),

        # LatDeriveCentroid is always True - so removing, and we'll base centroids on it being label such in DarLatLongComments
        # ('esites.LatDeriveCentroid', 'centroid', 'bool'),
        ('esites.GeorefMaxErrorDist', 'maxError', 'string:100'),
        ('esites.GeorefMaxErrorDistUnits', '_errorUnit', 'string:100'),
        ('esites.LatLongitude', 'verbatimLongitude', 'string:100'),
        ('esites.LatLatitude', 'verbatimLatitude', 'string:100'),

        # Occurrence
        ('ecatalogue.DarMinimumElevationInMeters', 'minimumElevationInMeters', 'string:100'),
        ('ecatalogue.DarMaximumElevationInMeters', 'maximumElevationInMeters', 'string:100'),
        ('ecatalogue.DarMinimumDepthInMeters', 'minimumDepthInMeters', 'string:100'),
        ('ecatalogue.DarMaximumDepthInMeters', 'maximumDepthInMeters', 'string:100'),
        # DarCollector doesn't have multiple collectors NHMUK:ecatalogue:1751715 - Switched to using ecollectionevents.ColParticipantLocal
        # ('ecatalogue.DarCollector', 'Recorded by', 'string:100'),
        ('ecatalogue.DarCollectorNumber', 'recordNumber', 'string:100'),
        ('ecatalogue.DarIndividualCount', 'individualCount', 'string:100'),
        # According to docs, ageClass has been superseded by lifeStage. We have both, but ageClass duplicates
        # And for the ~200 it has extra data, the data isn't good
        # ('ecatalogue.DarAgeClass', 'ageClass', 'string:100'),
        ('ecatalogue.DarLifeStage', 'lifeStage', 'string:100'),
        ('ecatalogue.DarSex', 'sex', 'string:100'),
        ('ecatalogue.DarPreparations', 'preparations', 'string:100'),

        # Identification
        ('ecatalogue.DarIdentifiedBy', 'identifiedBy', 'string:100'),
        # KE Emu has 3 fields for identification date: DarDayIdentified, DarMonthIdentified and DarYearIdentified
        # But EntIdeDateIdentified holds them all - which is what we want for dateIdentified
        ('ecatalogue.EntIdeDateIdentified', 'dateIdentified', 'string:100'),
        ('ecatalogue.DarIdentificationQualifier', 'identificationQualifier', 'string:100'),
        # ('ecatalogue.DarFieldNumber', 'Field number', 'string:100'),  Removed as mostly duplicates DarCollectorNumber (JW - feedback)
        ('ecatalogue.DarTimeOfDay', 'eventTime', 'string:100'),
        ('ecatalogue.DarDayCollected', 'day', 'string:100'),
        ('ecatalogue.DarMonthCollected', 'month', 'string:100'),
        ('ecatalogue.DarYearCollected', 'year', 'string:100'),

        # Geo
        ('ecatalogue.DarEarliestEon', 'earliestEonOrLowestEonothem', 'string:100'),
        ('ecatalogue.DarLatestEon', 'latestEonOrHighestEonothem', 'string:100'),
        ('ecatalogue.DarEarliestEra', 'earliestEraOrLowestErathem', 'string:100'),
        ('ecatalogue.DarLatestEra', 'latestEraOrHighestErathem', 'string:100'),
        ('ecatalogue.DarEarliestPeriod', 'earliestPeriodOrLowestSystem', 'string:100'),
        ('ecatalogue.DarLatestPeriod', 'latestPeriodOrHighestSystem', 'string:100'),
        ('ecatalogue.DarEarliestEpoch', 'earliestEpochOrLowestSeries', 'string:100'),
        ('ecatalogue.DarLatestEpoch', 'latestEpochOrHighestSeries', 'string:100'),
        ('ecatalogue.DarEarliestAge', 'earliestAgeOrLowestStage', 'string:100'),
        ('ecatalogue.DarLatestAge', 'latestAgeOrHighestStage', 'string:100'),
        ('ecatalogue.DarLowestBiostrat', 'lowestBiostratigraphicZone', 'string:100'),
        ('ecatalogue.DarHighestBiostrat', 'highestBiostratigraphicZone', 'string:100'),
        ('ecatalogue.DarGroup', 'group', 'string:100'),
        ('ecatalogue.DarFormation', 'formation', 'string:100'),
        ('ecatalogue.DarMember', 'member', 'string:100'),
        ('ecatalogue.DarBed', 'bed', 'string:100'),

        # Resource relationship
        # ('ecatalogue.DarRelatedCatalogItem', 'Related resource id', 'string:100'), Only 34 records have this field populated
        # So it's better to build automatically from part / parent records

        # Multimedia
        ('ecatalogue.MulMultiMediaRef', 'associatedMedia', 'json'),

        # Dynamic properties
        # These fields do not map to DwC, but are still very useful
        ('ecatalogue.ColRecordType', 'recordType', 'string:100'),
        ('ecatalogue.ColSubDepartment', 'subDepartment', 'string:100'),
        ('ecatalogue.PrtType', 'partType', 'string:100'),
        ('ecatalogue.RegCode', 'registrationCode', 'string:100'),
        ('ecatalogue.CatKindOfObject', 'kindOfObject', 'string:100'),
        ('ecatalogue.CatKindOfCollection', 'kindOfCollection', 'string:100'),
        ('ecatalogue.CatPreservative', 'preservative', 'string:100'),
        ('ecatalogue.ColKind', 'collectionKind', 'string:100'),
        ('ecatalogue.EntPriCollectionName', 'collectionName', 'string:100'),
        ('ecatalogue.PalAcqAccLotDonorFullName', 'donorName', 'string:100'),
        ('ecatalogue.DarPreparationType', 'preparationType', 'string:100'),
        ('ecatalogue.DarObservedWeight', 'observedWeight', 'string:100'),

        # Location
        # Data is stored in sumViceCountry field in ecatalogue data - but actually this
        # should be viceCountry (which it is in esites)
        ('ecatalogue.sumViceCountry', 'viceCounty', 'string:100'),

        ('ecatalogue.DnaExtractionMethod', 'extractionMethod', 'string:100'),
        ('ecatalogue.DnaReSuspendedIn', 'resuspendedIn', 'string:100'),
        ('ecatalogue.DnaTotalVolume', 'totalVolume', 'string:100'),
        # Parasite card
        ('ecatalogue.CardBarcode', 'barcode', 'string:100'),
        # Egg
        ('ecatalogue.EggClutchSize', 'clutchSize', 'string:100'),
        ('ecatalogue.EggSetMark', 'setMark', 'string:100'),
        # Nest
        ('ecatalogue.NesShape', 'nestShape', 'string:100'),
        ('ecatalogue.NesSite', 'nestSite', 'string:100'),
        # Silica gel
        ('ecatalogue.SilPopulationCode', 'populationCode', 'string:100'),
        # Botany
        ('ecatalogue.CollExsiccati', 'exsiccati', 'string:100'),
        ('ecatalogue.ColExsiccatiNumber', 'exsiccatiNumber', 'string:100'),
        ('ecatalogue.ColSiteDescription', 'labelLocality', 'string:100'),  # JW asked for this to be renamed from Site Description => Label locality
        ('ecatalogue.ColPlantDescription', 'plantDescription', 'string:100'),
        ('ecatalogue.FeaCultivated', 'cultivated', 'string:100'),

        # ('ecatalogue.FeaPlantForm', 'Plant form', 'string:100'),  # JW asked for this to be removed
        # Paleo
        ('ecatalogue.PalDesDescription', 'catalogueDescription', 'string:100'),
        ('ecatalogue.PalStrChronostratLocal', 'chronostratigraphy', 'string:100'),
        ('ecatalogue.PalStrLithostratLocal', 'lithostratigraphy', 'string:100'),
        # Mineralogy
        ('ecatalogue.MinDateRegistered', 'dateRegistered', 'string:100'),
        ('ecatalogue.MinIdentificationAsRegistered', 'identificationAsRegistered', 'string:100'),
        ('ecatalogue.MinIdentificationDescription', 'identificationDescription', 'string:200'),
        ('ecatalogue.MinPetOccurance', 'occurrence', 'string:100'),
        ('ecatalogue.MinOreCommodity', 'commodity', 'string:200'),
        ('ecatalogue.MinOreDepositType', 'depositType', 'string:100'),
        ('ecatalogue.MinTextureStructure', 'texture', 'string:100'),
        ('ecatalogue.MinIdentificationVariety', 'identificationVariety', 'string:100'),
        ('ecatalogue.MinIdentificationOther', 'identificationOther', 'string:100'),
        ('ecatalogue.MinHostRock', 'hostRock', 'string:100'),
        ('ecatalogue.MinAgeDataAge', 'age', 'string:100'),
        ('ecatalogue.MinAgeDataType', 'ageType', 'string:100'),
        # Mineralogy location
        ('ecatalogue.MinNhmTectonicProvinceLocal', 'tectonicProvince', 'string:100'),
        ('ecatalogue.MinNhmStandardMineLocal', 'mine', 'string:100'),
        ('ecatalogue.MinNhmMiningDistrictLocal', 'miningDistrict', 'string:100'),
        ('ecatalogue.MinNhmComplexLocal', 'mineralComplex', 'string:100'),
        ('ecatalogue.MinNhmRegionLocal', 'geologyRegion', 'string:100'),
        # Meteorite
        ('ecatalogue.MinMetType', 'meteoriteType', 'string:100'),
        ('ecatalogue.MinMetGroup', 'meteoriteGroup', 'string:100'),
        ('ecatalogue.MinMetChondriteAchondrite', 'chondriteAchondrite', 'string:100'),
        ('ecatalogue.MinMetClass', 'meteoriteClass', 'string:100'),
        ('ecatalogue.MinMetPetType', 'petrologyType', 'string:100'),
        ('ecatalogue.MinMetPetSubtype', 'petrologySubtype', 'string:100'),
        ('ecatalogue.MinMetRecoveryFindFall', 'recovery', 'string:100'),
        ('ecatalogue.MinMetRecoveryDate', 'recoveryDate', 'string:100'),
        ('ecatalogue.MinMetRecoveryWeight', 'recoveryWeight', 'string:100'),
        ('ecatalogue.MinMetWeightAsRegistered', 'registeredWeight', 'string:100'),
        ('ecatalogue.MinMetWeightAsRegisteredUnit', 'registeredWeightUnit', 'string:100'),

        # Record level
        ('ecatalogue.AdmDateModified', 'modified', 'string:100'),
        # This isn't actually in DwC - but I'm going to use dcterms:created
        ('ecatalogue.AdmDateInserted', 'created', 'string:100'),

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
        ('ecatalogue.IdeCitationTypeStatus', '_determinationTypes', 'string:100'),
        ('ecatalogue.EntIdeScientificNameLocal', '_determinationNames', 'string:250'),
        ('ecatalogue.EntIdeFiledAs', '_determinationFiledAs', 'string:100'),
        # If DarTypeStatus is empty, we'll use sumTypeStatus which has previous determinations
        ('ecatalogue.sumTypeStatus', '_sumTypeStatus', 'string:100'),

        # Locality if nearest named place is empty
        # The encoding of DarLocality is buggered - see ecatalogue.1804973
        # So better to use the original field with the correct encoding
        ('ecatalogue.sumPreciseLocation', '_preciseLocation', 'string:100'),
        # Locality if precise and nearest named place is empty
        ('ecatalogue.MinNhmVerbatimLocalityLocal', '_minLocalityLocal', 'string:100'),

        # CITES specimens
        ('ecatalogue.cites', '_cites', 'bool'),

        # Parasite cards use a different field for life stage
        ('ecatalogue.CardParasiteStage', '_parasiteStage', 'string:100'),

        # Join keys
        ('ecollectionevents._id', '_ecollectioneventsIrn', 'int32'),
        ('esites._id', '_esitesIrn', 'int32'),

        # Removed: We do not want notes, could contain anything
        # ('ecatalogue.DarNotes', 'DarNotes', 'string:100'),
        # ('ecatalogue.DarLatLongComments', 'latLongComments', 'string:100'),

    ]

    # Used to merge in data from parasite cards, which do not have taxonomic data
    parasite_taxonomy_fields = [
        ('_id', '_irn', 'int32'),
        ('ClaScientificNameBuilt', 'scientificName', 'string:100'),
        ('ClaKingdom', 'kingdom', 'string:60'),
        ('ClaPhylum', 'phylum', 'string:100'),
        ('ClaClass', 'class', 'string:100'),
        ('ClaOrder', 'order', 'string:100'),
        ('ClaFamily', 'family', 'string:100'),
        ('ClaGenus', 'genus', 'string:100'),
        ('ClaSubgenus', 'subgenus', 'string:100'),
        ('ClaSpecies', 'specificEpithet', 'string:100'),
        ('ClaSubspecies', 'infraspecificEpithet', 'string:100'),
        ('ClaRank', 'taxonRank', 'string:10')  # NB: CKAN uses rank internally
    ]

    # Columns not selected from the database
    # In the format (field_name, field_type, default_value)
    literal_columns = [
        ('institutionCode', 'string:100', 'NHMUK'),
        ('basisOfRecord', 'string:100', 'Specimen'),
        ('determinations', 'json', np.NaN),
        # This is set dynamically if this is a part record (with parent Ref)
        ('relatedResourceID', 'string:100', np.NaN),
        ('relationshipOfResource', 'string:100', np.NaN),
        ('centroid', 'bool', False),
        ('otherCatalogNumbers', 'string:100', np.NaN),
        # Add data quality indicators
        ('dqi', 'string:16', DQI_UNKNOWN),
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

        # And exclude all with an embargo date
        query['NhmSecEmbargoDate'] = 0

        # query['EntIdeScientificNameLocal'] = {"$exists": 1}
        # query['MulMultiMediaRef'] = {"$exists": 1}
        # query['_id'] = {'$in': [209958]}

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
        df['collectionCode'] = df['collectionCode'].str.upper().str[0:3]
        # Entom record collection code = BMNH(E)
        df['collectionCode'][df['collectionCode'] == 'ENT'] = "BMNH(E)"

        # We use GBIF to populate Data Quality Indicators, so mark all mineralogy records as N/A
        df['dqi'][df['collectionCode'] == 'MIN'] = DQI_NOT_APPLICABLE

        # Add the old stable identifier - IRN concatenated with catalogue name etc.,
        df['otherCatalogNumbers'] = 'NHMUK:ecatalogue:' + df['_id'].astype('str')

        # Ensure multimedia resources are suitable (jpeg rather than tiff etc.,)
        self.ensure_multimedia(df, 'associatedMedia')

        # Assign determination name, type and field as to determinations for determination history
        determination_fields = [
            ('name', '_determinationNames'),
            ('type', '_determinationTypes'),
            ('filedAs', '_determinationFiledAs')
        ]

        def determinations_json(row):
            """
            Convert determination fields to json
            Dictionary comprehension looping through each field, and if it exists adding to a dict
            @param row:
            @return:
            """
            return json.dumps({field_name: row[determination].split(';') for field_name, determination in determination_fields if row[determination]})

        df['determinations'] = df[df['_determinationNames'] != ''].apply(determinations_json, axis=1)

        # There doesn't seem to be a good way to identify centroids in KE EMu
        # I was using esites.LatDeriveCentroid, but this always defaults to True
        # And trying to use centroid lat/lon fields, also includes pretty much every record
        # But matching against *entroid being added to georeferencing notes produces much better results
        df['centroid'][df['_latLongComments'].str.contains("entroid")] = True

        # Convert all blank strings to NaN so we can use fillna & combine_first() to replace NaNs with value from parent df
        df = df.applymap(lambda x: np.nan if isinstance(x, basestring) and x == '' else x)

        df['catalogNumber'].fillna(df['_regRegistrationNumber'], inplace=True)

        # If PalNearestNamedPlaceLocal is missing, use sumPreciseLocation
        # And then try MinNhmVerbatimLocalityLocal
        df['locality'].fillna(df['_preciseLocation'], inplace=True)
        df['locality'].fillna(df['_minLocalityLocal'], inplace=True)

        # Replace missing DarTypeStatus
        df['typeStatus'].fillna(df['_sumTypeStatus'], inplace=True)

        # Replace missing CatPreservative
        df['preservative'].fillna(df['_entCatPreservation'], inplace=True)

        # Cultivated should only be set on Botany records - but is actually on everything
        df['cultivated'][df['collectionCode'] != 'BOT'] = np.nan

        # Process part parents
        parent_irns = self._get_unique_irns(df, '_parentRef')

        if parent_irns:
            # We want to get all parts associated to one parent record, so we can provide them as associated records
            # So select all records matching the parent IRN
            q = dict(self.query)

            # Delete _id if it's set - need this for testing
            if '_id' in q:
                del q['_id']

            # Get all records with the same parent, so we can add them as related records
            q['RegRegistrationParentRef'] = {'$in': parent_irns}
            monary_query = m.query(config.get('mongo', 'database'), 'ecatalogue', q, ['RegRegistrationParentRef', 'AdmGUIDPreferredValue'], ['int32', 'string:36'])
            part_df = pd.DataFrame(np.matrix(monary_query).transpose(), columns=['RegRegistrationParentRef', 'AdmGUIDPreferredValue'])
            part_df['RegRegistrationParentRef'] = part_df['RegRegistrationParentRef'].astype('int32')

            # Group by parent ref and concatenate all the GUIDs together
            # So we now have:
            # parent_irn   guid; guid
            parts = part_df.groupby('RegRegistrationParentRef')['AdmGUIDPreferredValue'].apply(lambda x: "%s" % ';'.join(x))

            # And update the main date frame with the group parts, merged on _parentRef
            df['relatedResourceID'] = df.apply(lambda row: parts[row['_parentRef']] if row['_parentRef'] in parts else np.NaN, axis=1)
            df['relationshipOfResource'][df['relatedResourceID'].notnull()] = 'Parts'

            parent_df = self.get_dataframe(m, 'ecatalogue', self.get_collection_source_columns('ecatalogue'), parent_irns, '_id')

            # Ensure the parent multimedia images are usable
            self.ensure_multimedia(parent_df, 'associatedMedia')

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
        df['decimalLongitude'] = df['decimalLongitude'].astype('float64')
        df['decimalLatitude'] = df['decimalLatitude'].astype('float64')

        # Get all collection columns
        collection_columns = self.get_collection_source_columns()

        # Load extra sites info (if there's an error radius + unit)
        site_irns = self._get_unique_irns(df, '_siteRef')

        sites_df = self.get_dataframe(m, 'esites', collection_columns['esites'], site_irns, '_esitesIrn')
        # Append the error unit to the max error value
        # Error unit can be populated even when Max error is not, so need to check max error first
        sites_df['maxError'][sites_df['maxError'] != ''] = sites_df['maxError'].astype(str) + ' ' + sites_df['_errorUnit'].astype(str)

        df = pd.merge(df, sites_df, how='outer', left_on=['_siteRef'], right_on=['_esitesIrn'])

        # For CITES species, we need to hide Lat/Lon and Locality data - and label images
        for i in ['locality', 'labelLocality', 'decimalLongitude', 'decimalLatitude', 'verbatimLongitude', 'verbatimLatitude', 'centroid', 'maxError', 'higherGeography', 'associatedMedia']:
            df[i][df['_cites'] == 'True'] = np.NaN

        # Some records are being assigned a Centroid even if they have no lat/lon fields.
        # Ensure it's False is latitude is null
        df['centroid'][df['decimalLatitude'].isnull()] = False

        # Load collection event data
        collection_event_irns = self._get_unique_irns(df, '_collectionEventRef')

        # if collection_event_irns:
        collection_event_df = self.get_dataframe(m, 'ecollectionevents', collection_columns['ecollectionevents'], collection_event_irns, '_ecollectioneventsIrn')
        # print collection_event_df
        df = pd.merge(df, collection_event_df, how='outer', left_on=['_collectionEventRef'], right_on=['_ecollectioneventsIrn'])

        # Add parasite life stage
        # Parasite cards use a different field for life stage
        df['lifeStage'].fillna(df['_parasiteStage'], inplace=True)

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