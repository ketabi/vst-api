from Loader.SheetLoader import SheetLoader

from vst_api.config import settings
from vst_api.indexer.elastic import index_in_elastic, index_json_in_elastic, index_in_elastic_all, update_in_elastic
from vst_api.indexer.Loader.AmputationHistoryLoader import (
    AmputationHistoryLoader,
)
from vst_api.indexer.Loader.EmploymentSheetLoader import (
    EmploymentHistorySheetLoader,
)
from vst_api.indexer.Loader.FoodLoader import FoodLoader
from vst_api.indexer.Loader.GeneralDataSheetLoader import (
    GeneralDataSheetLoader,
)
from vst_api.indexer.Loader.HFHistorySheetLoader import HFHistorySheetLoader
from vst_api.indexer.Loader.UsedDrugLoader import UsedDrugLoader
from vst_api.indexer.files import files_tuple
from vst_api.indexer.utils.utils import (
    camel_to_snake,
    get_filename,
    list_files,
)

from vst_api.utils.log import logger


files_path, file_names = list_files()

logger.info(f"{len(files_path)} files")
logger.info(files_path)


def extract_single_files():
    i = 0
    for i, file_path in enumerate(files_path):
        file_name = get_filename(file_path)
        name = camel_to_snake(file_name)

        logger.info("-" * 50)
        logger.info(f"{i} - {file_name}")

        loader = None

        if file_name == "SleepAssessment":
            # continue
            name = "sleep_assessment"
            loader = SheetLoader(file_path, name, calculate_dates=True)

        if file_name == "EmploymentStatus":
            # continue
            name = "employment_status"
            loader = SheetLoader(file_path, name, calculate_dates=True)

        if file_name == "GeneralData":
            # continue
            name = "general_data"
            loader = GeneralDataSheetLoader(file_path, name, calculate_dates=True)

        if file_name == "Final_MET_Score":
            # continue
            name = "final_met_score"
            loader = SheetLoader(file_path, name, calculate_dates=True)

        if file_name == "ChronicDisease1":
            # continue
            name = "chronic_disease_1"
            loader = SheetLoader(file_path, name, calculate_dates=True)

        if file_name == "Gynecologist":
            # continue
            name = "gynecologist"
            loader = SheetLoader(file_path, name, calculate_dates=True)

        if file_name == "ImportantMedicationAndDiseases":
            # continue
            name = "important_medication"
            loader = SheetLoader(file_path, name, calculate_dates=True)

        if file_name == "AmputationHistory":
            # continue
            name = "amputation_history"
            loader = AmputationHistoryLoader(file_path, name, calculate_dates=True)

        if file_name == "FamilyHistory":
            # continue
            name = "family_history"
            loader = SheetLoader(file_path, name, calculate_dates=True)

        if file_name == "PhysicalExam":
            # continue
            name = "physical_exam"
            loader = SheetLoader(file_path, name, calculate_dates=True)

        if file_name == "HFHistory":
            # continue
            name = "hf_history"
            loader = HFHistorySheetLoader(file_path, name, calculate_dates=True)

        if file_name == "NBS":
            # continue
            name = "nbs"
            loader = SheetLoader(file_path, name, calculate_dates=True)

        if file_name == "GPD":
            # continue
            name = "gpd"
            loader = SheetLoader(file_path, name, calculate_dates=True)

        if file_name == "Habit":
            name = "habit"
            loader = SheetLoader(file_path, name, calculate_dates=True)

        if file_name == "Socioeconomic":
            # continue
            name = "socioeconomic"
            loader = SheetLoader(file_path, name, calculate_dates=True)

        if file_name == "OralHealth":
            # continue
            name = "oral_health"
            loader = SheetLoader(file_path, name, calculate_dates=True)

        if file_name == "BloodPressure":
            # continue
            name = "blood_pressure"
            loader = SheetLoader(file_path, name, calculate_dates=True)

        if file_name == "foods":
            # continue
            name = "foods_habit"
            loader = FoodLoader(file_path, name, calculate_dates=True)

        if file_name == "GFR_Score":
            # continue
            name = "gfr_score"
            loader = SheetLoader(file_path, name, calculate_dates=True)

        if file_name == "UsedDrug":
            # continue
            name = "used_drug"
            loader = UsedDrugLoader(file_path, name, calculate_dates=True)

        if file_name == "PhysicalActivity":
            # continue
            name = "physical_activity"
            loader = SheetLoader(file_path, name, calculate_dates=True)

        if file_name == "AnthropometericExam":
            # continue

            name = "anthropometeric_exam"
            loader = SheetLoader(file_path, name, calculate_dates=True)

        if file_name == "ChronicDisease2":
            name = "chronic_disease_2"
            loader = SheetLoader(file_path, name, calculate_dates=True)

        if file_name == "EmploymentHistory":
            name = "employment_history"
            loader = EmploymentHistorySheetLoader(
                file_path, name, calculate_dates=True
            )

        if file_name == "LifeStyle":
            name = "life_style"
            loader = SheetLoader(file_path, name, calculate_dates=True)

        if file_name == "Wealth_Score":
            name = "wealth_score"
            loader = SheetLoader(file_path, name, calculate_dates=True)

        indexed, ignored = index_in_elastic(
            loader, settings.index_prefix + name.lower()
        )

        info = loader.get_info()
        info["failed_to_index"] = ignored
        info["indexed_count"] = indexed

        index_json_in_elastic(info, id=name, index_name="cohort_indexes_info")
        logger.info(f"index info {info}")


def process_columns(loader):
    sheet = loader.sheet_info

    idx_prefix = sheet.idx_prefix
    sheet_name = sheet.file_name

    is_json_list = True if hasattr(loader, "json_list") else False

    if is_json_list:
        return loader
    else:
        """
        common fields for all:
            'QuesID', 'InterviewCenterID', 'InterviewDateG'
            We keep those for now to check weather they are not equal or has same values
        
        common fields that should be the same for all
        and we keep just in general
            'GenderID', 'BirthYear', 'AgeInStudy'
        """
        if sheet_name == 'general':
            cols_to_remove = []
        else:
            cols_to_remove = ['GenderID', 'BirthYear', 'AgeInStudy']

        df = loader.df.drop(cols_to_remove, axis=1, errors='ignore')
        logger.info(f'<{sheet_name}> - removed {len(cols_to_remove)} cols: {cols_to_remove}')

        loader.df = df.rename(columns={c: f'{idx_prefix}.{c}' for c in df.columns if c not in ['IRPC']})
        logger.info(f'<<{sheet_name}>> - {len(df.columns)} new cols: {loader.df.columns}')

        return loader


def merge_files():
    i = 0
    for i, file_path in enumerate(files_path):
        file_name = get_filename(file_path)
        # name = camel_to_snake(file_name)

        logger.info("-" * 50)
        logger.info(f"Processing {i} - <{file_name}>")

        loader = None

        if file_name == "SleepAssessment":
            t = files_tuple['SleepAssessment']
            loader = SheetLoader(file_path, t, calculate_dates=True)

        if file_name == "EmploymentStatus":
            t = files_tuple['EmploymentStatus']
            loader = SheetLoader(file_path, t, calculate_dates=True)

        if file_name == "GeneralData":
            t = files_tuple['GeneralData']
            loader = GeneralDataSheetLoader(file_path, t, calculate_dates=True)

        if file_name == "Final_MET_Score":
            # continue
            t = files_tuple['Final_MET_Score']
            loader = SheetLoader(file_path, t, calculate_dates=True)

        if file_name == "ChronicDisease1":
            # continue
            t = files_tuple['ChronicDisease1']
            loader = SheetLoader(file_path, t, calculate_dates=True)

        if file_name == "Gynecologist":
            # continue
            t = files_tuple['Gynecologist']
            loader = SheetLoader(file_path, t, calculate_dates=True)

        if file_name == "ImportantMedicationAndDiseases":
            # continue
            t = files_tuple['ImportantMedicationAndDiseases']
            loader = SheetLoader(file_path, t, calculate_dates=True)

        if file_name == "AmputationHistory":
            t = files_tuple['AmputationHistory']
            loader = AmputationHistoryLoader(file_path, t, calculate_dates=True, add_prefix_to_colnames=True)

        if file_name == "FamilyHistory":
            # continue
            t = files_tuple['FamilyHistory']
            loader = SheetLoader(file_path, t, calculate_dates=True)

        if file_name == "PhysicalExam":
            # continue
            t = files_tuple['PhysicalExam']
            loader = SheetLoader(file_path, t, calculate_dates=True)

        if file_name == "HFHistory":
            # continue
            t = files_tuple['HFHistory']
            loader = HFHistorySheetLoader(file_path, t, calculate_dates=True, add_prefix_to_colnames=True)

        if file_name == "NBS":
            # continue
            t = files_tuple['NBS']
            loader = SheetLoader(file_path, t, calculate_dates=True)

        if file_name == "GPD":
            # continue
            t = files_tuple['GPD']
            loader = SheetLoader(file_path, t, calculate_dates=True)

        if file_name == "Habit":
            t = files_tuple['Habit']
            loader = SheetLoader(file_path, t, calculate_dates=True)

        if file_name == "Socioeconomic":
            # continue
            t = files_tuple['Socioeconomic']
            loader = SheetLoader(file_path, t, calculate_dates=True)

        if file_name == "OralHealth":
            # continue
            t = files_tuple['OralHealth']
            loader = SheetLoader(file_path, t, calculate_dates=True)

        if file_name == "BloodPressure":
            # continue
            t = files_tuple['BloodPressure']
            loader = SheetLoader(file_path, t, calculate_dates=True)

        if file_name == "foods":
            # continue
            t = files_tuple['foods']
            loader = FoodLoader(file_path, t, calculate_dates=True)

        if file_name == "GFR_Score":
            # continue
            t = files_tuple['GFR_Score']
            loader = SheetLoader(file_path, t, calculate_dates=True)

        if file_name == "UsedDrug":
            # continue
            t = files_tuple['UsedDrug']
            loader = UsedDrugLoader(file_path, t, calculate_dates=True, add_prefix_to_colnames=True)

        if file_name == "PhysicalActivity":
            # continue
            t = files_tuple['PhysicalActivity']
            loader = SheetLoader(file_path, t, calculate_dates=True)

        if file_name == "AnthropometericExam":
            # continue

            t = files_tuple['AnthropometericExam']
            loader = SheetLoader(file_path, t, calculate_dates=True)

        if file_name == "ChronicDisease2":
            t = files_tuple['ChronicDisease2']
            loader = SheetLoader(file_path, t, calculate_dates=True)

        if file_name == "EmploymentHistory":
            t = files_tuple['EmploymentHistory']
            loader = EmploymentHistorySheetLoader(
                file_path, t, calculate_dates=True, add_prefix_to_colnames=True
            )

        if file_name == "LifeStyle":
            t = files_tuple['LifeStyle']
            loader = SheetLoader(file_path, t, calculate_dates=True)

        if file_name == "Wealth_Score":
            t = files_tuple['Wealth_Score']
            loader = SheetLoader(file_path, t, calculate_dates=True)

        if loader:
            indexed, ignored, updated = update_in_elastic(process_columns(loader), settings.index_prefix + 'all', file_name)

            info = loader.get_info()
            info["count_failed_to_index"] = ignored
            info["count_indexed"] = indexed
            info["count_updated"] = updated

            info["idx_prefix"] = t.idx_prefix
            info["file_name"] = t.file_name + '.xlsx'

            index_json_in_elastic(info, id=t.idx_name, index_name="cohort_indexes_info_all")
            logger.info(f" ✔️ {loader.sheet_info.idx_name} - Index info {info}")
        else:
            logger.warning('Loader is None')

    logger.info('Merging DONE ✔️✔️✔️ ')


merge_files()
