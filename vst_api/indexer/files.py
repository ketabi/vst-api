import collections

from vst_api.indexer.Loader.AmputationHistoryLoader import AmputationHistoryLoader
from vst_api.indexer.Loader.EmploymentSheetLoader import EmploymentHistorySheetLoader
from vst_api.indexer.Loader.FoodLoader import FoodLoader
from vst_api.indexer.Loader.GeneralDataSheetLoader import GeneralDataSheetLoader
from vst_api.indexer.Loader.HFHistorySheetLoader import HFHistorySheetLoader
from vst_api.indexer.Loader.SheetLoader import SheetLoader
from vst_api.indexer.Loader.UsedDrugLoader import UsedDrugLoader

Sheet = collections.namedtuple('Sheet', ['file_name', 'idx_prefix', 'idx_name', 'loader'])

files_tuple = {
    "SleepAssessment":                Sheet('SleepAssessment',                'sleep',            'sleep_assessment',     SheetLoader),
    "EmploymentStatus":               Sheet('EmploymentStatus',               'employ',           'employment_status',    SheetLoader),
    "GeneralData":                    Sheet('GeneralData',                    'general',          'general_data',         GeneralDataSheetLoader),
    "Final_MET_Score":                Sheet('Final_MET_Score',                'met',              'final_met_score',      SheetLoader),
    "ChronicDisease1":                Sheet('ChronicDisease1',                'chronic_1',        'chronic_disease_1',    SheetLoader),
    "Gynecologist":                   Sheet('Gynecologist',                   'gynecologist',     'gynecologist',         SheetLoader),
    "ImportantMedicationAndDiseases": Sheet('ImportantMedicationAndDiseases', 'medication',       'important_medication', SheetLoader),
    "AmputationHistory":              Sheet('AmputationHistory',              'amputation',       'amputation_history',   AmputationHistoryLoader),
    "FamilyHistory":                  Sheet('FamilyHistory',                  'family',           'family_history',       SheetLoader),
    "PhysicalExam":                   Sheet('PhysicalExam',                   'ph_exam',          'physical_exam',        SheetLoader),
    "HFHistory":                      Sheet('HFHistory',                      'hf',               'hf_history',           HFHistorySheetLoader),
    "NBS":                            Sheet('NBS',                            'nbs',              'nbs',                  SheetLoader),
    "GPD":                            Sheet('GPD',                            'gpd',              'gpd',                  SheetLoader),
    "Habit":                          Sheet('Habit',                          'habit',            'habit',                SheetLoader),
    "Socioeconomic":                  Sheet('Socioeconomic',                  'socioeconomic',    'socioeconomic',        SheetLoader),
    "OralHealth":                     Sheet('OralHealth',                     'oral',             'oral_health',          SheetLoader),
    "BloodPressure":                  Sheet('BloodPressure',                  'blood',            'blood_pressure',       SheetLoader),
    "foods":                          Sheet('foods',                          'foods',            'foods_habit',          FoodLoader),
    "GFR_Score":                      Sheet('GFR_Score',                      'gfr',              'gfr_score',            SheetLoader),
    "UsedDrug":                       Sheet('UsedDrug',                       'drug',             'used_drug',            UsedDrugLoader),
    "PhysicalActivity":               Sheet('PhysicalActivity',               'ph_activity',      'physical_activity',    SheetLoader),
    "AnthropometericExam":            Sheet('AnthropometericExam',            'anthropometeric',  'anthropometeric_exam', SheetLoader),
    "ChronicDisease2":                Sheet('ChronicDisease2',                'chronic_2',        'chronic_disease_2',    SheetLoader),
    "EmploymentHistory":              Sheet('EmploymentHistory',              'employ',           'employment_history',   EmploymentHistorySheetLoader),
    "LifeStyle":                      Sheet('LifeStyle',                      'life',             'life_style',           SheetLoader),
    "Wealth_Score":                   Sheet('Wealth_Score',                   'wealth',           'wealth_score',         SheetLoader),
}


