import re

from vst_api.indexer.Loader.SheetLoader import SheetLoader
from vst_api.utils.log import logger


def extract_drugname_dose(text):
    """
    sample drug name and dose:
        "ASA (Acetylsalicylic Acid) 80 mg"

    returns:
        "ASA (Acetylsalicy", "80 mg"
    """
    try:
        # Match any non-digit characters at the beginning for drugname
        result = re.match(r"([^0-9]+)\s(.*)", text)

        name = result.group(1) if result else None
        dose = result.group(2) if result else None
        return name, dose
    except Exception as e:
        logger.warning(e)
        return "", ""


# class NpEncoder(json.JSONEncoder):
#     def default(self, obj):
#         if isinstance(obj, np.integer):
#             return int(obj)
#         if isinstance(obj, np.floating):
#             return float(obj)
#         if isinstance(obj, np.ndarray):
#             return obj.tolist()
#         return super(NpEncoder, self).default(obj)
#
# # Your codes ....


json_list = []


def process_drug_name(drug):
    pass


class UsedDrugLoader(SheetLoader):

    def __init__(
        self,
        file_path,
        sheet_name,
        data_types=None,
        skiprows=1,
        calculate_dates=False,
        add_prefix_to_colnames=False
    ):
        super().__init__(
            file_path, sheet_name, data_types, skiprows, calculate_dates
        )

        self.prefix = sheet_name.idx_prefix + '.' if add_prefix_to_colnames else ''

        self.df["UseNo"] = self.df["UseNo"].fillna(-1).astype(int)
        self.df["DurationYear"] = (
            self.df["DurationYear"].fillna(-1).astype(int)
        )
        self.df["DayInWeek"] = self.df["DayInWeek"].fillna(-1).astype(int)
        self.df["WeekInMonth"] = self.df["WeekInMonth"].fillna(-1).astype(int)
        self.df["MonthInYear"] = self.df["MonthInYear"].fillna(-1).astype(int)

        self.df.groupby("IRPC").apply(self._agg_func)
        self.json_list = json_list

    def __len__(self):
        return len(self.json_list)

    def _agg_func(self, rows):
        drugs_list = []
        count = 0
        failed_to_parse = 0

        for index, row in rows.iterrows():
            try:
                count += 1
                drug_name = extract_drugname_dose(row["Name"])
                drugs_list.append(
                    {
                        "DrugID": row["DrugID"],
                        "drug": {
                            "name": drug_name[0],
                            "dose": drug_name[1],
                        },
                        "UseNo": row["UseNo"],
                        "DurationYear": row["DurationYear"],
                        "DayInWeek": row["DayInWeek"],
                        "WeekInMonth": row["WeekInMonth"],
                        "MonthInYear": row["MonthInYear"],
                        "DrugIntervalUsedTypeID": row["DrugIntervalUsedTypeID"],
                    }
                )
            except Exception as e:
                failed_to_parse += 1
                logger.warning(e)

        row_new = rows.iloc[0]
        a = {
            "IRPC": rows.index[0],
            self.prefix + "QuesID": row_new["QuesID"],
            self.prefix + "GenderID": row_new["GenderID"],
            self.prefix + "InterviewCenterID": row_new["InterviewCenterID"],
            self.prefix + "BirthYear": row_new["BirthYear"],
            self.prefix + "InterviewDateG": row_new["InterviewDateG"],
            self.prefix + "AgeInStudy": row_new["InterviewCenterID"],
            self.prefix + "UserName": row_new["UserName"],
            self.prefix + "drugs": {
                "drugs_list": drugs_list,
                "count": count,
                "failed_to_parse": failed_to_parse,
            },
        }
        json_list.append(a)

