import json

import numpy as np
import pandas as pd

from vst_api.indexer.Loader.SheetLoader import SheetLoader
from vst_api.utils.log import logger

#
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

aa = {
    "جنگی": "war",
    "به علت بیماری - دیابت": "disease",
    "شغلی / حوادث": "accident_or_job",
    "مادرزادی": "birth_defect",
    "فلج اطفال": "polio",
}


class AmputationHistoryLoader(SheetLoader):

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

        # self.df['UseNo'] = self.df['UseNo'].fillna(-1).astype(int)
        # self.df['DurationYear'] = self.df['DurationYear'].fillna(-1).astype(int)
        # self.df['DayInWeek'] = self.df['DayInWeek'].fillna(-1).astype(int)
        # self.df['WeekInMonth'] = self.df['WeekInMonth'].fillna(-1).astype(int)
        # self.df['MonthInYear'] = self.df['MonthInYear'].fillna(-1).astype(int)

        self.df.groupby("IRPC").apply(self._agg_func)
        self.json_list = json_list

    def __len__(self):
        return len(self.json_list)

    def _agg_func(self, rows):

        count = 0
        failed_to_parse = 0
        disabilities = {}
        causes = ""
        for index, row in rows.iterrows():
            try:
                causes += f"{aa[row['CauseName']]}, "
                disabilities.update(
                    {
                        aa[row["CauseName"]]: {
                            "DisabilityTypeID": row["DisabilityTypeID"],
                            "OrgansName": row["OrgansName"],
                            "DisabilityCauseID": row["DisabilityCauseID"],
                            # 'CauseName': row['CauseName'],
                        }
                    }
                )
                count += 1
            except Exception as e:
                failed_to_parse += 1
                logger.warning(e)

        new_row = rows.iloc[0]

        json_list.append(
            {
                "IRPC": rows.index[0],
                self.prefix + "QuesID": new_row["QuesID"],
                self.prefix + "GenderID": new_row["GenderID"],
                self.prefix + "InterviewCenterID": new_row["InterviewCenterID"],
                self.prefix + "BirthYear": new_row["BirthYear"],
                self.prefix + "InterviewDateG": new_row["InterviewDateG"],
                self.prefix + "AgeInStudy": new_row["AgeInStudy"],
                self.prefix + "disabilities": {
                    "list": disabilities,
                    "count": count,
                    "failed_to_parse": failed_to_parse,
                    "causes": causes,
                },
            }
        )
