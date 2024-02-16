import json

import numpy as np

from vst_api.indexer.Loader.SheetLoader import SheetLoader
from vst_api.utils.log import logger


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)


# Your codes ....


json_list = []


def _process_number(f):
    try:
        return int(f)
    except Exception as e:
        return -1


class HFHistorySheetLoader(SheetLoader):

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

        self.df["FromAge"] = (
            self.df["FromAge"]
            .astype(str)
            .apply(lambda x: x.replace("/", ".").split(".")[0])
            .apply(_process_number)
            .astype(np.int8)
        )

        self.df["ToAge"] = (
            self.df["ToAge"]
            .astype(str)
            .apply(lambda x: x.replace("/", ".").split(".")[0])
            .apply(_process_number)
            .astype(np.int8)
        )

        self.df["HouseTypeID"].astype(np.int8)
        self.df["CookingFeulTypeID"].astype(np.int8)
        self.df["HeatingFuelTypeID"].astype(np.int8)
        self.df["HeatingSystemID"].astype(np.int8)

        self.df.groupby("IRPC").apply(self.agg_func)
        self.json_list = json_list

    def __len__(self):
        return len(self.json_list)

    def agg_func(self, rows):
        hf_history_list = []
        fluctuations = ""
        count = 0
        has_fix_length = True
        length = 0
        lengths = ""
        failed_to_parse = 0

        for index, row in rows.iterrows():
            try:
                fluctuations += str(row["FromAge"]) + "-" + str(row["ToAge"]) + ","
                count += 1

                _length = int(row["ToAge"]) - int(row["FromAge"])
                if length != _length:
                    has_fix_length = False
                lengths += str(_length) + ", "
                length = _length

                hf_history_list.append(
                    {
                        "FromAge": row["FromAge"],
                        "ToAge": row["ToAge"],
                        "HouseTypeID": row["HouseTypeID"],
                        "CookingFuelTypeID": row["CookingFeulTypeID"],
                        "HeatingFuelTypeID": row["HeatingFuelTypeID"],
                        "HeatingSystemID": row["HeatingSystemID"],
                    }
                )
            except Exception as e:
                failed_to_parse += 1
                logger.warning(f'{self.sheet_name.idx_name} - {e.message}')

        row_new = rows.iloc[0]
        # TODO cols in merging could be different (not having some common cols)
        a = {
            "IRPC": rows.index[0],
            self.prefix + "QuesID": row_new["QuesID"],
            self.prefix + "GenderID": row_new["GenderID"],
            self.prefix + "InterviewCenterID": row_new["InterviewCenterID"],
            self.prefix + "UserName": row_new["UserName"],
            self.prefix + "BirthYear": row_new["BirthYear"],
            self.prefix + "InterviewDateG": row_new["InterviewDateG"],
            self.prefix + "AgeInStudy": row_new["AgeInStudy"],
            self.prefix + "hf_history": {
                "hf_history_list": hf_history_list,
                "count": count,
                "has_fix_length": has_fix_length,
                "lengths": lengths,
                "fluctuations": fluctuations,
                "failed_to_parse": failed_to_parse,
            },
        }
        json_list.append(a)
