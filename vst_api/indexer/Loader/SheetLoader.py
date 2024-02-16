import numpy as np
import pandas as pd

from vst_api.indexer.utils.utils import (
    calculate_study_age,
    extract_year,
    to_gregorian,
)
from vst_api.utils.log import logger

MALE_GENDER = 1
FEMALE_GENDER = 2


class SheetLoader(object):

    def __init__(
        self,
        file_path,
        sheet_info,
        data_types=None,
        skiprows=1,
        calculate_dates=False,
    ):

        try:
            sheet_name = sheet_info.idx_name
            logger.info(f"Processing sheet <{sheet_name}>")

            self.sheet_info = sheet_info

            self.info = {"removed_cols": []}

            # print(f'Creating a loader: {sheet_name}')
            self.file_path = file_path
            self.sheet_name = sheet_name

            self.info["name"] = sheet_name
            self.info["path"] = file_path
            self.info["skip_rows"] = skiprows

            self.removed_cols = []
            self.csv_data = self._read_data(data_types, skiprows)

            # FIXME two df
            self.df = self.csv_data
            # self._set_unique_ids()
            self.df = self._process_irpc()

            self._find_duplicate_by_id()

            if calculate_dates:
                self._birth_year_calculation()
                self._interview_date_calculation()
                self._age_calculation()

            self.df = self._process_date_fields()

            self.info["row_count"] = len(self.df)
            self.info["column_count"] = len(self.df.columns)
            self.info["columns_indexed"] = list(self.df.columns)

            self.info["removed_cols_count"] = len(self.info["removed_cols"])
        except Exception as e:
            logger.error(e)
            raise Exception

    # def __iter__(self):
    #     return iter(self.df)

    def __len__(self):
        return self.df.shape[0]

    def _read_data(self, data_types, skip_rows):
        # print('='*100)
        # print()
        # print("Reading this file  {} ".format(self.file_path))
        if self.file_path.endswith(".csv"):
            # parse csv with pandas
            csv_data = pd.read_csv(
                self.file_path,
                skiprows=skip_rows,  # number of lines to skip (int) at the start of the file.
                header=0,  # Row number(s) to use as the column names, and the start of the data.
                dtype=data_types,
            )
        elif self.file_path.endswith(".xlsx"):
            csv_data = pd.read_excel(
                self.file_path,
                skiprows=skip_rows,  # number of lines to skip (int) at the start of the file.
                header=0,  # Row number(s) to use as the column names, and the start of the data.
                dtype=data_types,
            )
        # iterator=True,
        # converters={
        #     'no': int,
        # }
        # # chunksize=chunksize
        # )

        # print('File {} has this shape:{}'.format(self.file_path, csv_data.shape))
        logger.info(f"<{self.sheet_name}> {csv_data.shape}")
        # print('columns: {}'.format(csv_data.columns))

        self.csv_data = csv_data

        return csv_data

    def _set_unique_ids(self):
        """
        List Unique Values In A pandas Column

        #List unique values in the df['name'] column
        df.name.unique()

        https://chrisalbon.com/python/data_wrangling/pandas_list_unique_values_in_column
        :return:
        """
        # self.ids_unique = self.csv_data[IRPC_COLUMN_NAME].unique()
        # print('No. unique ids :{}'.format(len(self.ids_unique)))

    def describe(self):
        # -----------------------------------------------------------------------
        # DESCRIBE
        # -----------------------------------------------------------------------
        print("\n" + "-" * 50)
        print("Describe")
        print("-" * 50)

        print(self.csv_data.describe())

    def get_info(self):
        # -----------------------------------------------------------------------
        # INFO
        # -----------------------------------------------------------------------
        # print("\n" + "-" * 50)
        # print("info")
        # print("-" * 50)
        #
        # print(self.csv_data.info())
        return self.info

    def value_count(self):
        # -----------------------------------------------------------------------
        # VALUE COUNT
        # -----------------------------------------------------------------------
        print("\n" + "-" * 50)
        print("value count")
        print("-" * 50)

        # print('value counts for {} column'.format(IRPC_COLUMN_NAME))
        # print(self.csv_data[IRPC_COLUMN_NAME].value_counts(
        #     sort=True,
        #     # normalize=True
        # ))

    def _process_irpc(self):
        df = self.df
        # print(f'input df shape: {df.shape}')

        # This returns a pandas Series contain true for IRPC's which is not null
        null_rows = df.loc[~df["IRPC"].notnull()]
        # if not null_rows.empty:
        logger.info(f"{len(null_rows)} rows have null IRPC")
        self.info["null_irpc_count"] = len(null_rows)

        # Remove null IRPC's
        df = df.loc[df["IRPC"].notnull()]
        self.info["null_irpc_removed"] = True

        # convert type of IRPC to an integer instead of float
        # print(f"IRPC column has {df['IRPC'].dtypes} type")

        # TODO SettingWithCopyWarning: in Habit
        # A value is trying to be set on a copy of a slice from a DataFrame.
        # Try using .loc[row_indexer,col_indexer] = value instead
        df["IRPC"] = df["IRPC"].astype(np.int64)

        self.info["irpc_type"] = "np.int64"

        # print(f'input df shape: {df.shape}')
        return df

    def _rename_cols(self):
        prefix = self.sheet_name + "."
        columns = {
            d: prefix + d.split(".")[0]
            for d in list(self.df.columns)
            if d != "IRPC"
        }
        self.info["is_cols_renamed"] = True
        # print(f'renamed and prefix added')
        return self.df.rename(columns=columns)

    def _process_date_fields(self):
        df = self.df
        removed_cols = []
        if "InterviewDate" in self.df:
            df.drop("InterviewDate", axis=1, inplace=True)
            self.info["removed_cols"].append("InterviewDate")
        if "BirthDate" in self.df:
            df.drop("BirthDate", axis=1, inplace=True)
            self.info["removed_cols"].append("BirthDate")
        logger.info(
            f"<{self.sheet_name}> removed cols: {str(self.removed_cols)}"
        )
        return df

    def _find_duplicate_by_id(self):
        duplicated = self.df[self.df.duplicated(["IRPC"], keep=False)]

        duplicated_count = len(duplicated)
        logger.info(f"<{self.sheet_name}>: {duplicated_count} duplicated")

        self.info["duplicated_count"] = duplicated_count
        # self.info['duplicated_removed'] = True

        duplicated_removed = self.df[
            ~self.df.duplicated(["IRPC"], keep="first")
        ]
        self.df.set_index("IRPC", inplace=True)

        return self.df

    def _birth_year_calculation(self):
        if "BirthDate" in self.df.columns:
            # TODO SettingWithCopyWarning:  in Habit
            # A value is trying to be set on a copy of a slice from a DataFrame.
            # Try using .loc[row_indexer,col_indexer] = value instead
            self.df["BirthYear"] = self.df["BirthDate"].apply(extract_year)

    def _interview_date_calculation(self):
        if "InterviewDate" in self.df.columns:

            # TODO SettingWithCopyWarning: in Habit
            # A value is trying to be set on a copy of a slice from a DataFrame.
            # Try using .loc[row_indexer,col_indexer] = value instead

            self.df["InterviewDateG"] = self.df["InterviewDate"].apply(
                to_gregorian
            )

    def _age_calculation(self):
        if (
            "BirthDate" in self.df.columns
            and "InterviewDate" in self.df.columns
        ):
            self.df["AgeInStudy"] = self.df.apply(calculate_study_age, axis=1)

    def _birth_date_calculation(self):
        if "BirthDate" in self.df.columns:
            self.df["BirthDateG"] = self.df["BirthDate"].apply(to_gregorian)

    def _remove_fields(self, cols):
        for col in cols:
            if col in self.df.columns:
                self.df.drop(col, axis=1, inplace=True)
                self.info["removed_cols"].append(col)
                logger.info(f"<{self.sheet_name}> Removed column {col}")
            else:
                logger.info(f"<{self.sheet_name}> Removed column {col}")
