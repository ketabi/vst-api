import os

from Loader.GeneralSheetLoader import GeneralSheetLoader


class UsedDrugSheetLoader(GeneralSheetLoader):
    def __init__(self, data_folder="../data/csv"):
        self.file_name = "UsedDrug.csv"
        self.file_path = os.path.join(data_folder, self.file_name)

        super().__init__(
            self.file_path,
            data_types={
                # IRPC_COLUMN_NAME: np.str
            },
        )

        # self.df[IRPC_COLUMN_NAME] = self.df[IRPC_COLUMN_NAME].fillna(-1).astype(int)
        # self.df[HAS_DIABET_COLUMN_NAME] = self.df[HAS_DIABET_COLUMN_NAME].fillna(-1).astype(int)
