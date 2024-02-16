import random

from vst_api.indexer.Loader.SheetLoader import SheetLoader
from vst_api.utils.log import logger


def get_random_location():
    """
    For testing purposes
    """
    rand_lat = random.uniform(53.3662679102239, 54.04759000150581)
    rand_lon = random.uniform(28.297173518635304, 29.39660518147582)

    return f"POINT ({rand_lat} {rand_lon})"


points_map = {
    "ششده": (28.94863479631229, 53.99544081946746),
    "ششد": (28.94863479631229, 53.99544081946746),
    "ششده0": (28.94863479631229, 53.99544081946746),
    "نوبندگان": (28.855006543910918, 53.8261957953313),
    "فسا": (28.946287894156313, 53.643134782345584),
    "امام حسین": (28.927026131257676, 54.02933252959684),
    "امیرحاجلو": (28.95469025188683, 54.077625454762284),
    "اکبراباد": (29.245444709990508, 52.77842026507051),
    "جلیان": (28.87909928333329, 53.880170003323826),
    "قره بلاغ": (28.906012663533673, 54.13151348649026),
    "نبوبندگان": (28.854588987724853, 53.82585993339643),
    "وکیل آباد": (28.927840001205425, 54.05089281886745),
    "other": (28.77940028250383, 53.93280770581512),
}


class GeneralDataSheetLoader(SheetLoader):

    def __init__(
        self,
        file_path,
        sheet_name,
        data_types=None,
        skiprows=1,
        calculate_dates=False,
    ):
        super().__init__(
            file_path, sheet_name, data_types, skiprows, calculate_dates
        )

        self.df.loc[self.df["AddNo"] == "-", "AddNo"] = None
        self.df.loc[self.df["Mobile"] == "-", "Mobile"] = None
        self.df.loc[self.df["Phone"] == "-", "Phone"] = None
        self.df.loc[self.df["GPRSX"] == "-", "GPRSX"] = None
        self.df.loc[self.df["GPRSY"] == "-", "GPRSY"] = None

        self.df["point"] = self.df["City"].apply(self._set_point)

        self._remove_fields(
            [
                "EmailAddress",
                "BirthDateReal",
                "RelativeMobile1",
                "RelativePhone2",
                "RelativePhone1",
                "RelativeMobile2",
                "EnrollmentDate",
            ]
        )

    def _set_point(self, city):
        try:
            point = points_map[city]
        except Exception as e:
            logger.warning(f"{e} in set_point ")
            point = points_map["other"]
        return f"<{self.sheet_info.idx_name}> - POINT ({point[1]} {point[0]})"

