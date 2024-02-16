import glob
import re

import jdatetime

from vst_api import settings
from vst_api.utils.log import logger

pat = r"([^/]+).xlsx$"
pat = re.compile(pat)

pattern_camel_case = re.compile(r"(?<!^)(?=[A-Z])")


def get_filename(file):
    return pat.search(file).group(1)


def list_files():
    files = glob.glob(f"{settings.data_path}//*.xlsx")
    file_names = [get_filename(f) for f in files]
    return files, file_names


def camel_to_snake(name):
    return pattern_camel_case.sub(".", name).lower()


def extract_year(birth_date):
    try:
        age = birth_date.split("/")[0]
        return int(age)
    except Exception as e:
        logger.warning(e)
        return -1


def calculate_study_age(data):
    try:
        interview_date = data["InterviewDate"]
        birth_date = data["BirthDate"]

        birth_year = birth_date.split("/")[0]
        interview_year = interview_date.split("/")[0]

        return int(interview_year) - int(birth_year)

    except Exception as e:
        logger.warning(e)
        return -1


def to_gregorian(date):
    try:
        y = date.split("/")[0]
        m = date.split("/")[1]
        d = date.split("/")[2]
        jalali = jdatetime.date(int(y), int(m), int(d))
        return jalali.togregorian().strftime("%Y-%m-%d")
    except Exception as e:
        logger.warning(e)
        return None
