import hashlib
import logging
from os import path
from typing import List, Optional

from inspector_types import DbFormat, RowType


class CountriesDataInspector:
    def __init__(self, gdp_filepath: str, population_filepath: str):
        """
        :param gdp_filepath: str path on the system to gdp file location
        :param population_filepath: str path on the system to population file location
        """
        self._logger = logging.getLogger(__name__)
        self._gdp_db = self._parse_csv(gdp_filepath, [0])
        self._population_db = self._parse_csv(population_filepath, [0, 2])

    def _parse_csv(self, filename: str, hash_fields_indexes: Optional[List[int]]) -> DbFormat:
        """
        :param filename: str path on the system
        :param hash_fields_indexes: which values from db must be used as params for hashing process
        :return: Optional[Dict[str, Union[List[RowType], RowType]]]
        """
        if not path.exists(filename) or not path.isfile(filename) or not filename.endswith('.csv'):
            self._logger.error('Filepath doesn\'t exist or wrong file extension.')
            return None
        with open(filename) as csv_file:
            lines = [line.rstrip() for line in csv_file]

        db: DbFormat = {}
        keys: List[str] = []

        def _append_obj_to_db(values: List[str]):
            """
            :param values: List of strings that parsed from the file ex ['Arab World','ARB','1968','25760683041.0857']
            :return: None
            """
            assert len(values) > 0
            key = self._hash_value(''.join(map(lambda index: values[index], hash_fields_indexes)))
            obj = dict(zip(keys, values))
            if len(hash_fields_indexes) > 1:  # if more than one key is used for hashing collision chance is decreased
                db[key] = obj  # so we may handle key: obj
            elif db.get(key, []):  # if just one key is used for hashing collision rate is high are we have
                db[key].append(obj)  # store data ib our DB as key: List[obj]
            else:
                db[key] = [obj]

        for line in lines:
            # ======THIS=BLOCK=IS=USED=TO=AVOID=TROUBLES=WITH=PARSING=Country=Name=WITH="===============================
            county_name: str = ''
            if line.startswith('\"'):
                for letter in line:
                    if letter == '"' and not len(county_name):
                        continue
                    elif letter != '\"':
                        county_name += letter
                        continue
                    break
            if county_name:
                line = line[len(county_name) + 2:]
            words = line.split(',')
            if county_name:
                words[0] = county_name
            # ==========================================================================================================
            if not keys:
                keys = list(map(lambda word: word, words))
                continue

            assert len(words) == len(keys)
            _append_obj_to_db(words)
        # if keys are not list let's order them by year field for future easy usage
        if len(hash_fields_indexes) == 1:
            for key in db:
                db[key].sort(key=lambda value: value['Year'], reverse=True)
        return db

    def _order_db_values_by_key(self, values: List[RowType], key: str, reverse: bool = True):
        """
        :param values: List[RowType] DB rows from the single hashing key
        :param key: str What field should be used for sorting ex Country Name,Country Code,Year,Value
        :param reverse: bool sorting order
        :return:
        """
        values.sort(key=lambda value: value[key], reverse=reverse)

    def _hash_value(self, value: str) -> str:
        """
        :param value: value that must be hashed
        :return: hashstring created by default python lib hashlib
        """
        return hashlib.sha1(value.encode('utf-8')).hexdigest()

    def latest_county_gdp(self, county_name: str) -> Optional[float]:
        """
        :param county_name: string describing Country Name in database
        """
        assert self._gdp_db is not None, 'GDP DB does not exist'
        values = self._gdp_db.get(self._hash_value(county_name))
        if not values:
            return None
        self._order_db_values_by_key(values, 'Year')
        return float(values.pop(0).get('Value', 0))

    def _calculate_gdc_per_capita_result(self, county_name: str,
                                         gdp_values: RowType,
                                         last_period_year: int = 0) -> (Optional[float], int):
        """
        :param county_name: string describing Country Name in database
        :param gdp_values:  List[RowType] returned from DB for current string
        :param last_period_year: in what year searching period ends
        """
        for gdp_value in gdp_values:
            gdp_year = int(gdp_value.get('Year', 0))
            if not gdp_year or (last_period_year and not gdp_year == last_period_year):
                continue
            population_value = self._population_db.get(self._hash_value(f"{county_name}{gdp_year}"))
            if not population_value:
                continue
            if gdp_value.get('Value', 0) and population_value.get('Value', 0):
                return float(gdp_value.get('Value')) / float(population_value.get('Value')), gdp_year

    def _calculate_country_gdp_per_capita(self, county_name: str, years_amount: int = 0) -> Optional[float]:
        """
        :param county_name: string describing Country Name in database
        :param years_amount: period to calculate gdp per capita growth, if pass 0 works as self.gdp_per_capita
        :return: float if data for current country exists on database else None returned
        """
        country_hash = self._hash_value(county_name)
        assert self._gdp_db is not None, 'GDP DB does not exist'
        gdp_values = self._gdp_db.get(country_hash)
        assert gdp_values is not None, f'{county_name} does not exist in GDP DB'
        self._order_db_values_by_key(gdp_values, 'Year')
        start_period_result, start_period_year = self._calculate_gdc_per_capita_result(
            county_name,
            gdp_values
        )
        if not years_amount:
            return start_period_result
        last_period_result, last_period_year = self._calculate_gdc_per_capita_result(
            county_name,
            gdp_values,
            start_period_year - years_amount
        )
        return start_period_result - last_period_result

    def gdp_per_capita(self, county_name: str) -> Optional[float]:
        """
        :param county_name: string describing Country Name in database
        :return: float if data for current country exists on database else None returned
        """
        return self._calculate_country_gdp_per_capita(county_name)

    def gdp_per_capita_growth(self, county_name: str, years_amount: int) -> Optional[float]:
        """
        :param county_name: string describing Country Name in database
        :param years_amount: period to calculate gdp per capita growth, if pass 0 works as self.gdp_per_capita
        :return: float if data for current country exists on database else None returned
        """
        assert years_amount > 0, 'years_amount must be positive integer'
        return self._calculate_country_gdp_per_capita(county_name, years_amount)
