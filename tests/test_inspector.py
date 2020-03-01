import unittest

from utils import CountriesDataInspector


class TestInspector(unittest.TestCase):
    def setUp(self) -> None:
        self.inspector = CountriesDataInspector(
            gdp_filepath='../data/gdp.csv',
            population_filepath='../data/population.csv'
        )

    def tearDown(self) -> None:
        self.inspector = None

    def test_retrieve_none_for_doesnt_exist_county_gdp(self):
        value = self.inspector.latest_county_gdp('Any country name that doesn\'t exist')
        self.assertIsNone(value)

    def test_not_a_valid_filepath(self):
        inspector = CountriesDataInspector(
            gdp_filepath='does not exist file.ASD',
            population_filepath='../data/population.csv'
        )
        self.assertIsNone(inspector._gdp_db)
        self.assertIsNotNone(inspector._population_db)

    def test_failed_retrieve_gdp_for_county_cuz_of_no_db(self):
        inspector = CountriesDataInspector(
            gdp_filepath='does not exist file.ASD',
            population_filepath='../data/population.csv'
        )
        with self.assertRaises(AssertionError) as context:
            inspector.latest_county_gdp('Caribbean small states')
        self.assertTrue('GDP DB does not exist' in str(context.exception))

    def test_not_a_valid_extension(self):
        inspector = CountriesDataInspector(
            gdp_filepath='../data/gdp.csv',
            population_filepath='population.jpg'
        )
        self.assertIsNotNone(inspector._gdp_db)
        self.assertIsNone(inspector._population_db)

    def test_latest_county_gdp_time_execution(self):
        from time import time

        start = time()
        self.inspector.latest_county_gdp('Caribbean small states')
        end = time()
        self.assertLess(end - start, 0.001)

    def test_success_retrieve_valid_gdp_for_country(self):
        value = self.inspector.latest_county_gdp('Caribbean small states')
        self.assertEqual(value, 66707362091.378)

    def test_success_gdp_per_capita(self):
        value = self.inspector.gdp_per_capita('Central Europe and the Baltics')
        self.assertEqual(float(1312157690492.89) / float(102974082), value)

        value = self.inspector.gdp_per_capita('Euro area')
        self.assertEqual(float(11934055071906) / float(340894606), value)

    def test_success_gdp_per_capita_growth(self):
        value = self.inspector.gdp_per_capita_growth('East Asia & Pacific', 10)
        self.assertEqual(float(22480427869996.2) / float(2296786207) - float(10939731186788.5) / float(2145245494),
                         value)
