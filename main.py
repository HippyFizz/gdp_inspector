from utils import CountriesDataInspector

if __name__ == "__main__":
    inspector = CountriesDataInspector(gdp_filepath='data/gdp.csv', population_filepath='data/population.csv')
    print(inspector.latest_county_gdp('Arab World'))
