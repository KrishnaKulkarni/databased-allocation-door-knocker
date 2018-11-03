import pandas

CSV_NAME = "test_data.csv"
FILTERS = {
  "min_age": 30,
  "max_age": 50,
  "zipcodes": ["77584", "77479"],
}

def run():
  csv_name = CSV_NAME

  voter_df = pandas.read_csv(csv_name)
  voter_df = sanitize_walk_universe(voter_df)
  voter_df = voter_list(voter_df)

  return voter_df

def sanitize_walk_universe(walk_universe):
    cleaned_universe = walk_universe.rename(
      columns={
      'Voter File VANID': 'van_id',
      'Age': 'age', 'PrecinctName':
      'precinct'
      })

    return cleaned_universe

def voter_list(walk_universe):
    filtered_universe = walk_universe[
    age_filter(walk_universe) & \
    zipcode_filter(walk_universe)
    ]

    return filtered_universe

def age_filter(walk_universe):
  return (walk_universe.age >= FILTERS['min_age']) & \
  (walk_universe.age <= FILTERS['max_age'])

def zipcode_filter(walk_universe):
  return walk_universe.mZip5.isin(FILTERS["zipcodes"])

