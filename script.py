import pandas

CSV_NAME = "test_data.csv"
FILTERS = {
  "min_age": 30,
  "max_age": 70,
  "zipcodes": ["77459", "77581", "77584", "77584"],
}

def run():
  csv_name = CSV_NAME

  voter_df = pandas.read_csv(csv_name)
  voter_df = voter_list(voter_df)
  precincts = precinct_counts(voter_df)

  return [voter_df, precincts]

def precinct_counts(voter_df):
  return voter_df.groupby(['precinct']).agg(['count'])

def voter_list(walk_universe):
  sanitized_universe = __sanitize_walk_universe(walk_universe)

  filtered_universe = sanitized_universe[
  __age_filter(sanitized_universe) & \
  __zipcode_filter(sanitized_universe)
  ]

  return filtered_universe

def __sanitize_walk_universe(walk_universe):
  cleaned_universe = walk_universe.rename(
    columns={
    'Voter File VANID': 'van_id',
    'Age': 'age',
    'PrecinctName':'precinct',
    })

  return cleaned_universe

def __age_filter(walk_universe):
  return (walk_universe.age >= FILTERS['min_age']) & \
  (walk_universe.age <= FILTERS['max_age'])

def __zipcode_filter(walk_universe):
  return walk_universe.mZip5.isin(FILTERS["zipcodes"])

