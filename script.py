import pandas

CSV_NAME = "test_data.csv"
FILTERS = {
  "min_age": 30,
  "max_age": None,
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
  min_age, max_age = FILTERS['min_age'], FILTERS['max_age']
  universe_filter = walk_universe

  if min_age is not None:
    universe_filter = walk_universe.age >= min_age

  if max_age is not None:
    universe_filter = universe_filter & (walk_universe.age <= max_age)

  return universe_filter

def __zipcode_filter(walk_universe):
  return walk_universe.mZip5.isin(FILTERS["zipcodes"])

