import pandas

INPUT_CSV_NAME = "test_data.csv"
OUTPUT_VAN_IDS_CSV = "van_ids.csv"
OUTPUT_PRECINCT_COUNTS_CSV = "precinct_counts.csv"

VAN_LABELS_TO_OUR_LABELS = {
  'Voter File VANID': 'van_id',
  'Age': 'age',
  'PrecinctName':'precinct',
}

FILTERS = {
  "min_age": None,
  "max_age": None,
  "zipcodes": None,
}

IS_KULKARNI_EXPANSION_LABELS = [
  "Tamil_(Sri_Preston_Kulkarni)",
  "Vietnamese_(Public)",
]

def run():
  csv_name = INPUT_CSV_NAME

  voter_df = pandas.read_csv(csv_name)
  voter_df = voter_list(voter_df)
  precincts = precinct_counts(voter_df)

  __write_csvs(voter_df, precincts)
  return [voter_df, precincts]

def precinct_counts(voter_df):
  return voter_df.groupby(['precinct']).agg(['count'])[['van_id']]

def voter_list(walk_universe):
  sanitized_universe = __sanitize_walk_universe(walk_universe)
  augmented_universe = __augment_walk_universe(sanitized_universe)

  filtered_universe = sanitized_universe[
  __age_filter(sanitized_universe) & \
  __zipcode_filter(sanitized_universe)
  ]

  return filtered_universe[["van_id", "precinct", "is_kulkarni_expansion"]]

def __sanitize_walk_universe(walk_universe):
  cleaned_universe = walk_universe.rename(columns=VAN_LABELS_TO_OUR_LABELS)

  return cleaned_universe

def __augment_walk_universe(walk_universe):
  walk_universe["is_kulkarni_expansion"] = walk_universe.apply(__is_kulkarni_expansion, axis=1)

  return walk_universe

def __is_kulkarni_expansion(row):
  for header in IS_KULKARNI_EXPANSION_LABELS:
    if __is_present_string(row[header]):
      return True

  return False

def __is_present_string(value):
  return isinstance(value, str) & bool(value)

def __age_filter(walk_universe):
  min_age, max_age = FILTERS['min_age'], FILTERS['max_age']
  universe_filter = __noop_filter(walk_universe)

  if min_age is not None:
    universe_filter = walk_universe.age >= min_age

  if max_age is not None:
    universe_filter = universe_filter & (walk_universe.age <= max_age)

  return universe_filter

def __zipcode_filter(walk_universe):
  zipcodes = FILTERS["zipcodes"]

  if zipcodes is not None:
    return walk_universe.mZip5.isin(zipcodes)
  else:
    return __noop_filter(walk_universe)

def __noop_filter(walk_universe):
  return walk_universe.van_id > 0

def __write_csvs(voter_ids_df, voter_counts):
  voter_ids_df.to_csv(OUTPUT_VAN_IDS_CSV)
  voter_counts.to_csv(OUTPUT_PRECINCT_COUNTS_CSV)

