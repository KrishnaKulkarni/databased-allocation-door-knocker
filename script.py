import pandas
import datetime

INPUT_CSV_NAME = "sample_data2.csv"
PRECINCT_DATA_CSV_NAME = "sample_precinct_data.csv"
OUTPUT_VAN_IDS_CSV = "van_ids.csv"
OUTPUT_PRECINCT_COUNTS_CSV = "precinct_counts.csv"

VAN_LABELS_TO_OUR_LABELS = {
  'Voter File VANID': 'van_id',
  'Age': 'age',
  'PrecinctName':'precinct',
  "DateReg": "date_registered",
}

SELECTED_COMMUNITY_GROUPS = None

IS_KULKARNI_COMMUNITY_LABELS = [
  # "Arab_Christian_(Public)",
  "Bangladeshi_(Public)",
  "Bengali_(Sri_Preston_Kulkarni)",
  "Cantonese_(Public)",
  "Chinese_(Public)",
  # "Family_(Sri_Preston_Kulkarni)",
  # "Filipino_(Public)",
  "Gujarati_(Sri_Preston_Kulkarni)",
  "Hindi_(Sri_Preston_Kulkarni)",
  "Igbo_(Public)",
  "Ismaili_(Public)",
  "Kannada_(Sri_Preston_Kulkarni)",
  "Konkani_(Public)",
  "Malayalam_(Sri_Preston_Kulkarni)",
  "Marati_(Sri_Preston_Kulkarni)",
  # "Mom_(Public)",
  "Nepalese_(Public)",
  # "Polish_(Public)",
  # "Portuguese_(Public)",
  "Punjabi_(Sri_Preston_Kulkarni)",
  # "Registered_Nurse_(Public)",
  "Sindhi_(Public)",
  "Taiwanese_(Public)",
  "Tamil_(Sri_Preston_Kulkarni)",
  "Telugu_(Sri_Preston_Kulkarni)",
  "Turkish_(Public)",
  # "Vietnamese_(Public)",
  "Yoruba_(Public)",
  "Zoroastrian_(Public)",
  "DESI_(Public)",
  # "East_Asian_(Public)",
  "Hindu_(Public)",
  "Muslim_(Public)",
  # "Nigerian_(Public)",
  "NIGERIAN_(Public)",
  # "Oct24_(Public)",
  "S._Indian_Christian_(Public)",
  # "MuslimAll_(Public)",
]

def run():
  csv_name = INPUT_CSV_NAME

  voter_df = pandas.read_csv(csv_name)
  precinct_df = pandas.read_csv(PRECINCT_DATA_CSV_NAME)[["Precinct", "2016 % Turnout", "%H"]]
  voter_df = voter_df.merge(precinct_df, left_on="PrecinctName", right_on="Precinct", how="left", indicator=True)

  voter_df = voter_list(voter_df)
  precincts = precinct_counts(voter_df)

  __write_csvs(voter_df, precincts)
  return [voter_df, precincts]

def voter_list(walk_universe):
  sanitized_universe = __sanitize_walk_universe(walk_universe)
  augmented_universe = __augment_walk_universe(sanitized_universe)

  s1 = Search(augmented_universe, {
    "min_age": None,
    "max_age": None,
    "zipcodes": None,
    "is_kulkarni_community": True,
    "registered_after": None,
    "community_groups": None,
    "excluded_precincts": None,
    "CivRace": None,
  })
  s2 = Search(augmented_universe, {
    "min_age": None,
    "max_age": None,
    "zipcodes": None,
    "is_kulkarni_community": False,
    "registered_after": None,
    "community_groups": None,
    "excluded_precincts": ["36", "2157"],
    "CivRace": ["Black-Low"],
  })
  union = s1.intersection() | s2.intersection()
  filtered_universe = augmented_universe[union]

  return filtered_universe
  # return filtered_universe[
  #   ["van_id", "precinct", "is_kulkarni_community",
  #   "is_selected_community", "date_registered"]
  # ]

def precinct_counts(voter_df):
  kulkarni_voter_df = voter_df[voter_df.is_kulkarni_community]

  return voter_df.groupby(['precinct']).agg(['count'])[['van_id']]

def __sanitize_walk_universe(walk_universe):
  cleaned_universe = walk_universe.rename(columns=lambda x: x.strip())
  cleaned_universe = cleaned_universe.rename(columns=VAN_LABELS_TO_OUR_LABELS)
  cleaned_universe["date_registered"] = pandas.to_datetime(
    cleaned_universe["date_registered"]
  )

  return cleaned_universe

def __augment_walk_universe(walk_universe):
  walk_universe["is_kulkarni_community"] = walk_universe.apply(__is_kulkarni_community, axis=1)
  walk_universe["is_selected_community"] = walk_universe.apply(__is_selected_community, axis=1)

  return walk_universe

def __is_kulkarni_community(row):
  return __marked_for_at_least_one_column(row, IS_KULKARNI_COMMUNITY_LABELS)

def __is_selected_community(row):
  community_groups = SELECTED_COMMUNITY_GROUPS

  if community_groups is not None:
    return __marked_for_at_least_one_column(row, community_groups)
  else:
    return False

def __marked_for_at_least_one_column(row, labels):
  for header in labels:
    if __is_present_string(row[header]):
      return True

  return False

def __is_present_string(value):
  return isinstance(value, str) & bool(value)

def __write_csvs(voter_ids_df, voter_counts):
  voter_ids_df.to_csv(OUTPUT_VAN_IDS_CSV)
  voter_counts.to_csv(OUTPUT_PRECINCT_COUNTS_CSV)

class Search:
  def __init__(self, walk_universe, filters):
    self.walk_universe = walk_universe
    self.filters = filters

  def intersection(self):
    return self.__age_filter() & \
      self.__zipcode_filter() & \
      self.__is_kulkarni_filter() & \
      self.__registered_date_filter() & \
      self.__community_group_filter() & \
      self.__excluded_precinct_filter() & \
      self.__civrace_filter()

  def __noop_filter(self):
    return self.walk_universe.van_id > 0

  def __age_filter(self):
    min_age, max_age = self.filters['min_age'], self.filters['max_age']
    universe_filter = self.__noop_filter()

    if min_age is not None:
      universe_filter = self.walk_universe.age >= min_age

    if max_age is not None:
      universe_filter = universe_filter & (self.walk_universe.age <= max_age)

    return universe_filter

  def __zipcode_filter(self):
    zipcodes = self.filters["zipcodes"]

    if zipcodes is not None:
      return self.walk_universe.mZip5.isin(zipcodes)
    else:
      return self.__noop_filter()

  def __is_kulkarni_filter(self):
    if self.filters["is_kulkarni_community"]:
      return self.walk_universe.is_kulkarni_community
    else:
      return self.__noop_filter()

  def __registered_date_filter(self):
    days_to_go_back = self.filters["registered_after"]

    if days_to_go_back is not None:
      return self.walk_universe.date_registered > self.__date_days_ago(days_to_go_back)
    else:
      return self.__noop_filter()

  def __date_days_ago(self, days_ago):
    return datetime.datetime.now() - datetime.timedelta(days=days_ago)

  def __community_group_filter(self):
    community_groups = self.filters["community_groups"]

    if community_groups is not None:
      return self.walk_universe.is_selected_community
    else:
      return self.__noop_filter()

  def __excluded_precinct_filter(self):
    precincts = self.filters["excluded_precincts"]

    if precincts is not None:
      return ~self.walk_universe.precinct.isin(precincts)
    else:
      return self.__noop_filter()

  def __civrace_filter(self):
    civ_races = self.filters["CivRace"]

    if civ_races is not None:
      return self.walk_universe.CivRace.isin(civ_races)
    else:
      return self.__noop_filter()
