# -*- coding: utf-8 -*-
"""
process_walk_universe(walk_universe) currently just takes a pandas DataFrame with columns: van_id, age, precinct
"""

import numpy as np
import pandas as pd

from datetime import datetime

def process_walk_universe(walk_universe):
    
    walk_universe = walk_universe.rename(index=str, columns={'Voter File VANID': 'van_id', 'Age': 'age', 'PrecinctName': 'precinct'})
    
    for i in range(1, len(walk_universe)):
        break
    
    return walk_universe