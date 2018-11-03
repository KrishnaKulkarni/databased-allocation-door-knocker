# -*- coding: utf-8 -*-
"""
Created on Fri Nov  2 21:37:10 2018

list takes arguments:
    walk_universe (a pandas Dataframe of invidivual voters)
    age (int) range: [min_age, max_age]
   # precinct (int)
"""

import numpy as np
import pandas as pd

from datetime import datetime

def voter_list(walk_universe, age_range):
    
    min_age = age_range[0]
    max_age = age_range[1]
    
    walk_universe = walk_universe[(walk_universe.age >= min_age) & (walk_universe.age <= max_age)]
    
    return walk_universe
