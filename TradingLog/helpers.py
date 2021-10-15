from datetime import datetime, timedelta
import pandas as pd

def hour_rounder(t):
    # Rounds to nearest hour by adding a timedelta hour if minute >= 30
    return (t.replace(second=0, microsecond=0, minute=0, hour=t.hour)
               +timedelta(hours=t.minute//30))

def half_hour_rounder(t):
    # Rounds to nearest half hour
    return (t.replace(second=0, microsecond=0, minute=0, hour=t.hour)
               +timedelta(minute=t.minute//30))



