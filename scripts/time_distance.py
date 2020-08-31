import math
from datetime import datetime


def create_timestamp(date):
    dt = datetime(int(date[0:4]), int(date[5:7]), int(date[8:10]), 
                   int(date[11:13]), int(date[14:16]), int(date[17:19]))
    
    dt = datetime.timestamp(dt)
    
    return dt


# in minutes
def time_gap(current, new):
    
    td = abs(current - new)
    res = td/60
    return res


# in meters
def distance(lat1, lat2, lon1, lon2):
    
    R = 6372800  # Earth radius in meters
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    d = 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return d