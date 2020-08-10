import math


# Application de l'algorithme des stay points

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


def algorithm_stayPoint_detection(P, distThreh, timeThreh):

    i = 0
    pointNum = len(P)
    SP = []

    while i < pointNum-1:
        
        j = i + 1
        
        while j < pointNum:
            dist = distance(float(P[i]['location']['geo']['coordinates'][1]),
                            float(P[j]['location']['geo']['coordinates'][1]),
                            float(P[i]['location']['geo']['coordinates'][0]),
                            float(P[j]['location']['geo']['coordinates'][0]))

            if dist > distThreh:
                break
                
            j+=1
        
        j-=1
        
        deltaT = time_gap(P[i]['recorded_at'], P[j]['recorded_at'])
        if deltaT > timeThreh:
            S = {}
            S['arv'] = i
            S['lev'] = j
            SP.append(S)
            i=j
        else:
            i+=1
     
    return SP
