from time_distance import *

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
