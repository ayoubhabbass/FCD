import datetime
import pymongo
import folium
from stay_point import *


def points_density(collection, lon, lat, rad, datemin, datemax):

    # points à une période T
    queryT = {'$and': [{'recorded_at': {"$gt": datemin}}, {'recorded_at': {"$lt": datemax}}]}
    
    option= {'$and':[{'GPS_SPEED' :{"$ne": 0}},{'moving' : 1}]} 
    query = {'$and': [queryT, option]}
    

    #density zone Z sur période T
    pipeline = [
        {
            '$geoNear': {
                'near': { 'type': "Point", 'coordinates': [lon, lat] },
                'distanceField': "location.geo.dist",
                'maxDistance': rad*1000,
                'query': query,
                'spherical': True
            }
        },
        { 
            '$group': { 
                "_id": "$asset_id"
            } 
        },
        {
            '$group': {
                '_id': 'null',
                'density': { '$sum': 1 }
            }
        },
        { 
            '$project': { 
                '_id': 0
            } 
        }
        
    ]
    d = list(collection.aggregate(pipeline))
    
    if len(d) < 1:
        return {'density': 0}
        
    d = d[0]
    
    #points zone Z sur période T
    pipeline = [
        {
            '$geoNear': {
                'near': { 'type': "Point", 'coordinates': [lon, lat] },
                'distanceField': "location.geo.dist",
                'maxDistance': rad*1000,
                'query': query,
                'spherical': True
            }
        },
        {
            '$project': {
                '_id' : '$asset_id',
                'moving' : '$moving',
                'recorded_at' : '$recorded_at',
                'GPS_SPEED' : '$GPS_SPEED',
                'GPS_DIR' : '$GPS_DIR',
                'longitude' : { 
                    '$arrayElemAt': ['$location.geo.coordinates', 0] 
                },
                'latitude' : { 
                    '$arrayElemAt': ['$location.geo.coordinates',  1]
                }
            }
        }

    ]
    
    res = {}
    
    res['density'] = d['density']
    
    z = {}
    z['center'] = [lon, lat]
    z['radius'] = rad
    res['zone'] = z
    
    res['points'] = list(collection.aggregate(pipeline))
    
    return res
    
    
def add_average_speed(zone):
    
    s = 0
    for point in zone['points']:
        s += point['GPS_SPEED']
    
    zone['avg_speed'] = s/len(zone['points'])
        
    return zone



# Points du Top k zones les plus denses
def points_top_k_zones(collection, k, rad,datemin,datemax):
    periode ={'$and': [{'recorded_at': {"$gt": datemin}}, {'recorded_at': {"$lt": datemax}}]}
    # toutes les points de la base de données
    base = list(collection.find(periode))
    
    # tri selon la longitude puis la latitude
    base = sorted(base, key=lambda point:point['location']['geo']['coordinates'])
    print('sorting done')
    
    
    zones = []
    lon = 0
    lat = 0
    nextP = False
    
    for point in base:
        
        if distance(lat, point['location']['geo']['coordinates'][1],
                    lon, point['location']['geo']['coordinates'][0]) >= rad*1000*2:
            
            if len(zones) > 0:
                for z in zones:
                     
                    if distance(z['zone']['center'][1], point['location']['geo']['coordinates'][1],
                                z['zone']['center'][0], point['location']['geo']['coordinates'][0]) < rad*1000*2:
                        nextP = True
                        break
            if nextP:
                nextP = False
                continue
                
            lon = point['location']['geo']['coordinates'][0]
            lat = point['location']['geo']['coordinates'][1]
            
            d = points_density(collection, lon, lat, rad, datemin, datemax)
            
            if d['density'] > 0:
                zones.append(add_average_speed(d))
        
    zones = sorted(zones, key=lambda zone:zone['density'], reverse=True)
    
    if k > len(zones):
        k = len(zones)
        
    return zones[0:k]
    
    
def color_index(km):
    
    if km <= 10:
        return 8
    
    if km <= 30:
        return 7
    
    if km <= 50:
        return 6
    
    if km <= 70:
        return 5
    
    if km <= 90:
        return 4
    
    if km <= 100:
        return 3
    
    if km <= 110:
        return 2
    
    if km <= 130:
        return 1
    
    if km > 130:
        return 0
    
    
def afficher_zones_vitesse(dico):
    
    #         0bleu_foncé    1bleu   2bleu-clair  3turquoise  4vert    5vert-clair  6jaune    7orange     8rouge
    gradient = ['#0000ff', '#0080ff', '#00ffff', '#00ff80', '#00ff00', '#80ff00', '#ffff00', '#ff8000', '#ff0000']
    map = None
    
    first = 1
    num = 0
    
    #chaque zone
    for zone in dico:
        
        avg = zone['avg_speed']
        num += 1
        
        z = zone['points']
        z = sorted(z, key=lambda point:point['recorded_at'])
        time = 0
        
        # chaque point dans une zone
        for point in z:
            
            # écart de temps inférieur à 12 secondes
            if time_gap(time, point['recorded_at']) < 0.2:
                continue                        
            
            time = point['recorded_at']

            couleur = gradient[color_index(point['GPS_SPEED'])]
            
            if first == 1:
                first +=1
                map = folium.Map(location=[point['latitude'], point['longitude']])
                
            else: 
                folium.Circle(
                    radius=0.9,
                    location=[point['latitude'], point['longitude']],
                    color= couleur,
                    fill=False,
                ).add_to(map)
                
        
        folium.Circle(
            location=[zone['zone']['center'][1] , zone['zone']['center'][0]],
            radius= zone['zone']['radius']*1000,
            color='#3186cc',
            fill= False,
            popup = "Top"+ str(num)+ "\n"+ str(zone['density'])+ " voitures",
        ).add_to(map)
                            
    return map



def avg_color_index(km, avg):
    
    # 5 km/h autour de la moyenne
    if avg-5 <= km and km <= avg+5:
        return 4
    
    # 5-10 km/h au dessus
    if avg+5 < km and km <= avg+10:
        return 3
    
    # 10-20 km/h au dessus
    if avg+10 < km and km <= avg+20:
        return 2
    
    # 20-30 km/h au dessus
    if avg+20 < km and km <= avg+30:
        return 1
    
    # plus de 30 km/h au dessus
    if avg+30 < km:
        return 0
    
    # 5-10 km/h en dessous
    if avg-5 > km and km >= avg-10:
        return 5
    
    # 10-20 km/h en dessous
    if avg-10 > km and km >= avg-20:
        return 6
    
    # 20-30 km/h en dessous
    if avg-20 > km and km >= avg-30:
        return 7
    
    # plus de 30 km/h en dessous
    if avg-30 > km:
        return 1
    
    
def afficher_zones_avg_vitesse(dico):
    
    #         0bleu_foncé    1bleu   2bleu-clair  3turquoise  4vert    5vert-clair  6jaune    7orange     8rouge
    gradient = ['#0000ff', '#0080ff', '#00ffff', '#00ff80', '#00ff00', '#80ff00', '#ffff00', '#ff8000', '#ff0000']
    map = None
    
    first = 1
    num = 0
    
    #chaque zone
    for zone in dico:
        
        avg = zone['avg_speed']
        num += 1
        
        z = zone['points']
        z = sorted(z, key=lambda point:point['recorded_at'])
        time = 0
        
        # chaque point dans une zone
        for point in z:
            
            # écart de temps inférieur à 12 secondes
            if time_gap(time, point['recorded_at']) < 0.2:
                continue                        
            
            time = point['recorded_at']

            couleur = gradient[avg_color_index(point['GPS_SPEED'], avg)]
            
            if first == 1:
                first +=1
                map = folium.Map(location=[point['latitude'], point['longitude']])
                
            else: 
                folium.Circle(
                    radius=0.9,
                    location=[point['latitude'], point['longitude']],
                    color= couleur,
                    fill=False,
                ).add_to(map)
                
        
        folium.Circle(
            location=[zone['zone']['center'][1] , zone['zone']['center'][0]],
            radius= zone['zone']['radius']*1000,
            color='#3186cc',
            fill= False,
            popup = "Top"+ str(num)+ "\n"+ str(zone['density'])+ " voitures",
        ).add_to(map)
                            
    return map
