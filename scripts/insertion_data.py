import csv
from stay_point import *

import pymongo
from pymongo import GEOSPHERE


# Création d'un dictionnaire à partir de fichiers csv



def filename_from_path(file):
    
    begin = len(file)-1
    
    while file[begin] != ".":
        begin -= 1
        
    end = begin
    
    while file[begin] != "\\":
          begin -= 1
          
    begin += 1
    return file[begin:end]
    
    
    
def csv_file_reader(paths, i):

    data = [] 
    name = filename_from_path(paths[i])

    with open(paths[i], newline='') as csvfile:

        reader = csv.DictReader(csvfile)
        print('file', name, '...')

        for row in reader:
            point = {}      
            point['asset_id'] = name
            point['recorded_at'] = create_timestamp(row['recorded_at'])        

            g = {}
            coord = []
            coord.append(float(row['longitude']))
            coord.append(float(row['latitude']))
            g['coordinates'] = coord
            g['type'] = "Point"

            loc = {}
            loc['geo'] = g
            point['location'] = loc
            point['moving'] = 1  # en mouvement par défaut

            direction = 0.0
            speed = 0.0

            if row['GPS_DIR'] != '':
                direction = float(row['GPS_DIR'])

            if row['GPS_SPEED'] != '':
                speed = float(row['GPS_SPEED'])

            point['GPS_DIR'] = direction
            point['GPS_SPEED'] = speed

            data.append(point)
    
    return data



# Ajout de l'index géo spatial
def add_2dsphere_index(collection, field):
    
    resp = collection.create_index([(field, GEOSPHERE)])
    print ("index response:", resp)
    
    
# Importation du dictionnaire dans la collection 'assets'
def insert_into_database(collection, data):
    
    #print('insertion...')
    collection.insert_many(data)
    #print('insertion done')
    
    
# Modification de l'attribut moving dans 'fcd'
def set_stayPoints_database(SP, asset_points, collection):
    
    for stayP in SP:

        for i in range(stayP['arv']+1, stayP['lev']):

            filtre = {'_id': asset_points[i]['_id']}
            collection.update_one(filtre, {"$set": {"moving": 0}})
            
            
def research_stayPoints(data, collection):

    # tri chronologique
    data = sorted(data, key=lambda point: point['recorded_at'])

    #print("stay points research...")

    # detection des stay points
    SP = algorithm_stayPoint_detection(data, 200, 30)

    # mofication de la base de données
    set_stayPoints_database(SP, data, collection)

    #print(count_SP, 'stay points detected')
    
    
def insert_data(paths, col, collection):
    
    
    for i in range(0, len(paths) ):
        
        # lecture du fichier 'i' csv du répertoire
        dico = csv_file_reader(paths, i)
    
        # insertion dans la collection
        insert_into_database(collection, dico)

        # modif du champs 'moving' dans la base de données
        research_stayPoints(dico, collection)
        
        print('file', filename_from_path(paths[i]), 'done,',i+1, 'assets added');
        
    
    print('\nall done')
    
