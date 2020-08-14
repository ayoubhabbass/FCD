import csv
from stay_point import *
from update_dates import *
from datetime import datetime
import pymongo
from pymongo import GEOSPHERE


# Création d'un dictionnaire à partir de fichiers csv

def create_timestamp(date):
    dt = datetime(int(date[0:4]), int(date[5:7]), int(date[8:10]), 
                   int(date[11:13]), int(date[14:16]), int(date[17:19]))
    
    dt = datetime.timestamp(dt)
    return dt


def filename_from_path(file):
    
    begin = len(file)-1
    
    while file[begin] != ".":
        begin -= 1
        
    end = begin
    
    while file[begin] != "\\":
          begin -= 1
          
    begin += 1
    return file[begin:end]
    
    
def csv_files_reader(paths):

    data = []

    print('reading...')
    
    for i in range (0, 300):

        name = filename_from_path(paths[i])
        
        if name[0] != '3':
            i -= 1
            continue

        with open(paths[i], newline='') as csvfile:
            
            reader = csv.DictReader(csvfile)

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
    
    
    print('reading done')
    return data



# Ajout de l'index géo spatial
def add_2dsphere_index(collection, field):
    
    resp = collection.create_index([(field, GEOSPHERE)])
    print ("index response:", resp)
    
    
# Importation du dictionnaire dans la collection 'assets'
def insert_into_database(collection, data):
    
    print('insertion...')
    collection.insert_many(data)
    print('insertion done')
    
    
# Modification de l'attribut moving dans 'fcd'
def set_stayPoints_database(SP, asset_points, collection):
    
    for stayP in SP:

        for i in range(stayP['arv']+1, stayP['lev']):

            filtre = {'_id': asset_points[i]['_id']}
            collection.update_one(filtre, {"$set": {"moving": 0}})
            
            
def research_stayPoints(data, collection):

    # tri par asset
    
    data = sorted(data, key=lambda point: point['asset_id'])
    current_id = data[0]['asset_id']

    asset_points = []
    count_SP = 0

    print("stay points research...")
    for point in data:

        if point['asset_id'] == current_id:
            asset_points.append(point)
        
        else:
            # tri chronologique           
            asset_points = sorted(asset_points, key=lambda point: point['recorded_at'])
            # detection des stay points
            SP = algorithm_stayPoint_detection(asset_points, 200, 30)
            count_SP += len(SP)

            # mofication de la base de données
            set_stayPoints_database(SP, asset_points, collection)

            #reinitialisation
            current_id = point['asset_id']
            asset_points = []
            asset_points.append(point)

            
    #----- pour le dernier asset

    # tri chronologique           
    asset_points = sorted(asset_points, key=lambda point: point['recorded_at'])
    # detection des stay points
    SP = algorithm_stayPoint_detection(asset_points, 200, 30)
    count_SP += len(SP)
    # mofication de la base de données
    set_stayPoints_database(SP, asset_points, collection)

    print(count_SP, 'stay points detected')
    
    
def insert_data(paths, col, collection):
    
    # lecture fichiers csv du répertoire
    dico = csv_files_reader(paths)

    # insertion dans la collection
    insert_into_database(collection, dico)

    # modif du champs 'moving' dans la base de données
    research_stayPoints(dico, collection)
    
    print('\ndone')
    
