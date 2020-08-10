import csv
from datetime import datetime
import os
import glob
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
    
    
def csv_files_reader(directory):
    
    notebook_path = os.path.abspath("Notebook.ipynb")
    chemin = os.path.join(os.path.dirname(notebook_path), directory)

    paths = glob.glob(chemin+'/*')

    data = []

    for i in range(0, 100):

        name = filename_from_path(paths[i])

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
    
    
def insert_data(directory, base, col):
    
    client = pymongo.MongoClient('localhost',27017)
    mydb = client[base]
    collection = mydb[col]
    
    # lecture fichiers csv du répertoire
    dico = csv_files_reader(directory)

    # insertion dans la collection
    insert_into_database(collection, dico)

    # modif du champs 'moving' dans la base de données
    research_stayPoints(dico, collection)
    
    # mettre à jour la date de mofication de la collection 'assets'
    update_last_modification_date(client, base, col)
    
    print('\ndone')
    
