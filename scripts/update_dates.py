# Mise à jour de la collection 'update_dates'

def insert_doc_into_update_dates(collection, date, col):
    
    data = [{ 'col': col, 'last_modification': date}]
    collection.insert_many(data)

    
def update_last_modification_date(client, base, col):
    
    mydb = client[base]
    collection = mydb["update_dates"]
    now = datetime.now()
    
    if len(list(collection.find({'col': col}))) == 0:
        insert_doc_to_modification_dates(collection, now, col)
    else:
        filtre = {'col': col}
        collection.update_one(filtre, {"$set": {"last_modification": now}})
        
    print('update done')