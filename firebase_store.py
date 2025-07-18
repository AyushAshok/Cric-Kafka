import json
from datetime import datetime
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db

cred = credentials.Certificate("crick-kafka-firebase.json")
firebase_admin.initialize_app(cred,{
    'databaseURL':'https://crick-kafka-default-rtdb.firebaseio.com/'
})

def push_firebase_database(topics):
    ref=db.reference('Math-Errors')
    data=topics

    for key,value in data.items():
        for msg in value:
            try:
                decoded=json.loads(msg)
                if decoded.get('event')=='ERROR':
                    ref.child(key).push(decoded)
            

            except json.JSONDecodeError:
                continue

    print(f"[Firebase] Pushed {len(data)} topic(s) at {datetime.now().strftime('%d %m %Y %H:%M:%S')}")


