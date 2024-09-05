from pymongo import MongoClient
from datetime import datetime, timedelta
import Config
import pandas as pd



client = MongoClient(Config.mongo_host,
                     Config.mongo_port,
                     username=Config.mongo_username,
                     password=Config.mongo_password,
                     authSource="admin")



######################################################## resample_spot_data ########################################################


def resample_spot_data(time_frame_minutes, collection_name, start_date, end_date):
    
    
    
    time_frame_ms = time_frame_minutes * 60 * 1000 
    
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date) + timedelta(days=1)
    
    
    match_stage = {"$match": 
                   {"DateTime": {
                       "$gte": start_date,
                       "$lt": end_date}}}

    

    
    pipeline = [
        match_stage,
        {
         "$addFields": {
             "adjusted_timestamp": {
                 "$cond": {
                     "if": {
                         "$and": [
                             {"$gte": ["$DateTime", {"$dateFromParts": {
                                 "year": {"$year": "$DateTime"},
                                 "month": {"$month": "$DateTime"},
                                 "day": {"$dayOfMonth": "$DateTime"},
                                 "hour": 9,
                                 "minute": 15,
                                 "second": 0,
                                 "millisecond": 0
                             }}]},
                             {"$lte": ["$DateTime", {"$dateFromParts": {
                                 "year": {"$year": "$DateTime"},
                                 "month": {"$month": "$DateTime"},
                                 "day": {"$dayOfMonth": "$DateTime"},
                                 "hour": 15,
                                 "minute": 29,
                                 "second": 59,
                                 "millisecond": 0
                             }}]}
                         ]
                     },
                     "then": {
                            "$let": {
                                "vars": {
                                    "diff": {
                                        "$subtract": [
                                            {"$toLong": "$DateTime"},
                                            {"$toLong": {"$dateFromString": {"dateString": "1970-01-01T09:15:00.000Z"}}}
                                        ]
                                    }
                                },
                                "in": {
                                    "$subtract": [
                                        "$$diff",
                                        {"$mod": ["$$diff", time_frame_ms]}
                                    ]
                                }
                            }
                        },
                     "else": "$$REMOVE"
                 }
             },
            "Price": {"$toDecimal": "$Price"}
         }
     },
     {
         "$match": {
             "adjusted_timestamp": {"$exists": True}
         }
     },
     {
         "$group": {
             "_id": "$adjusted_timestamp",
             "open": {"$first": "$Price"},
             "high": {"$max": "$Price"},
             "low": {"$min": "$Price"},
             "close": {"$last": "$Price"},
             "quantity": {"$sum": "$Quantity"},
             "open_timestamp": {"$first": "$DateTime"},
             "close_timestamp": {"$last": "$DateTime"}
         }
     },
     {
         "$sort": {"_id": 1}
     },
     {
        "$unset": "_id"  # Remove the _id field
    }]
    
    collection = db[collection_name]
    result = list(collection.aggregate(pipeline))
    df = pd.DataFrame(result)
    
    df['open'] = df['open'].astype(str).apply(lambda x: "{:.2f}".format(float(x)))
    df['high'] = df['high'].astype(str).apply(lambda x: "{:.2f}".format(float(x)))
    df['low'] = df['low'].astype(str).apply(lambda x: "{:.2f}".format(float(x)))
    df['close'] = df['close'].astype(str).apply(lambda x: "{:.2f}".format(float(x)))
    df['open'] = df['open'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['close'] = df['close'].astype(float)
    
    # result = df.to_dict(orient='records')


    return df




'''
Example USE
This file can be used for directly getting resampled data to a Dataframe


db = client["Index_Spot"]
collection_name = "NIFTY_SPOT"

'''
db = client["Equity_Spot"]
collection_name = "LT_SPOT"



time_frame_minutes = 15
start_date = "2018-01-01"
end_date = "2024-02-29"

st = datetime.now()
resampled_data = resample_spot_data(time_frame_minutes, collection_name, start_date, end_date)
print(datetime.now() - st)
resampled_data.to_csv('LT_spot_data.csv')


######################################################## resample_fut_data ########################################################

def resample_fut_data(time_frame_minutes, collection_name, start_date, end_date, contract_month=None, expiry_date=None):
    
    
    
    time_frame_ms = time_frame_minutes * 60 * 1000 
    
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date) + timedelta(days=1)
    
    
    match_stage = {"$match": 
                   {"DateTime": {
                       "$gte": start_date,
                       "$lt": end_date}}}

    if contract_month is not None:
        match_stage["$match"]["Contract_Month"] = contract_month
        
    if expiry_date is not None:
        match_stage["$match"]["Expiry"] = pd.to_datetime(expiry_date)
    

    
    pipeline = [
        match_stage,
        {
         "$addFields": {
             "adjusted_timestamp": {
                 "$cond": {
                     "if": {
                         "$and": [
                             {"$gte": ["$DateTime", {"$dateFromParts": {
                                 "year": {"$year": "$DateTime"},
                                 "month": {"$month": "$DateTime"},
                                 "day": {"$dayOfMonth": "$DateTime"},
                                 "hour": 9,
                                 "minute": 15,
                                 "second": 0,
                                 "millisecond": 0
                             }}]},
                             {"$lte": ["$DateTime", {"$dateFromParts": {
                                 "year": {"$year": "$DateTime"},
                                 "month": {"$month": "$DateTime"},
                                 "day": {"$dayOfMonth": "$DateTime"},
                                 "hour": 15,
                                 "minute": 29,
                                 "second": 59,
                                 "millisecond": 0
                             }}]}
                         ]
                     },
                     "then": {
                            "$let": {
                                "vars": {
                                    "diff": {
                                        "$subtract": [
                                            {"$toLong": "$DateTime"},
                                            {"$toLong": {"$dateFromString": {"dateString": "1970-01-01T09:15:00.000Z"}}}
                                        ]
                                    }
                                },
                                "in": {
                                    "$subtract": [
                                        "$$diff",
                                        {"$mod": ["$$diff", time_frame_ms]}
                                    ]
                                }
                            }
                        },
                     "else": "$$REMOVE"
                 }
             },
            "Price": {"$toDecimal": "$Price"}
         }
     },
     {
         "$match": {
             "adjusted_timestamp": {"$exists": True}
         }
     },
     {
         "$group": {
             "_id": "$adjusted_timestamp",
             "Ticker": {"$first": "$Ticker"},
             "open": {"$first": "$Price"},
             "high": {"$max": "$Price"},
             "low": {"$min": "$Price"},
             "close": {"$last": "$Price"},
             "quantity": {"$sum": "$Quantity"},
             "OI_first": {"$first": "$OI"},
             "OI_last": {"$last": "$OI"},
             "Expiry": {"$last": "$Expiry"},
             "open_timestamp": {"$first": "$DateTime"},
             "close_timestamp": {"$last": "$DateTime"}
         }
     },
     {
         "$sort": {"_id": 1}
     },
     {
        "$unset": "_id"  # Remove the _id field
    }]
    
    collection = db[collection_name]
    result = list(collection.aggregate(pipeline))
    df = pd.DataFrame(result)
    
    df['open'] = df['open'].astype(str).apply(lambda x: "{:.2f}".format(float(x)))
    df['high'] = df['high'].astype(str).apply(lambda x: "{:.2f}".format(float(x)))
    df['low'] = df['low'].astype(str).apply(lambda x: "{:.2f}".format(float(x)))
    df['close'] = df['close'].astype(str).apply(lambda x: "{:.2f}".format(float(x)))
    df['open'] = df['open'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['close'] = df['close'].astype(float)
    
    # result = df.to_dict(orient='records')


    return df




'''
Example USE
This file can be used for directly getting resampled data to a Dataframe

contract_month should be 1 for CurrentMonth / Continuous Futures Data




db = client["Index_Futures"]
collection_name = "NIFTY_FUT"


db = client["Equity_Futures"]
collection_name = "LT_FUT"


time_frame_minutes = 15
start_date = "2018-01-01"
end_date = "2024-02-29"
contract_month = 1

# expiry_date = "2019-01-31T00:00:00.000+00:00"
st = datetime.now()
resampled_data = resample_fut_data(time_frame_minutes, collection_name, start_date, end_date, contract_month)
print(datetime.now() - st)
resampled_data.to_csv('LT_fut_data.csv')

'''
################################################## full spot data ############################################################


def fetch_all_data_from_mongodb( database_name, collection_name):
   
    # Connect to MongoDB
    
    # Select the database
    db = client[database_name]
    
    # Select the collection
    collection = db[collection_name]
    
    # Fetch all data
    all_data = list(collection.find())
    
    # Close the connection
    client.close()
    pd.DataFrame(all_data).to_csv(f'{collection_name}.csv')
    

'''
fetch_all_data_from_mongodb( "Index_Spot", "BANKNIFTY_SPOT")

'''





