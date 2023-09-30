import requests
import config
import database as db
import pymongo
import statistics
import numpy
import time

dbname = db.get_database()

def get_history_liquidations_data(period, symbol): #get liquidation data history from coinglass API  
    url = f"https://open-api.coinglass.com/public/v2/liquidation_history?time_type={period}&symbol={symbol}"
    headers = {
        "accept": "application/json",
        "coinglassSecret": config.coinglassSecret
    }
    response = requests.get(url, headers=headers)
    selected_attributes = [{"buyVolUsd": item["buyVolUsd"], "sellVolUsd": item["sellVolUsd"], "createTime": item["createTime"]} for item in response.json()["data"]] #select only wanted data in API response 
    return selected_attributes
    
def get_last_liquidation_data(period, symbol):
    data = get_history_liquidations_data(period, symbol)
    lastData = data[-1] #select last data in API repsonse
    return lastData

def historical_send_to_db(data, symbol): #push full API response to include some history in the database #optimized
    collection_name = dbname[symbol]
    collection_name.drop()
    collection_name.insert_many(data)

def send_last_to_db(data, symbol): #push last data in database
    collection_name = dbname[symbol]
    collection_name.insert_one(data)
    
def retreive_last_data_db(collectionName): #get last data from known data in database #optimized
    collection_name = dbname[collectionName]
    last_document = collection_name.find_one(sort=[("_id", pymongo.DESCENDING)])  # Supprimer l'argument vide {} dans find_one()
    return last_document

def retreive_every_data_percentil(collectionName): #get every liquidation of database and perform percentil calculus on these datas #optimized
    collection_name = dbname[collectionName]
    data = list(collection_name.find({}, {'buyVolUsd': 1, 'sellVolUsd': 1}))
    
    buyData = [item['buyVolUsd'] for item in data]
    sellData = [item['sellVolUsd'] for item in data]
    
    percentilBuy = numpy.quantile(buyData, 0.97)
    percentilSell = numpy.quantile(sellData, 0.97)
    
    return percentilBuy, percentilSell
      
def update_data(filter, collectionName, update): #update new liquidation into known database data #optimized
    collection = dbname[collectionName]
    collection.update_one(filter, update)

def main():
    percentilBuy97ETH, percentilSell97ETH = retreive_every_data_percentil('ETHH12')
    percentilBuy97BTC, percentilSell97BTC = retreive_every_data_percentil('BTCH12')

    dataETH = get_last_liquidation_data('H12', 'ETH')
    dataBTC = get_last_liquidation_data('H12', 'BTC')

    isBuyThresholdETH = dataETH['buyVolUsd'] > percentilBuy97ETH
    isSellThresholdETH = dataETH['sellVolUsd'] > percentilSell97ETH
    isBuyThresholdBTC = dataBTC['buyVolUsd'] > percentilBuy97BTC
    isSellThresholdBTC = dataBTC['sellVolUsd'] > percentilSell97BTC
    
    isBuyThresholdETH = bool(isBuyThresholdETH)
    isSellThresholdETH = bool(isSellThresholdETH)
    isBuyThresholdBTC = bool(isBuyThresholdBTC)
    isSellThresholdBTC = bool(isSellThresholdBTC)

    percentilDataETH = {
        "percentilBuy": float(percentilBuy97ETH),
        "percentilSell": float(percentilSell97ETH),
        'isBuyThreshold': isBuyThresholdETH,
        'isSellThreshold': isSellThresholdETH
    }

    percentilDataBTC = {
        "percentilBuy": float(percentilBuy97BTC),
        "percentilSell": float(percentilSell97BTC),
        'isBuyThreshold': isBuyThresholdBTC,
        'isSellThreshold': isSellThresholdBTC
    }

    dataETH.update(percentilDataETH)
    dataBTC.update(percentilDataBTC)

    lastDocBTC = retreive_last_data_db('BTCH12')
    lastDocETH = retreive_last_data_db('ETHH12')

    if int(dataETH['createTime']) > int(lastDocETH['createTime']):
        send_last_to_db(dataETH, 'ETHH12')
    elif (
        int(dataETH['createTime']) == int(lastDocETH['createTime'])
        and (
            float(dataETH['buyVolUsd']) != float(lastDocETH['buyVolUsd'])
            or float(dataETH['sellVolUsd']) != float(lastDocETH['sellVolUsd'])
        )
    ):
        update_data(
            {"createTime": dataETH['createTime']}, 'ETHH12',
            {
                "$set": {
                    "buyVolUsd": dataETH['buyVolUsd'],
                    "sellVolUsd": dataETH['sellVolUsd'],
                    "isBuyThreshold": isBuyThresholdETH,
                    "isSellThreshold": isSellThresholdETH
                }
            }
        )

    if int(dataBTC['createTime']) > int(lastDocBTC['createTime']):
        send_last_to_db(dataBTC, 'BTCH12')
    elif (
        int(dataBTC['createTime']) == int(lastDocBTC['createTime'])
        and (
            float(dataBTC['buyVolUsd']) != float(lastDocBTC['buyVolUsd'])
            or float(dataBTC['sellVolUsd']) != float(lastDocBTC['sellVolUsd'])
        )
    ):
        update_data(
            {"createTime": dataBTC['createTime']}, 'BTCH12',
            {
                "$set": {
                    "buyVolUsd": dataBTC['buyVolUsd'],
                    "sellVolUsd": dataBTC['sellVolUsd'],
                    "isBuyThreshold": isBuyThresholdBTC,
                    "isSellThreshold": isSellThresholdBTC
                }
            }
        )
if __name__ == "__main__":
    main()