import requests
import config
import database as db
import pymongo
import statistics


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

def historical_send_to_db(data, symbol): #push full API response to include some history in the database
    dbname = db.get_database()
    collection_name = dbname[symbol]
    collection_name.drop()
    for item in data:
        collection_name.insert_one(item)

def send_last_to_db(data, symbol): #push last data in database
    dbname = db.get_database()
    collection_name = dbname[symbol]
    collection_name.insert_one(data)
    
def retreive_last_data_db(collectionName):
    dbname = db.get_database()
    collection_name = dbname[collectionName]
    last_document = collection_name.find_one({}, sort=[("_id", pymongo.DESCENDING)])
    return last_document

def retreive_x_last_data_stdev(collectionName, xNumbers):
    dbname = db.get_database()
    collectionName = dbname[collectionName]
    last_documents = collectionName.find({}).sort([("_id", pymongo.DESCENDING)]).limit(xNumbers)
    data = list(last_documents)
    buyData = [item['buyVolUsd'] for item in data]
    sellData = [item['sellVolUsd'] for item in data]
    
    stdevBuy = statistics.stdev(buyData)
    stdevSell = statistics.stdev(sellData)
    return stdevBuy, stdevSell


if __name__ == "__main__":
    # historicalData = get_history_liquidations_data('M5', 'ETH')   
    # historical_send_to_db(historicalData, 'ETHM5')
    # historicalData = get_history_liquidations_data('M5', 'BTC')   
    # historical_send_to_db(historicalData, 'BTCM5')
    
    stdevBuy500, stdevSell500 = retreive_x_last_data_stdev('ETHM5', 500)
    stdevBuy200, stdevSell200 = retreive_x_last_data_stdev('ETHM5', 200)
    stdevDataETH = {"stdevBuy500":float(stdevBuy500), "stdevBuy200":float(stdevBuy200), "stdevSell500":float(stdevSell500), "stdevSell200":float(stdevSell200)}
    
    stdevBuy500, stdevSell500 = retreive_x_last_data_stdev('BTCM5', 500)
    stdevBuy200, stdevSell200 = retreive_x_last_data_stdev('BTCM5', 200)
    stdevDataBTC = {"stdevBuy500":float(stdevBuy500), "stdevBuy200":float(stdevBuy200), "stdevSell500":float(stdevSell500), "stdevSell200":float(stdevSell200)}
    
    lastDocBTC = retreive_last_data_db('BTCM5')
    lastDocETH = retreive_last_data_db('ETHM5')
    dataETH = get_last_liquidation_data('M5', 'ETH')
    dataETH.update(stdevDataETH)
    dataBTC = get_last_liquidation_data('M5', 'BTC')
    dataBTC.update(stdevDataBTC)
    print(dataETH['createTime'], lastDocETH['createTime'])

    if int(dataETH['createTime']) > int(lastDocETH['createTime']):
        send_last_to_db(dataETH, 'ETHM5')
    
    if int(dataBTC['createTime']) > int(lastDocBTC['createTime']):
        send_last_to_db(dataBTC, 'BTCM5')
