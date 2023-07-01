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
    
def retreive_last_data_db(collectionName): #get last data from known data in database
    dbname = db.get_database()
    collection_name = dbname[collectionName]
    last_document = collection_name.find_one({}, sort=[("_id", pymongo.DESCENDING)])
    return last_document

def retreive_every_data_percentil(collectionName):
    dbname = db.get_database()
    collectionName = dbname[collectionName]
    last_documents = collectionName.find({})
    data = list(last_documents)    
    buyData = [item['buyVolUsd'] for item in data]
    sellData = [item['sellVolUsd'] for item in data]
    #print(data)
    
    percentilBuy = numpy.quantile(buyData, [0.95, 0.97])
    percentilSell = numpy.quantile(sellData, [0.95, 0.97])
    
    # for x in data:
        # if x['sellVolUsd'] > percentilBuy[1]:
            # print(x)
    
    return percentilBuy, percentilSell
    
def update_data(filter, collectionName, update): #update new liquidation into known database data
    dbname = db.get_database()
    collectionName = dbname[collectionName]
    collectionName.update_one(filter, update)


if __name__ == "__main__":
    # historicalData = get_history_liquidations_data('M5', 'ETH')   
    # historical_send_to_db(historicalData, 'ETHM5')
    # historicalData = get_history_liquidations_data('M5', 'BTC')   
    # historical_send_to_db(historicalData, 'BTCM5')    
    
    
    
    isBuyThresholdETH = False
    isSellThresholdETH = False
    isBuyThresholdBTC = False
    isSellThresholdBTC = False
    
    lastDocBTC = retreive_last_data_db('BTCH12')
    lastDocETH = retreive_last_data_db('ETHH12')
    dataETH = get_last_liquidation_data('H12', 'ETH')
    dataBTC = get_last_liquidation_data('H12', 'BTC')
    
    percentilBuyBTC, percentilSellBTC = retreive_every_data_percentil('BTCH12')
    percentilBuy97BTC = percentilBuyBTC[1] 
    percentilSell97BTC = percentilSellBTC[1] 
    
    percentilBuyETH, percentilSellETH = retreive_every_data_percentil('ETHH12')
    percentilBuy97ETH = percentilBuyETH[1] 
    percentilSell97ETH = percentilSellETH[1] 
    
    #stdevBuy500ETH, stdevSell500ETH = retreive_x_last_data_stdev('ETHM5', 500)
    #stdevBuy200, stdevSell200 = retreive_x_last_data_stdev('ETHM5', 200)
    #stdevBuy500BTC, stdevSell500BTC = retreive_x_last_data_stdev('BTCM5', 500)
    #stdevBuy200, stdevSell200 = retreive_x_last_data_stdev('BTCM5', 200)
    
    if dataETH['buyVolUsd'] > percentilBuy97ETH:
        isBuyThresholdETH = True
    if dataETH['sellVolUsd'] > percentilSell97ETH:
        isSellThresholdETH = True
    if dataBTC['buyVolUsd'] > percentilBuy97BTC:
        isBuyThresholdBTC = True
    if dataBTC['sellVolUsd'] > percentilSell97BTC:
        isSellThresholdBTC = True
        
    percentilDataETH = {"percentilBuy":float(percentilBuy97ETH), "percentilSell":float(percentilSell97ETH), 'isBuyThreshold': isBuyThresholdETH, 'isSellThreshold': isSellThresholdETH}
    percentilDataBTC = {"percentilBuy":float(percentilBuy97BTC), "percentilSell":float(percentilSell97BTC), 'isBuyThreshold': isBuyThresholdBTC, 'isSellThreshold': isSellThresholdBTC}
    
    dataETH.update(percentilDataETH)
    dataBTC.update(percentilDataBTC)
    
    #print(dataBTC)
    #print(dataETH)

    if int(dataETH['createTime']) > int(lastDocETH['createTime']):
        send_last_to_db(dataETH, 'ETHH12')
    elif int(dataETH['createTime']) == int(lastDocETH['createTime']) and (float(dataETH['buyVolUsd']) != float(lastDocETH['buyVolUsd']) or float(dataETH['sellVolUsd']) != float(lastDocETH['sellVolUsd'])):
        update_data({"createTime": dataETH['createTime']} , 'ETHH12', {"$set": {"buyVolUsd" : dataETH['buyVolUsd']}})
        update_data({"createTime": dataETH['createTime']} , 'ETHH12', {"$set": {"sellVolUsd" : dataETH['sellVolUsd']}})
        if dataETH['buyVolUsd'] > percentilBuy97ETH:
            update_data({"createTime": dataETH['createTime']} , 'ETHH12', {"$set": {"isBuyThreshold" : True}})
        if dataETH['sellVolUsd'] > percentilSell97ETH:
            update_data({"createTime": dataETH['createTime']} , 'ETHH12', {"$set": {"isSellThreshold" : True}})
        
    
    if int(dataBTC['createTime']) > int(lastDocBTC['createTime']):
        send_last_to_db(dataBTC, 'BTCH12')
    elif int(dataBTC['createTime']) == int(lastDocBTC['createTime']) and (float(dataBTC['buyVolUsd']) != float(lastDocBTC['buyVolUsd']) or float(dataBTC['sellVolUsd']) != float(lastDocBTC['sellVolUsd'])):
        update_data({"createTime": dataBTC['createTime']} , 'BTCH12', {"$set": {"buyVolUsd" : dataBTC['buyVolUsd']}})
        update_data({"createTime": dataBTC['createTime']} , 'BTCH12', {"$set": {"sellVolUsd" : dataBTC['sellVolUsd']}})
        if dataBTC['buyVolUsd'] > percentilBuy97BTC:
            update_data({"createTime": dataBTC['createTime']} , 'BTCH12', {"$set": {"isBuyThreshold" : True}})
        if dataBTC['sellVolUsd'] > percentilSell97BTC:
            update_data({"createTime": dataBTC['createTime']} , 'BTCH12', {"$set": {"isSellThreshold" : True}})
