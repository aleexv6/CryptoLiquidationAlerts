import config
import database as db
import pymongo
from discord_webhook import DiscordWebhook, DiscordEmbed

webhook = DiscordWebhook(url=config.discordWeebhookUrl)

dbname = db.get_database()
collection_name = dbname['BTCM5']

with collection_name.watch() as stream:
    for change in stream:
        print(change)
        if change['operationType'] == 'update':
            document = collection_name.find_one({'_id': change['documentKey']['_id']})
            if 'isBuyThreshold' in change['updateDescription']['updatedFields']:
                buyVolUsd = round(document['buyVolUsd'])
                #message = f'@here Liquidation des shorts M5 - {buyVolUsd}$ liquidés'
                embed = DiscordEmbed(title='BTC M5 - Liquidation des shorts', description=f'Liquidation anormale des shorts : **{buyVolUsd}$** liquidé lors des 5 dernières minutes.', color='E11A00')
                webhook.add_embed(embed)
                webhook.set_content('@here')
                response = webhook.execute(remove_embeds=True)
                

            if 'isSellThreshold' in change['updateDescription']['updatedFields']:
                sellVolUsd = document['sellVolUsd']
                #message = f'@here Liquidation des longs M5 - {sellVolUsd}$ liquidés'
                embed = DiscordEmbed(title='BTC M5 - Liquidation des longs', description=f'Liquidation anormale des longs : **{sellVolUsd}$** liquidé lors des 5 dernières minutes.', color='32BD3F')
                webhook.add_embed(embed)
                webhook.set_content('@here')
                response = webhook.execute(remove_embeds=True)
                
        elif change['operationType'] == 'insert':
            if change['fullDocument']['isBuyThreshold'] == True:
                buyVolUsd = change['fullDocument']['buyVolUsd']
                embed = DiscordEmbed(title='BTC M5 - Liquidation des shorts', description=f'Liquidation anormale des shorts : **{buyVolUsd}$** liquidé lors des 5 dernières minutes.', color='E11A00')
                webhook.add_embed(embed)
                webhook.set_content('@here')
                response = webhook.execute(remove_embeds=True)
            if change['fullDocument']['isSellThreshold'] == True:
                sellVolUsd = change['fullDocument']['sellVolUsd']
                embed = DiscordEmbed(title='BTC M5 - Liquidation des longs', description=f'Liquidation anormale des longs : **{sellVolUsd}$** liquidé lors des 5 dernières minutes.', color='32BD3F')
                webhook.add_embed(embed)
                webhook.set_content('@here')
                response = webhook.execute(remove_embeds=True)
                
                
                
            
# {'_id': {'_data': '82649EDFE6000000082B022C0100296E5A10049B04AC2A66A24E1F96E1D68A5CE3BC6146645F69640064649EDFE65969B507DCC25C990004'},
 # 'operationType': 'insert', 'clusterTime': Timestamp(1688133606, 8), 'wallTime': datetime.datetime(2023, 6, 30, 14, 0, 6, 290000),
 # 'fullDocument': {'_id': ObjectId('649edfe65969b507dcc25c99'), 'buyVolUsd': 2948.204457, 'sellVolUsd': 0.0, 'createTime': 1688133600000, 'stdevBuy500': 431016.32261148375, 'stdevSell500': 769139.9490612656, 'isBuyThreshold': False, 'isSellThreshold': False},
 # 'ns': {'db': 'CodeReaderLiquidations', 'coll': 'BTCM5'}, 'documentKey': {'_id': ObjectId('649edfe65969b507dcc25c99')}}
