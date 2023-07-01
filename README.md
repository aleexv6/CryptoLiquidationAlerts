# CodeReaderLiquidation
 Alert on Discord server when pic liquidation on crypto markets
 
 Package needed : requests, pymongo, numpy, discord-webhook
 
 To install package, use : python -m pip install myPackageName
 
 Add MongoDB user, password, coinglass secret api and Discord Webhook URL to config.py
 
 Remember to change string connection and mongodb database name to database.py
 
 Command to run script in background with a log file : python streamBTCM5.py >> /home/opc/liquidations/streamBTCM5.log 2>&1 &
