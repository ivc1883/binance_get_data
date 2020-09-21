# used  in python v2.7.15
import urllib
import json
import ccxt
import re
import datetime
import requests
import time
import math



def all_ids():
    # load all markets from binance into a list
    id = 'binance'
    exchange_found = id in ccxt.exchanges
    if exchange_found:
        exchange = getattr(ccxt, id)({})
        markets = exchange.load_markets()
        tuples = list(ccxt.Exchange.keysort(markets).items())

        ids = []

        for (k, v) in tuples:
          try:
            if (re.search('.USDT', v['id'])): 
                r=requests.get('https://fapi.binance.com/fapi/v1/klines' + '?symbol=' + str(v['id']) + '&interval=' + '1h'+ '&limit=' + str(1))
                if (r.json()[0][0])>((time.time()*1000) - (24*60*60*1000)):
                    ids.append(v['id'])
                    print(str(v['id']) + ' - last candle ' + str(datetime.datetime.fromtimestamp(int(r.json()[0][0])/1000)))
                    print("")
          except:
            pass
        

        with open('BIN-F.txt','w') as f:
            f.write(str(ids))

    return ids

def give_first_kline_open_stamp(interval, symbol, start_ts):
    '''
    Returns the first kline from an interval and start timestamp and symbol
    :param interval:  1w, 1d, 1m etc - the bar length to query
    :param symbol:    BTCUSDT or LTCBTC etc
    :param start_ts:  Timestamp in miliseconds to start the query from
    :return:          The first open candle timestamp
    '''


    url_to_get = "https://fapi.binance.com/fapi/v1/klines?symbol="+ str(symbol) + "&interval="+ str(interval) + "&startTime="+ str(start_ts) 
    kline_data = requests.get(url_to_get).json()

    return kline_data[0][0]


# Get list of all IDs on binance
ids = []
ids = all_ids()

print("------------------------------------------------------");
print("------------------------------------------------------");

for this_id in ids:
    '''
    Find launch Week of symbol, start at Binance launch date 2017-01-01 (1483228800000) or 1/1/19 1546308971000 or 1/1/20 1577844971000
    Find launch Day of symbol in week
    Find launch minute of symbol in day
    '''
    
    symbol_launch_week_stamp   = give_first_kline_open_stamp('1w', this_id, 1483228800000 )
    symbol_launch_day_stamp    = give_first_kline_open_stamp('1d', this_id, symbol_launch_week_stamp)
    symbol_launch_minute_stamp = give_first_kline_open_stamp('1m', this_id, symbol_launch_day_stamp)

    print(this_id + ' launched ' + str(datetime.datetime.fromtimestamp(int(symbol_launch_minute_stamp)/1000)) )

    start_date =symbol_launch_minute_stamp
    end_date= (time.time()*1000)



    '''**********************************************************************************************************************************************
       ********************************************************EDIT ONLY THIS PARAMETER**************************************************************
    '''
    timeframe = '4h' #Available values: '1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M'
    '''**********************************************************************************************************************************************
       **********************************************************************************************************************************************
    '''

    if(timeframe=='1m'):
      rangetime=1*60*1000

    if(timeframe=='5m'):
      rangetime=5*60*1000

    if(timeframe=='15m'):
      rangetime=15*60*1000

    if(timeframe=='30m'):
      rangetime=30*60*1000

    if(timeframe=='1h'):
      rangetime=1*60*60*1000

    if(timeframe=='2h'):
      rangetime=2*60*60*1000

    if(timeframe=='4h'):
      rangetime=4*60*60*1000

    if(timeframe=='6h'):
      rangetime=6*60*60*1000

    if(timeframe=='8h'):
      rangetime=8*60*60*1000

    if(timeframe=='12h'):
      rangetime=12*60*60*1000

    if(timeframe=='1d'):
      rangetime=1*24*60*60*1000

    if(timeframe=='3D'):
      rangetime=3*24*60*60*1000

    if(timeframe=='1w'):
      rangetime=7*24*60*60*1000

    if(timeframe=='1M'):
      rangetime=30.5*24*60*60*1000

    final_data=[]


    try:
      xx = open('BIN_F_'+this_id+'_'+timeframe+'.json')
      print('Existing Data')
      with open('BIN_F_'+this_id+'_'+timeframe+'.json', 'r') as json_file :
        checkdata = json.load(json_file)
        start_date=checkdata[len(checkdata)-1][0]
        checkdata.pop(len(checkdata)-1)
        print('Original data on file : ' + str(len(checkdata)));
        final_data=checkdata

    except IOError:
      print("New Data")

    print("")


    requireds=(math.ceil((end_date-start_date)/(rangetime*1000))) # 1000 is becuase of the limit we can get from binance, max 1000 
    print('total cycles required : ' + str(int(requireds)))
    print("")

    for _ in range(0,int(requireds)):

      url = "https://fapi.binance.com/fapi/v1/klines?symbol=" + str(this_id) + "&interval=" + str(timeframe) + "&startTime=" + str(start_date) + "&endTime=" + str(int(end_date)) + "&limit=" + str(1000)  # 1000 is becuase of the limit, max 1000 
      

      temp_data = requests.get(url).json()

      final_data = final_data+temp_data

      print(time.ctime()+" - data adquired - " + str(len(temp_data)) + ' cycle - ' + str(_+1) + ' of aprox. ' +  str(int(requireds)))

      if len(temp_data)<1000:
        break

      start_date = temp_data[len(temp_data)-1][0]+(rangetime)    # 1499040000000,      // Open time
                                                                 #   "0.01634790",       // Open
                                                                 #   "0.80000000",       // High
                                                                 #   "0.01575800",       // Low
                                                                 #   "0.01577100",       // Close
                                                                 #   "148976.11427815",  // Volume
                                                                 #   1499644799999,      // Close time
                                                                 #   "2434.19055334",    // Quote asset volume
                                                                 #   308,                // Number of trades
                                                                 #   "1756.87402397",    // Taker buy base asset volume
                                                                 #   "28.46694368",      // Taker buy quote asset volume
                                                                 #   "17928899.62484339" // Ignore.

      
      time.sleep(1)
    try:
      xx = open('BIN_F_'+this_id+'_'+timeframe+'.json')
      print("")
      print("New data added to file : " + str(len(final_data)-len(checkdata)))
    except:
      print("")
      

    print('Total data : ' + str(len(final_data)));
    print("------------------------------------------------------");
    print("------------------------------------------------------");
    
    with open('BIN_F_'+this_id+'_'+timeframe+'.json','w') as f:
      f.write(str(final_data))

    
    with open('BIN_F_'+this_id+'_'+timeframe+'.json', 'r') as f :
      filedata = f.read()
      filedata = filedata.replace('L', '')
      filedata = filedata.replace("u'", '')
      filedata = filedata.replace("'", '')

    with open('BIN_F_'+this_id+'_'+timeframe+'.json', 'w') as f:
      f.write(filedata)