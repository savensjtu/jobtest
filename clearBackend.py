import pymongo
import time
import re
import sys
import os
if len(sys.argv) < 2:
  sys.exit();
con = pymongo.Connection('001.'+sys.argv[1]+'.mongo.ktpd.xd.com', 27018)
names = con.database_names()
eachCnt = 100000;
sleepInterval = 30;
leftMon = 2;
while 1:
   for data in names:
	if re.match("log_", data) or re.match("ktlog_", data):
		print data;
		coList = con[data].collection_names()
	        for name in coList:
			if re.match("notify_", name) or re.match("online", name):
				if name != "notify_useMoney" and name != "notify_log" and name != "notify_trade" and name != "notify_purchase" and name != "notify_give" and name != "notify_gameGive" and name != "notify_tradeTest" and name != "notify_useMoneyTest" and name != "notify_logTest":
					time.sleep(sleepInterval);
					now = int(time.time())
					endTime = now - leftMon * 30 * 86400
					totNum = con[data][name].find({"time":{"$lte" : endTime}}).count();
					while totNum > 0:
							if totNum <= eachCnt:
								con[data][name].remove({"time":{"$lte" : endTime}});
								time.sleep(sleepInterval);
								break;
							else:
								cur = con[data][name].find({"time":{"$lt" : endTime}}).sort("time", 1).skip(eachCnt);
								if cur.alive:
									cur = con[data][name].remove({"time":{"$lte" : cur[0]["time"]}});
									time.sleep(sleepInterval);
								else:
									time.sleep(sleepInterval);
									break;
							totNum = con[data][name].find({"time":{"$lte" : endTime}}).count();
