import pymongo
import time
import re
import string
import servers
import db
con = pymongo.Connection('001.shrd007.mongo.ktpd.xd.com', 27018)
names = con.database_names()
eachCnt = 100000;
sleepInterval = 30;
leftMon = 1;
from bson.objectid import ObjectId
import datetime;
#while 1:
if(True):
	coList = con["replay_data"].collection_names()
	for name in coList:
		#if (name == "xd_s110.files"):
		if(not (-1 == name.find("files"))):
			print name
			datas = name.split(".");
			vname = datas[0];
			tname = vname.replace("_new","");
			chunkName = vname + ".chunks"
			leftReplay = [];
			r = servers.GetServerByName(tname)
			if(0 == r):
				continue
			d = db.Database(r,False);
			d.Use('demon_winners');
			for item in d.Collection.find():
				if(item.has_key("replayId")):
					leftReplay.append(item["replayId"]);
			d.Use('servant_demon_battles')
			for item in d.Collection.find():
				if(item.has_key("first")):
					bts = item["first"].split(",");
					leftReplay.append(bts[0]);
					leftReplay.append(bts[2]);
					leftReplay.append(bts[4]);
				if(item.has_key("recent")):
					bts = item["recent"].split(",");
					leftReplay.append(bts[0]);
					leftReplay.append(bts[2]);
					leftReplay.append(bts[4]);
     			d.Use('rune_demon_battles');
			for item in d.Collection.find():
				if(item.has_key("first")):
					bts = item["first"].split(",");
					leftReplay.append(bts[0]);
					leftReplay.append(bts[2]);
					leftReplay.append(bts[4]);
				if(item.has_key("recent")):
					bts = item["recent"].split(",")
					leftReplay.append(bts[0]);
					leftReplay.append(bts[2]);
					leftReplay.append(bts[4]);
			d.Use('towerrank')
			for item in d.Collection.find():
				if(item.has_key("record1")):
					leftReplay.append(item["record1"]);
				if(item.has_key("record2")):
				        leftReplay.append(item["record2"]);
				if(item.has_key("record3")):
				        leftReplay.append(item["record3"]);
			d.Use('towerrecent')
			for item in d.Collection.find():
				if(item.has_key("record1")):
					leftReplay.append(item["record1"]);					
				if(item.has_key("record2")):		
					leftReplay.append(item["record2"]);
				if(item.has_key("record3")):
					leftReplay.append(item["record3"]);
			print("total_replay_left" + str(len(leftReplay)))
			for recordId in leftReplay:
				recordTmp = con["replay_data"][name].find({"filename":str(recordId)+".dat"});
				if(recordTmp.count() > 0):
					objId = recordTmp[0]["_id"]
					con["replay_data"][name].update({"_id":objId},{"$set":{"reserve":1}})
					recordChk = con["replay_data"][chunkName].find({"files_id":objId})
					for chk in recordChk:
						con["replay_data"][chunkName].update({"files_id":objId},{"$set":{"reserve":1}})
			#gen_time = datetime.datetime(2014, 9, 25);
			gen_time = datetime.datetime.now() - datetime.timedelta(days = 20);
			dummyid = ObjectId.from_datetime(gen_time);
			#var now = Date.parse(new Date()) / 1000;
			#var endTime = now - leftMon * 30 * 86400;
			#print(str(datetime.));
			cnames = []
			cnames.append(name)
			cnames.append(chunkName)
			for cname in cnames:
				totNum = con["replay_data"][cname].find({"_id":{"$lte" : dummyid}}).count();
				while totNum > 0:
					if totNum <= eachCnt:
						con["replay_data"][cname].remove({"_id":{"$lte" : dummyid},"reserve":{"$ne":1}});
						time.sleep(sleepInterval);
						break;
					else:
						cur = con["replay_data"][cname].find({"_id":{"$lt" : dummyid}}).sort("_id", 1).skip(eachCnt);
						if cur.alive:
							cur = con["replay_data"][cname].remove({"_id":{"$lte" : cur[0]["_id"]},"reserve":{"$ne":1}});
							time.sleep(sleepInterval);
						else:
							time.sleep(sleepInterval);
							break;
						totNum = con["replay_data"][cname].find({"_id":{"$lte" : dummyid}}).count();


print datetime.datetime.now();
