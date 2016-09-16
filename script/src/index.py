import os
import csv
from elasticsearch import helpers
from elasticsearch.client import Elasticsearch
import time
# See http://stackoverflow.com/questions/21129020/how-to-fix-unicodedecodeerror-ascii-codec-cant-decode-byte
import sys
reload(sys)
sys.setdefaultencoding('utf8')

es = Elasticsearch(['elasticsearch'])
actions = []

indexName = "generic"

es.indices.delete(index=indexName, ignore=[400, 404])
indexSettings = {
  "settings": {
	"index.number_of_replicas": 0,
	"index.number_of_shards": 1,
  },
  "mappings": {
	"_default_": {
		"dynamic_templates": [
		   {
			  "object_fields": {
				 "mapping": {
					"type": "object"
				 },
				 "match_mapping_type": "object"
			  }
		   },
		   {
			  "string_fields": {
				 "mapping": {
					"index": "analyzed",
					"omit_norms": True,
					"type": "string",
					"fields": {
					   "raw": {
						  "ignore_above": 256,
						  "index": "not_analyzed",
						  "type": "string",
						  "doc_values": True
					   }                        }
				 },
				 "match": "*"
			  }
		   }
		]
	 }
  }
}
es.indices.create(index=indexName, body=indexSettings)


def loadCsvToDict(allEntities, nodeType,filename, key, label):
    print "loading", nodeType, "reference data"
    headerRow=[]
    with open(filename, 'rb') as scsvfile:
        sreader = csv.reader(scsvfile)
        rowNum = 0
        for row in sreader:
            rowNum+=1
            if rowNum==1:
                headerRow=row
                continue
            doc={
                "nodeType":nodeType
            }
            for i in range(0,len(headerRow)):
                doc[headerRow[i]]=row[i]

            doc["id"]="["+doc[key]+"] "+ doc[label]

            if doc[key] in allEntities:
                old= allEntities[doc[key]]
                old.update(doc)
            else:
                allEntities[doc[key]]=doc
                                
allEntities={}
# TODO: Edit this next line
# loadCsvToDict(allEntities,"address","import/Addresses.csv", "node_id", "address")


edgesfile = "import/all_edges.csv"

headerRow=[]
actions = []
print "Building docs"
with open(edgesfile, 'rb') as scsvfile:
	sreader = csv.reader(scsvfile)
	rowNum = 0
	for row in sreader:
		rowNum+=1
		if rowNum==1:
			headerRow=row
			continue
		doc={
			"fromNodeId":row[0],
			"toNodeId":row[2]
		}
		ent1=allEntities[row[0]]
		ent2=allEntities[row[2]]
		
		if ent1["nodeType"]==ent2["nodeType"]:
			doc[ent1["nodeType"]]=[ent1, ent2]
		else:
			doc[ent1["nodeType"]]=ent1
			doc[ent2["nodeType"]]=ent2
		doc["relationship"]=row[1]
		doc["relationshipLabel"]=row[0]+" "+row[1] +" "+row[2]
		action = {
			"_index": indexName,
			'_op_type': 'index',
			"_type": "edge",
			"_source": doc
		}
		actions.append(action)
		# Flush bulk indexing action if necessary
		if len(actions) >= 5000:
			print rowNum, "docs loaded"
			helpers.bulk(es, actions)
			del actions[0:len(actions)]

if len(actions) > 0:
	helpers.bulk(es, actions)
