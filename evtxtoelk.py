import os
import ssl
import time

from elasticsearch.connection import create_ssl_context
from dotenv import load_dotenv

import contextlib
import mmap
import traceback
import json
import argparse
from collections import OrderedDict
from datetime import datetime

from Evtx.Evtx import FileHeader
from Evtx.Views import evtx_file_xml_view
from elasticsearch import Elasticsearch, helpers
import xmltodict
import sys


def save_last_timestamp(timestamp):
    with open('./last_timestamp.txt', "w") as last_timestamp:
        last_timestamp.write(timestamp)


def get_last_timestamp():
    try:
        file = ''.join(open('last_timestamp.txt').read().split('\n'))
    except:
        print("No file found");
        return -1
    return file.strip('\r')


class EvtxToElk:
    @staticmethod
    def bulk_to_elasticsearch(es, bulk_queue):
        try:
            helpers.bulk(es, bulk_queue)
            return True
        except:
            print(traceback.print_exc())
            return False

    @staticmethod
    def evtx_to_elk(filename, elk_ip, elk_index="hostlogs", bulk_queue_len_threshold=500, metadata={}):
        bulk_queue = []

        can_index_records = False

        last_timestamp_registered = get_last_timestamp()
        if last_timestamp_registered == -1:
            can_index_records = True
            print("No previous timestamp found, will index everything")

        context = create_ssl_context(cafile=os.environ.get("CA_PATH"))
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        es = Elasticsearch(
            ['host'],
            http_auth=(os.environ.get("ES_USER"), os.environ.get("ES_PWD")),
            scheme="https",
            port=9200,
            ssl_context=context,
        )

        if last_timestamp_registered == -1:
            print("Deleting in index '" + elk_index + "' Where @timestamp=" + last_timestamp_registered + "...")
            # es.indices.delete(index='IndexName', ignore=[400, 404])
            # es.delete_by_query(index=elk_index, q={"@timestamp:": last_timestamp_registered})
            es.delete_by_query(
                index=elk_index,
                body=
                {
                    "query": {
                        "match": {
                            "@timestamp": last_timestamp_registered
                        }
                    }
                }
            )
            time.sleep(10)
            print("Done.")

        with open(filename) as infile:
            with contextlib.closing(mmap.mmap(infile.fileno(), 0, access=mmap.ACCESS_READ)) as buf:
                fh = FileHeader(buf, 0x0)
                data = ""
                for xml, record in evtx_file_xml_view(fh):
                    try:
                        contains_event_data = False
                        log_line = xmltodict.parse(xml)

                        # Format the date field
                        date = log_line.get("Event").get("System").get("TimeCreated").get("@SystemTime")
                        if "." not in str(date):
                            date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
                        else:
                            date = datetime.strptime(date, "%Y-%m-%d %H:%M:%S.%f")
                        log_line['@timestamp'] = str(date.isoformat())
                        log_line["Event"]["System"]["TimeCreated"]["@SystemTime"] = str(date.isoformat())

                        if log_line['@timestamp'] == last_timestamp_registered:
                            print("Can index value now")
                            can_index_records = True
                        else:
                            print("Too soon : " + log_line['@timestamp'])

                        if can_index_records:
                            # Process the data field to be searchable
                            data = ""
                            if log_line.get("Event") is not None:
                                data = log_line.get("Event")
                                if log_line.get("Event").get("EventData") is not None:
                                    data = log_line.get("Event").get("EventData")
                                    if log_line.get("Event").get("EventData").get("Data") is not None:
                                        data = log_line.get("Event").get("EventData").get("Data")
                                        if isinstance(data, list):
                                            contains_event_data = True
                                            data_vals = {}
                                            for dataitem in data:
                                                try:
                                                    if dataitem.get("@Name") is not None:
                                                        data_vals[str(dataitem.get("@Name"))] = str(
                                                            str(dataitem.get("#text")))
                                                except:
                                                    pass
                                            log_line["Event"]["EventData"]["Data"] = data_vals
                                        else:
                                            if isinstance(data, OrderedDict):
                                                log_line["Event"]["EventData"]["RawData"] = json.dumps(data)
                                            else:
                                                log_line["Event"]["EventData"]["RawData"] = str(data)
                                            del log_line["Event"]["EventData"]["Data"]
                                    else:
                                        if isinstance(data, OrderedDict):
                                            log_line["Event"]["RawData"] = json.dumps(data)
                                        else:
                                            log_line["Event"]["RawData"] = str(data)
                                        del log_line["Event"]["EventData"]
                                else:
                                    if isinstance(data, OrderedDict):
                                        log_line = dict(data)
                                    else:
                                        log_line["RawData"] = str(data)
                                        del log_line["Event"]
                            else:
                                pass

                            # Insert data into queue
                            # event_record = json.loads(json.dumps(log_line))
                            # event_record.update({
                            #    "_index": elk_index,
                            #    "_type": elk_index,
                            #    "metadata": metadata
                            # })
                            # bulk_queue.append(event_record)
                            event_data = json.loads(json.dumps(log_line))
                            event_data["_index"] = elk_index
                            event_data["_type"] = elk_index
                            event_data["meta"] = metadata
                            bulk_queue.append(event_data)

                            # bulk_queue.append({
                            #    "_index": elk_index,
                            #    "_type": elk_index,
                            #    "body": json.loads(json.dumps(log_line)),
                            #    "metadata": metadata
                            # })

                            if len(bulk_queue) == bulk_queue_len_threshold:
                                print('Bulkingrecords to ES: ' + str(len(bulk_queue)))
                                # start parallel bulking to ElasticSearch, default 500 chunks;
                                if EvtxToElk.bulk_to_elasticsearch(es, bulk_queue):
                                    bulk_queue = []
                                else:
                                    print('Failed to bulk data to Elasticsearch')
                                    sys.exit(1)

                    except:
                        print("***********")
                        print("Parsing Exception")
                        print(traceback.print_exc())
                        print(json.dumps(log_line, indent=2))
                        print("***********")

                # Check for any remaining records in the bulk queue
                if len(bulk_queue) > 0:
                    print('Bulking final set of records to ES: ' + str(len(bulk_queue)))
                    if EvtxToElk.bulk_to_elasticsearch(es, bulk_queue):
                        bulk_queue = []
                    else:
                        print('Failed to bulk data to Elasticsearch')
                        sys.exit(1)


if __name__ == "__main__":
    load_dotenv()
    EvtxToElk.evtx_to_elk(os.environ.get("EVTX_PATH"),
                          os.environ.get("ES_HOST"),
                          elk_index=os.environ.get("AD_INDEX"),
                          bulk_queue_len_threshold=500,
                          metadata={}
                          )
