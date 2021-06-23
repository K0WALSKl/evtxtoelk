import ssl
from elasticsearch.connection import create_ssl_context
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


def save_last_record(secondLastLine):
    with open('./last_record.txt', "w") as last_record:
        last_record.write(secondLastLine)


def save_number_of_items(number_of_items):
    with open('./last_number_of_items.txt', "w") as last_record:
        last_record.write(str(number_of_items))


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

        # Pour sauvegarder le dernier timestamp
        secondLastLine = None
        lastLine = None

        # Sert à stocker et lire le dernier item
        last_record = None
        number_of_items_during_previous_exec = 0
        i = 0

        # Permet d'indexer sur ElasticSearch si = True (empêche l'indexation d'un item déjà existant)
        can_add_new_items = False

        context = create_ssl_context(cafile="/home/userone/evtxtoelk/ca.pem")
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        es = Elasticsearch(
            ['host'],
            http_auth=('user', 'password'),
            scheme="https",
            port=9200,
            ssl_context=context,
        )

        try:
            last_record = ''.join(open('last_record.txt').read().split('\n'))
            number_of_items_during_previous_exec = int(open('last_number_of_items.txt').read())
            print(str(number_of_items_during_previous_exec) + " items were indexed last time")
        except:
            print("No file named last_number_of_items.txt, will index everything")
            can_add_new_items = True

        with open(filename) as infile:
            with contextlib.closing(mmap.mmap(infile.fileno(), 0, access=mmap.ACCESS_READ)) as buf:
                fh = FileHeader(buf, 0x0)
                data = ""
                for xml, record in evtx_file_xml_view(fh):
                    try:
                        compare_xml = ''.join(xml.split('\n'))
                        compare_xml = ''.join(compare_xml.split('\r'))
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

                        if can_add_new_items:
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

                        else:
                            # if i == 76:
                            #     print("SAVING")
                            #     save_last_record(xml)
                            if i == number_of_items_during_previous_exec:
                                # if compare_xml == last_record:
                                print("Yes")
                                can_add_new_items = True
                            else:
                                print("No")

                        i += 1

                    except:
                        print("***********")
                        print("Parsing Exception")
                        print(traceback.print_exc())
                        print(json.dumps(log_line, indent=2))
                        print("***********")

                save_number_of_items(i)

                # Check for any remaining records in the bulk queue
                if len(bulk_queue) > 0:
                    print('Bulking final set of records to ES: ' + str(len(bulk_queue)))
                    if EvtxToElk.bulk_to_elasticsearch(es, bulk_queue):
                        bulk_queue = []
                    else:
                        print('Failed to bulk data to Elasticsearch')
                        sys.exit(1)


if __name__ == "__main__":
    # Create argument parser
    # parser = argparse.ArgumentParser()
    # # Add arguments
    # parser.add_argument('evtxfile', help="Evtx file to parse")
    # parser.add_argument('elk_ip', default="localhost", help="IP (and port) of ELK instance")
    # parser.add_argument('-i', default="hostlogs", help="ELK index to load data into")
    # parser.add_argument('-s', default=500, help="Size of queue")
    # parser.add_argument('-meta', default={}, type=json.loads, help="Metadata to add to records")
    # # Parse arguments and call evtx to elk class
    # args = parser.parse_args()
    # EvtxToElk.evtx_to_elk(args.evtxfile, args.elk_ip, elk_index=args.i, bulk_queue_len_threshold=int(args.s), metadata=args.meta)
    EvtxToElk.evtx_to_elk("/home/", "", elk_index="hostlogs", bulk_queue_len_threshold=500, metadata={})
