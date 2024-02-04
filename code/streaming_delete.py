import math
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch, exceptions
import warnings
import time
warnings.filterwarnings("ignore")


# delete by query (matching the exact term)
def delete_by_query(es, index,query):
    start_time = time.time()
    print("start delete by query...")
    response = es.delete_by_query(index=index, body=query,wait_for_completion=True,refresh=True)
    print("response: ", response)
    end_time = time.time()  # End time measurement
    total_time = end_time - start_time
    print(f"\tTotal time to delete: {total_time} seconds")
    return response

def bulk_delete(es, index_name, mu, start_date):

    while True:
        start_time = time.time()
        gte_date = start_date.strftime("%Y-%m-%dT%H:%M:%S")
        print("gte_date: ", gte_date)
        lte_date = (start_date + timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S")
        print("lte_date: ", lte_date)

        # delete using wildcard in a date range
        delete_query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "creationDate": {
                                    "gte": gte_date,
                                    "lte": lte_date
                                }
                            }
                        },
                        {
                            "wildcard": {
                                "body": {
                                    "value": "*database*"
                                }
                            }
                        }
                    ]
                }
            }
        }
        response = delete_by_query(es, index_name, delete_query)
        end_time = time.time()
        time_taken = end_time - start_time
        print("Deletion took: ", time_taken)
        deleted_docs = response['total']
        ratio = deleted_docs / 9000
        remainder = deleted_docs % 9000
        if remainder > 4500:
            sleep_intervals = math.ceil(ratio)
        else:
            sleep_intervals = math.floor(ratio)

        if sleep_intervals == 0:
            print(f"\tsleep_intervals: {sleep_intervals}")
            sleep_time = 300 - time_taken
            print(f"\tSleep for remaining {sleep_time} seconds")
            time.sleep(sleep_time)
        else:
            print(f"\tsleep_intervals: {sleep_intervals}")

            sleep_time = sleep_intervals * 60 * 5 - time_taken
            if sleep_time > 0:
                print(f"\tSleeping for remaining {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                print("no need to sleep")


        start_date += timedelta(days=30)
        print("\nnew start_date: ", start_date)

if __name__ == "__main__":
    es = Elasticsearch(['129.114.109.87:9200'], timeout=7200)  # delete-search
    index_name = "so"
    mu = 30
    start_date = datetime(2013, 4, 21)
    duration_time = 2400   # seconds
    bulk_delete(es, index_name, mu, start_date)