import math
from datetime import datetime, timedelta
from random import random

from elasticsearch import Elasticsearch
from elasticsearch.client import Elasticsearch, IndicesClient
from elasticsearch import Elasticsearch, exceptions
import warnings
from elasticsearch import helpers
from elasticsearch.helpers import bulk
import time
warnings.filterwarnings("ignore")
import json
import html
from bs4 import BeautifulSoup
from elasticsearch.client import SnapshotClient
import os
import gzip
from multiprocessing import Pool
import requests
import numpy as np
from scipy.stats import poisson
import threading

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


def delete_document_by_id(es, index_name, doc_id):
    try:
        response = es.delete(index=index_name, id=doc_id)
        print(f"Success:{doc_id}")
    except exceptions.NotFoundError as e:
        #print("Document ID not found:", doc_id)
        print("Elasticsearch error: " + str(e))

def forcemerge(es, index_name):
    response = es.indices.forcemerge(index=index_name, wait_for_completion=True, only_expunge_deletes=True)
    print(response)

def get_snapshot_info(repo_name, snap_name,snapshot_client):
    try:
        return snapshot_client.get(repository=repo_name, snapshot=snap_name)
    except Exception as e:
        print(f"Error fetching snapshot info: {e}")
        return None

def delete_docs_from_index(index,query):
    # Perform the delete by query operation
    try:
        # Perform the delete by query operation
        response = es.delete_by_query(index=index, body=query, wait_for_completion=True, refresh=True)
        print("response: ", response)
        # Force merge after deletion
        es.indices.forcemerge(index=index, only_expunge_deletes=True)

        # Delay to ensure force merge completion
        #time.sleep(60)
    except Exception as e:
        print(f"Error during delete by query: {e}")

def restore_snapshot(snapshot, snapshot_repo, snapshot_client):
    start_time = time.time()
    try:
        response = snapshot_client.restore(repository=snapshot_repo, snapshot=snapshot, wait_for_completion=True)
        print(f"Response from restore index '{index_name}': {response}")
    except Exception as e:
        print(f"Error during restore snapshot: {e}")

    end_time = time.time()  # End time measurement
    total_time = end_time - start_time
    print(f"\tTotal time to restore snapshot: {total_time} seconds")

def close_index_if_exists(index_name, es_client):
    indices_client = IndicesClient(es_client)
    try:
        response = indices_client.close(index=index_name)
        print(f"\tClosed existing index: '{index_name}' : {response}")
    except Exception as e:
        print(f"\tError closing index {index_name}: {e}")

def open_index(index_name, es_client):
    indices_client = IndicesClient(es_client)
    try:
        response = indices_client.open(index=index_name)
        print(f"Response from opening index '{index_name}': {response}")
    except Exception as e:
        print(f"Error opening index {index_name}: {e}")

def create_snapshot(snapshot_name, snapshot_repo, index_name, snapshot_client):
    start_time = time.time()
    try:
        body = {
            "indices": index_name
        }
        response = snapshot_client.create(repository=snapshot_repo, snapshot=snapshot_name, body=body, wait_for_completion=True)
        print(f"Response from create snapshot '{index_name}': {response}")
    except Exception as e:
        print(f"Error during create snapshot: {e}")
    end_time = time.time()  # End time measurement
    total_time = end_time - start_time
    print(f"\tTotal time to create snapshot: {total_time} seconds")

def delete_snapshot(snapshot_name, snapshot_repo, index_name, snapshot_client):
    start_time = time.time()
    try:
        time
        response = snapshot_client.delete(repository=snapshot_repo, snapshot=snapshot_name)
        print(f"Response from delete snapshot '{index_name}': {response}")
    except Exception as e:
        print(f"Error deleting snapshot {snapshot_name}: {e}")
    end_time = time.time()  # End time measurement
    total_time = end_time - start_time
    print(f"\tTotal time to delete snapshot: {total_time} seconds")

def snapshots_cleansing_delete(delete_query, index_name, snapshots, snapshot_repo):
    start_time = time.time()

    snapshot_client = SnapshotClient(es)
    for snapshot in snapshots:

        index_exist = False
        # Close the same name index if it exists
        print(f"Task1: Closing index if needed: {snapshot}")
        if es.indices.exists(index=index_name):
            index_exist = True
            close_index_if_exists(index_name, es)

        print(f"Task2: Restoring snapshot: {snapshot}")
        restore_snapshot(snapshot, snapshot_repo, snapshot_client)

        print(f"Deleting documents from index '{index_name}'")
        delete_docs_from_index(index_name, delete_query)

        # create a new snaphost and delete the old one
        new_snapshot_name = f"{snapshot}_modified"
        print(f"Creating new snapshot: {new_snapshot_name}")
        create_snapshot(new_snapshot_name, snapshot_repo, index_name, snapshot_client)
        # Delete the old snapshot
        delete_snapshot(snapshot, snapshot_repo, index_name, snapshot_client)
        print(f"Process completed for delete snapshot: {snapshot}")

        # open the previsouly closed index
        if index_exist:
            open_index(index_name, es)

    end_time = time.time()  # End time measurement
    total_time = end_time - start_time
    print(f"Total time to execute method: {total_time} seconds")


def request_data(line, keywords_to_delete):
    # List of keywords to delete

    # Check if any keyword is in the line
    return any(keyword in line for keyword in keywords_to_delete)

# handle logs file as .log
def process_logs_gz_file(log_file_path, keywords_to_delete):
   # Check if the file exists
    if not os.path.isfile(log_file_path):
        print(f"Log file {log_file_path} does not exist.")
        return
    temp_file_path = log_file_path + ".tmp"
    try:
        # Read the contents of the file
        with gzip.open(log_file_path, 'rt', encoding='utf-8') as file:
            lines = file.readlines()

        # Filter out the lines you want to delete
        lines = [line for line in lines if not request_data(line, keywords_to_delete)]

        # Write the modified data back to a compressed JSON file
        with gzip.open(log_file_path, 'wt', encoding='utf-8') as file:
            file.writelines(lines)
        print(f"File {log_file_path} has been processed.")
    except Exception as e:
        print(f"Error: {e}")

def follow(thefile):
    # Initially, read the entire file
    while True:
        line = thefile.readline()
        #print(line)
        if not line:
            break
        yield line

def request_data(line, keywords_to_delete):
    return any(keyword in line for keyword in keywords_to_delete)

def process_logs_file(log_file_path, keywords_to_delete):
    if not os.path.isfile(log_file_path):
        print(f"Log file {log_file_path} does not exist.")
        return
    temp_file_path = log_file_path + ".tmp"
    try:
        with gzip.open(log_file_path, 'rt', encoding='utf-8') as read_file, gzip.open(temp_file_path, 'wt', encoding='utf-8') as write_file:
            for line in follow(read_file):
                if not request_data(line, keywords_to_delete):
                    write_file.write(line)

        # Replace the original file with the modified one
        os.replace(temp_file_path, log_file_path)
        print(f"File {log_file_path} has been processed.")
    except Exception as e:
        print(f"Error: {e}")


def contains_keyword(data, keywords):
    if isinstance(data, dict):
        for key, value in data.items():
            # Convert value to string and make it lowercase
            value_str = str(value).lower()
            if any(keyword.lower() in value_str for keyword in keywords):
                return True
    return False

def process_json_gz_file(file_path, keywords):
    if not os.path.isfile(file_path):
        print(f"JSON file {file_path} does not exist.")
        return
    temp_file_path = file_path + ".tmp"
    try:
        with gzip.open(file_path, 'rt', encoding='utf-8') as read_file, gzip.open(temp_file_path, 'wt',
                                                                                  encoding='utf-8') as write_file:
            for line in follow(read_file):
                try:
                    json_data = json.loads(line)
                    if not contains_keyword(json_data, keywords):
                        write_file.write(line)
                except json.JSONDecodeError:
                    # Write the line as is if it's not valid JSON
                    write_file.write(line)

        os.replace(temp_file_path, file_path)
        print(f"File {file_path} has been processed.")
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")

def process_json_file(file_path, keywords):
    if not os.path.isfile(file_path):
        print(f"JSON file {file_path} does not exist.")
        return

    try:
        temp_file_path = file_path + ".tmp"

        with open(file_path, 'r') as read_file, open(temp_file_path, 'w') as write_file:
            json_lines = follow(read_file)
            for line in json_lines:
                try:
                    json_data = json.loads(line)
                    if not contains_keyword(json_data, keywords):
                        write_file.write(line)
                except json.JSONDecodeError:
                    # Write the line as is if it's not valid JSON
                    write_file.write(line)

        os.replace(temp_file_path, file_path)
        print(f"File {file_path} has been processed.")
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")


def process_file(file_path, keywords_to_delete):
    if file_path.endswith('.json'):
        process_json_file(file_path, keywords_to_delete)
    elif file_path.endswith('.log'):
        process_logs_file(file_path, keywords_to_delete)

# iterate all the files in the log_path, and call process_json_file and process_logs_file separately
def logs_cleansing_delete(log_path, keywords_to_delete, max_processes=4):
    files_to_multiprocess = []
    # Iterate through all files in the directory
    for filename in os.listdir(log_path):
        if filename.endswith('.json'):
            file_path = os.path.join(log_path, filename)
            files_to_multiprocess.append(file_path)
        elif filename.endswith('.log'):
            file_path = os.path.join(log_path, filename)
            files_to_multiprocess.append(file_path)
        elif filename.endswith('.json.gz'):
            file_path = os.path.join(log_path, filename)
            process_json_gz_file(file_path, keywords_to_delete)
        elif filename.endswith('.log.gz'):
            file_path = os.path.join(log_path, filename)
            process_logs_gz_file(file_path, keywords_to_delete)

    # Using multiprocessing Pool to process files in parallel
    with Pool(max_processes) as pool:
        pool.starmap(process_file, [(file, keywords_to_delete)
                                    for file in files_to_multiprocess])



if __name__ == "__main__":
    # Initialize Elasticsearch client
    es = Elasticsearch(['<your elasticsearch instance IP>'], timeout=7200)

    # which index to perform cleansing delete
    index_name = "so"

    # trigger delete by query
    delete_query = "<provide the delete query you want to delete>>"
    # example: {"query": {"range": {"creationDate": {"gte": "2009-01-01T00:00:00", "lte" : "2014-03-09T04:10:50"}}}}
    delete_by_query(es, "so", delete_query)

    # trigger delete by document id
    doc_id = "<provide the document id you want to delete>"
    delete_document_by_id(es, index_name, doc_id)

    # trigger force merge
    forcemerge(es, index_name)

    # clear elasticsearch cache
    response = es.indices.clear_cache(index=index_name)
    print(response)

    # clear translog
    response = es.indices.flush(index=index_name)
    print(response)

    # trigger cleansing delete in snapshots
    snapshot_client = SnapshotClient(es)
    snapshot_repo = "backup-repo"  # Provide snapshot repository name
    snapshots = ["snapshot-1"]  # Provide the list of snapshot names

    # trigger cleansing delete in elasticsearch log (.json, .log, .json.gz, .log,gz)
    log_path = "<provide your log path here>"  # change this
    keywords_to_delete = ["<delete keyword here>"]  # provide personal data keyword exmple: user1937021 (1937021)
    max_processes = 4
    logs_cleansing_delete(log_path, keywords_to_delete, max_processes)

