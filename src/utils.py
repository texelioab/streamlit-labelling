from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, bulk
from typing import Callable
import numpy as np
import os
import json
from iterstrat.ml_stratifiers import MultilabelStratifiedShuffleSplit
from dotenv import load_dotenv
import pandas as pd
from itertools import product
import random

def create_es_client(ELASTIC_HOST, ELASTIC_USER, ELASTIC_PASS) -> Elasticsearch:
    """Connect to ElasticSearch, using our API/client-pass. Run as, for instance, client = create_es_client(). client is used in any call to the database.

    Returns:
        Elasticsearch: the client.
    """ 
    elastic_client = Elasticsearch(
    cloud_id=ELASTIC_HOST,
    basic_auth=(ELASTIC_USER,ELASTIC_PASS)
)
    return elastic_client


def transform_field(client: Elasticsearch, index: str, field: str, transform_function: Callable, batch_size=1000) -> None:
    """Transforms a particular field (column) in a particular index (table), by a given function.
    For instance, if we want to add 1 to each entry in a field, enter transform_function = lambda x: x + 1.

    Args:
        client (Elasticsearch): Client connection to Elasticsearch.
        index (str): The index where the field we want to adjust exists.
        field (str): The desired field to be adjusted.
        transform_function (Callable): A function that acts on each field value and transforms it.
        batch_size (int, optional): Defaults to 1000. Batch size for accessing data. Max 10000, typically 1000 is a reasonable value.
    """        
    docs = []
    query = {"query": {"match_all": {}}}
    for hit in scan(client, index=index, query=query, size=batch_size):
        docs.append(hit)
    updates = []
    for doc in docs:
        new_value = transform_function(doc['_source'][field])
        update_action = {
            '_op_type': 'update',
            '_index': index,
            '_id': doc['_id'],
            'doc': {field: new_value}
        }
        updates.append(update_action)
        if len(updates) >= batch_size:
            bulk(client, updates)
            updates = []
    if updates:
        bulk(client, updates)



def insert_document(client: Elasticsearch, index: str, document: dict) -> None:
    """Enters a new document into a given index. The document is a dictionary with keys corresponding to the index fields.

    Args:
        client (Elasticsearch): Client connection to Elasticsearch.
        index (str): The index into which the document will be inserted.
        document (dict): A dictionary with keys corresponding to the index fields.
    """    
    client.index(index=index, document=document)


def match_query(identifier: dict) -> dict:
    """A function used elsewhere to search for documents with particular values in given fields.
    It is easy to either access an entire index or one parituclar document, less straight-forward collecting all documents which match a specific set of criteria.
    
    Args:
        identifier (dict): A dictionary which can have one or more keys, where each key corresponds to a field, where the values must match exactly.

    Returns:
        dict: A nested dictionary which is inputted as a query in functions calling the database.
    """    
    return {
        'query': {
            'bool': {
                'must': [
                    {'match': {key: value}} for key, value in identifier.items()
                ]
            }
        }
    }


def delete_document(client: Elasticsearch, index: str, identifier: dict, delete_all=False) -> None:
    """Deletes one or more documents from a given index.

    Args:
        client (Elasticsearch): Client connection to Elasticsearch.
        index (str): The index from which the document or documents will be deleted.
        identifier (dict): Identifier (see match_query above) is a dictionary, with as many keys as is necessary to select the desired documents. If identifier is empty, this will delete the entire index.
        delete_all (bool, optional): Defaults to False. If want to delete entire index, set to True, with identifier={}.

    Raises:
        ValueError: If identifier is empty, indicating a desire to delete the index, but delete_all is False, will raise a warning.
    """       
    if (not identifier and delete_all) or identifier:
        query = match_query(identifier)
        client.delete_by_query(index=index, body=query)
    else:
        raise ValueError("Refusing to delete all documents without explicit `delete_all` flag set to True.")


def search_document(client: Elasticsearch, index: str, identifier: dict, all=False, batch_size=1000) -> list:
    """Retrieves one or more documents from a given index. 

    Args:
        client (Elasticsearch): Client connection to Elasticsearch.
        index (str): The index from which the document or documents will be retrieved.
        identifier (dict): Identifier (see match_query above) is a dictionary, with as many keys as is necessary to select the desired documents. If identifier is empty, the function will retireve the entire index. Otherwise it will select all documents which match identifier, which could be one ore more doocuments.
        all (bool, optional): Defaults to False. Whether to return only the document, or also the '_id' and extra more general information.
        batch_size (int, optional): Defaults to 1000. Batch size for accessing data. Max 10000, typically 1000 is a reasonable value.

    Returns:
        list: A list of dictionaries, each dictionary corresponding to a document in the index.
    """    
    documents = []
    query = {"query": {"match_all": {}}} if not identifier else {"query": match_query(identifier)['query']}
    for hit in scan(client, index=index, query=query, size=batch_size):
        if not all:
            documents.append(hit['_source'])
        else:
            documents.append(hit)
    return documents


def insert_in_bulk(client: Elasticsearch, index: str, table: dict) -> None:
    """Inserts a large number of rows into an index.

    Args:
        client (Elasticsearch): Client connection to Elasticsearch.
        index (str): Name of the index into which data will be inputted.
        table (dict): List of dictionaries, where each dictionary represents a row in the table: keys correspond to columns.
    """    
    actions = [{"_index": index,"_source": document} for document in table]
    bulk(client, actions)


def create_index(client: Elasticsearch, index: str, table: dict) -> None:
    """Creates a new index, with a given name, with inserted documents.

    Args:
        client (Elasticsearch): Client connection to Elasticsearch.
        index (str): Name of the new index
        table (dict): List of dictionaries, where each dictionary represents a row in the table: keys correspond to columns.
    """    
    if not client.indices.exists(index=index):
        client.indices.create(index=index)
        insert_in_bulk(client, index, table)


def join_sl_and_los(client: Elasticsearch, include_parent_topic_label = True) -> list:
    """Returns a list of dictionaries which are joins on the sentence_labels and labelled_sentences indices.
    If one particular sentence has multiple entries in the sentence_labels index (due to having multiple labels) certain fields (such as 'topics') will have lists of labels. For the fields with lists, each index position corresponds to one document in sentence_labels.

    Args:
        client (Elasticsearch): Client connection to Elasticsearch.
        include_parent_topic_label (bool, optional): Defaults to True. Whether or not to include parent topics as labels for each sentence.

    Returns:
        list: A list of dictionaries, each dictionary corresponding to a sentence from labelled_sentences and its labels in sentence_labels
    """    
    joined = []
    d = {}
    ls = search_document(client, 'labelled_sentence',{},all=True)
    for i in ls:
        d[i['_id']] = [i['_source']]
    if include_parent_topic_label:
        ts = search_document(client, 'topic_entity',{})
        topic_to_parent = {}
        for t in ts:
            topic_to_parent[t['id']] = t['parent_topic_id']
    sentence_labels = search_document(client, 'sentence_label',{})
    for sentence_label in sentence_labels:
        d[sentence_label['sentence_id']].append(sentence_label)
        if include_parent_topic_label:
            p = sentence_label.copy()
            p['topic_id'] = topic_to_parent[sentence_label['topic_id']]
            if p['topic_id'] != 'none0' and p not in d[sentence_label['sentence_id']]:
                d[sentence_label['sentence_id']].append(p)
    sl_keys = list(sentence_label.keys())
    sl_keys.remove('sentence_id')
    for key in d.keys():
        if len(d[key]) > 1:
            sentence = d[key][0]
            sentence['sentence_id'] = key
            for k in sl_keys:
                sentence[k] = []
            for sentence_label in d[key][1:]:
                for k in sl_keys:
                    sentence[k].append(sentence_label[k])
            joined.append(sentence)
    return joined


def train_test_split_stratified(client: Elasticsearch, list_dictionary_documents: list, train_proportion: float, run_diagnostics: bool, batch_size=1000, random_state=42) -> list:
    """Splits data into train and test samples. Stratisfies: also ensuring that for each topic there is a proportional split between train and test as well.

    Args:
        client (Elasticsearch): Client connection to Elasticsearch.
        list_dictionary_documents (list): Input data. List of dictionaries, each dictionary representing a sentence with its labelled topics.
        train_proportion (float): [0,1], the proportion of sentences that goes into the training sample. Also the proportion of sentences with a particular topic that goes into the training sample.
        run_diagnostics (bool): If True, prints train/test split proportion of output data, as well as split for each topic.
        batch_size (int, optional): Defaults to 1000. Batch size for accessing data. Max 10000, typically 1000 is a reasonable value.
        random_state (int, optional): Defaults to 42. Random_state for split process, reproducibility.

    Returns:
        list: A list with two lists, first list is train sentences, second is test sentences.
    """        
    topics = search_document(client, 'topic_entity',{'type':'Topic'},batch_size=batch_size)
    topics.extend(search_document(client, 'topic_entity',{'type':'Subtopic'},batch_size=batch_size))
    topic_ids = [t['id'] for t in topics]
    parent_ids = [next((t for t in topics if t['id'] == k), None)['parent_topic_id'] for k in topic_ids]

    topics_binary = np.zeros((len(list_dictionary_documents),len(topic_ids)))
    for i, sentence in enumerate(list_dictionary_documents):
        for t in sentence['topic_id']:
            if t in topic_ids:
                topics_binary[i][topic_ids.index(t)] += 1
                if parent_ids[topic_ids.index(t)] != 'none0' and parent_ids[topic_ids.index(t)] not in sentence['topic_id']:
                    topics_binary[i][topic_ids.index(parent_ids[topic_ids.index(t)])] += 1
    
    msss = MultilabelStratifiedShuffleSplit(n_splits=1, test_size=1-train_proportion, random_state=random_state)
    for train_index, test_index in msss.split(list_dictionary_documents, topics_binary):
        train_sentences = [list_dictionary_documents[i] for i in train_index]
        test_sentences = [list_dictionary_documents[i] for i in test_index]
    
    if run_diagnostics:
        topics_binary_train = np.zeros((len(train_sentences),len(topic_ids)))
        for i, sentence in enumerate(train_sentences):
            for t in sentence['topic_id']:
                if t in topic_ids:
                    topics_binary_train[i][topic_ids.index(t)] += 1
                    if parent_ids[topic_ids.index(t)] != 'none0' and parent_ids[topic_ids.index(t)] not in sentence['topic_id']:
                        topics_binary_train[i][topic_ids.index(parent_ids[topic_ids.index(t)])] += 1

        topics_binary_test = np.zeros((len(test_sentences),len(topic_ids)))
        for i, sentence in enumerate(test_sentences):
            for t in sentence['topic_id']:
                if t in topic_ids:
                    topics_binary_test[i][topic_ids.index(t)] += 1
                    if parent_ids[topic_ids.index(t)] != 'none0' and parent_ids[topic_ids.index(t)] not in sentence['topic_id']:
                        topics_binary_test[i][topic_ids.index(parent_ids[topic_ids.index(t)])] += 1
        
        train_counts = np.sum(topics_binary_train, axis=0)
        test_counts = np.sum(topics_binary_test, axis=0)

        print(f'Train proportion: {len(train_sentences)/(len(train_sentences) + len(test_sentences))}')
        print(f'Topic train proportions: {np.array(train_counts)/(np.array(train_counts)+np.array(test_counts))}')
    return [train_sentences, test_sentences]


def list_dictionary_documents_to_json(client: Elasticsearch, list_dictionary_documents: list, json_name: str, destination: str) -> None:
    """Takes a list of dictionaries and writes to a json. Can be used both after calling simply fetch_all_content, or after join_ls_and_sl

    Args:
        client (Elasticsearch): Client connection to Elasticsearch.
        list_dictionary_documents (list): Data: the list of dictionaries outputted both by search_document and join_ls_and_sl (or anything of similar structure).
        json_name (str): The name of the file
        destination (str): Path to file. If in the same repository, ''; if in another directory, path it by, say, 'data\jsons\'.
    """    
    with open(os.path.realpath(f'{destination}{json_name}.json'), 'w', encoding='utf-8') as f:
        json.dump(list_dictionary_documents, f, ensure_ascii=False, indent=4)

def co_labelling_grid(client: Elasticsearch):
    """Creates a csv file "grid.csv", which contains, for each topic-topic pair, the number of sentences they have both been labelled in.
    
    Args:
        client (Elasticsearch): Client connection to Elasticsearch.
    """        
    topics = []
    d = {}
    l = search_document(client, 'topic_count_visualisation',{})
    for i in l:
        if i['topic_name'] not in topics:
            topics.append(i['topic_name'])
        if i['sentence_id'] not in d.keys():
            d[i['sentence_id']] = [i['topic_name']]
        else:
            d[i['sentence_id']].append(i['topic_name'])
    topics.sort()
    ar = np.zeros((len(topics),len(topics)))
    for k in d.keys():
        if len(d[k]) > 1:
            p = list(product(d[k], repeat=2))
        else:
            p = [(d[k][0],d[k][0])]
        for i in p:
            ar[topics.index(i[0])][topics.index(i[1])] += 1
    np.fill_diagonal(ar,0)
    df = pd.DataFrame(ar,index=topics,columns=topics)
    df.to_csv('grid.csv',sep=',',index=True,encoding='utf-8')


def push_visualisation_data(client: Elasticsearch):
    """When new labelling has been performed, adding new labelled sentences to the index labelled_sentence, run this function so that the visualisation is updated.
    
    Args: 
        client (Elasticsearch): Client connection to Elasticsearch.
    """        
    labeller_type = {}
    l = []
    query = {"query": {"match_all": {}}}
    for hit in scan(client, index='labeller', query=query, size=1000):
        l.append(hit)
    for i in l:
        labeller_type[i['_id']] = i['_source']['type']
    l = search_document(client, 'topic_entity',{})
    id_name = {}
    parent = {}
    for i in l:
        id_name[i['id']] = i['name']
        parent[i['id']] = i['parent_topic_id']
    sl = search_document(client, 'sentence_label',{})
    tcv = search_document(client, 'topic_count_visualisation',{})
    sentence_id_topic_id = []
    for i in sl:
        if labeller_type[i['labeller_id']] == 'Human':
            sentence_id_topic_id.append((i['sentence_id'],i['topic_id']))
    for i in sentence_id_topic_id:
        if parent[i[1]] != 'none0' and (i[0],parent[i[1]]) not in sentence_id_topic_id:
            sentence_id_topic_id.append((i[0],parent[i[1]]))
    l = []
    for i in sentence_id_topic_id:
        if {'sentence_id':i[0],'topic_name':id_name[i[1]]} not in tcv:
            print(i, {'sentence_id':i[0],'topic_name':id_name[i[1]]})
            l.append({'sentence_id':i[0],'topic_name':id_name[i[1]]})
    if len(l) >0:
        insert_in_bulk(client, 'topic_count_visualisation',l)
    

if __name__ == "__main__":
    load_dotenv('credentials.env')
    ELASTIC_HOST=os.getenv('ELASTIC_HOST')
    ELASTIC_USER=os.getenv('ELASTIC_USER')
    ELASTIC_PASS=os.getenv('ELASTIC_PASS')
    client = create_es_client(ELASTIC_HOST, ELASTIC_USER, ELASTIC_PASS)
