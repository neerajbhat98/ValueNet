# -*- coding: utf-8 -*-
"""
Created on Wed Jul  1 18:06:57 2020

@author: neera
"""


import os
import pickle
import sqlite3
from pprint import pprint

import torch
import json
from config import read_arguments_manual_inference
from intermediate_representation import semQL
from intermediate_representation.sem2sql.sem2SQL import transform
from intermediate_representation.sem_utils import alter_column0
from model.model import IRNet
from named_entity_recognition.api_ner.google_api_repository import remote_named_entity_recognition
from named_entity_recognition.pre_process_ner_values import pre_process, match_values_in_database
from preprocessing.process_data import process_datas
from preprocessing.utils import merge_data_with_schema
from spider import spider_utils
from spider.example_builder import build_example
from utils import setup_device, set_seed_everywhere

from spacy.lang.en import English

from termcolor import colored


def _inference_semql(data_row, schemas, model):
    example = build_example(data_row, schemas)

    with torch.no_grad():
        results_all = model.parse(example, beam_size=1)
    results = results_all[0]
    # here we set assemble the predicted actions (including leaf-nodes) as string
    full_prediction = " ".join([str(x) for x in results[0].actions])

    prediction = example.sql_json['pre_sql']
    prediction['model_result'] = full_prediction

    return prediction, example


def _tokenize_question(tokenizer, question):
    # Create a Tokenizer with the default settings for English
    # including punctuation rules and exceptions

    question_tokenized = tokenizer(question)

    return [str(token) for token in question_tokenized]


def _pre_process_values(row):
    # ner_results = remote_named_entity_recognition(row['question'])
    # row['ner_extracted_values'] = ner_results['entities']

    extracted_values = pre_process(row)

    row['values'] = match_values_in_database(row['db_id'], extracted_values)

    return row


def _semql_to_sql(prediction, schemas):
    alter_column0([prediction])
    result = transform(prediction, schemas[prediction['db_id']])
    return result[0]


def _execute_query(sql, db):
    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    cursor.execute(sql)
    result = cursor.fetchall()

    conn.close()

    return result


def _remove_spaces(sentence):
    s = sentence.strip().split()
    s = " ".join(s)
    return s

def _find_nums(sentence):
    nums = []
    for word in sentence.split():
        if word.isnumeric():
            nums.append(word)
    return nums

def load_static(args):
    device, n_gpu = setup_device()
    set_seed_everywhere(args.seed, n_gpu)
   
    schemas_raw, schemas_dict = spider_utils.load_schema(args.data_dir)
    
    grammar = semQL.Grammar()
    model = IRNet(args, device, grammar)
    model.to(device)
    # load the pre-trained parameters
    model.load_state_dict(torch.load(args.model_to_load, map_location=torch.device('cpu')))
    model.eval()
    print("Load pre-trained model from '{}'".format(args.model_to_load))
    
    nlp = English()
    tokenizer = nlp.Defaults.create_tokenizer(nlp)
    
    with open(os.path.join(args.conceptNet, 'english_RelatedTo.pkl'), 'rb') as f:
        related_to_concept = pickle.load(f)
    
    with open(os.path.join(args.conceptNet, 'english_IsA.pkl'), 'rb') as f:
        is_a_concept = pickle.load(f)
    
    return args,grammar,model,nlp,tokenizer,related_to_concept,is_a_concept, schemas_raw, schemas_dict

def predict(question,args,grammar,model,nlp,tokenizer,related_to_concept,is_a_concept, schemas_raw, schemas_dict):
        question = _remove_spaces(question)
        nums = _find_nums(question)
        row = {
                'question': question,
                'query': 'DUMMY',
                'db_id': args.database,
                'question_toks': _tokenize_question(tokenizer, question)
              }
        data, table = merge_data_with_schema(schemas_raw, [row])
        pre_processed_data = process_datas(data, related_to_concept, is_a_concept)
        pre_processed_with_values = _pre_process_values(pre_processed_data[0])
    
        for num in nums:
            if num not in row['values']:
                row['values'].append(num)
        prediction, example = _inference_semql(
                pre_processed_with_values, schemas_dict, model)
        sql = _semql_to_sql(prediction, schemas_dict)
        return sql
        
