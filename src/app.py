import argparse
import json
from updated_manual_inference import load_static,predict
from flask import Flask,request
app = Flask(__name__)
##routing
class args:
    model_to_load = '../saved_model/best_model.pt'
    database='dealPlatform'
    seed=90
    data_set='spider'
    batch_size=1
    cuda=False
    conceptNet='../data/spider/conceptNet'
    encoder_pretrained_model='bert-base-uncased'
    max_seq_length=512
    column_pointer=True
    embed_size=300
    hidden_size=300
    action_embed_size=128
    att_vec_size=300
    type_embed_size=128
    col_embed_size=300
    readout='identity'
    column_att='affine'
    dropout=0.3
    beam_size=1
    decode_max_time_step=40
    data_dir='../data/spider'
    database_path='../data/spider/original/database/dealPlatform/dealPlatform.db'

args,grammar,model,nlp,tokenizer,related_to_concept,is_a_concept, schemas_raw, schemas_dict = load_static(args)
@app.route('/testing',methods=['GET'])
def testing():
    question = request.args['question']
    result = predict(question,args,grammar,model,nlp,tokenizer,related_to_concept,is_a_concept, schemas_raw, schemas_dict)
    data = {}
    data['sql'] = result
    return data