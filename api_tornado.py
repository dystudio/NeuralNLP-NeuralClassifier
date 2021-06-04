import os
import tornado.web
import tornado.ioloop

import codecs
import math
import numpy as np
import os
import sys
import json

import torch
from torch.utils.data import DataLoader

from config import Config
from dataset.classification_dataset import ClassificationDataset
from dataset.collator import ClassificationCollator
from dataset.collator import ClassificationType
from dataset.collator import FastTextCollator
from model.classification.drnn import DRNN
from model.classification.fasttext import FastText
from model.classification.textcnn import TextCNN
from model.classification.textvdcnn import TextVDCNN
from model.classification.textrnn import TextRNN
from model.classification.textrcnn import TextRCNN
from model.classification.transformer import Transformer
from model.classification.dpcnn import DPCNN
from model.classification.attentive_convolution import AttentiveConvNet
from model.classification.region_embedding import RegionEmbedding
from model.model_util import get_optimizer, get_hierar_relations
from predict import Predictor

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello , I love this world~\n")

    def post(self):
        sentence = self.get_argument('sentence')
        #lan=0 English lan=1 Chinese
        lan = self.get_argument('language')
        print('sentence passed is %s' %sentence)

        #sentence = '{"doc_label": ["Computer--MachineLearning--DeepLearning", "Neuro--ComputationalNeuro"],"doc_token": ["I", "love", "deep", "learning"],"doc_keyword": ["deep learning"],"doc_topic": ["AI", "Machine learning"]}'
        config = Config(config_file='conf/train.json')
        if lan == '0':
         config = Config(config_file='conf/train.json')
        if lan == '1':
         print('trains.json used')
         config = Config(config_file='conf/train2.json')
        predictor = Predictor(config)
        batch_size = config.eval.batch_size
        input_texts = []
        predict_probs = []
        is_multi = config.task_info.label_type == ClassificationType.MULTI_LABEL
        #TODO pass sentence as input_texts
        #for line in codecs.open(sys.argv[2], "r", predictor.dataset.CHARSET):
        #    input_texts.append(line.strip("\n"))
        #    epoches = math.ceil(len(input_texts)/batch_size)
        # for line in iter(sentence, "\n"):
        #     print('current line is %s' %line)
        #     input_texts.append(line.strip("\n"))
        #     epoches = math.ceil(len(input_texts)/batch_size)

        input_texts.append(sentence.strip("\n"))
        epoches = math.ceil(len(input_texts)/batch_size)

        print('input_texts needed to be predicted is %s' %input_texts)

        for i in range(epoches):
            batch_texts = input_texts[i*batch_size:(i+1)*batch_size]
            predict_prob = predictor.predict(batch_texts)
        for j in predict_prob:
            predict_probs.append(j)

        for predict_prob in predict_probs:
            if not is_multi:
                predict_label_ids = [predict_prob.argmax()]
            else:
                predict_label_ids = []
                predict_label_idx = np.argsort(-predict_prob)
                for j in range(0, config.eval.top_k):
                    if predict_prob[predict_label_idx[j]] > config.eval.threshold:
                        predict_label_ids.append(predict_label_idx[j])
            predict_label_name = [predictor.dataset.id_to_label_map[predict_label_id] \
                    for predict_label_id in predict_label_ids]
            
        self.write(";".join(predict_label_name) + "\n")

    def put(self):
        sentence = self.get_argument('sentence')
        self.write("hello , UPDATE\n " + sentence)

    def delete(self):
        self.write("hello , DELETE\n")


if __name__ == "__main__":
    settings = {
        'debug' : True,
        'static_path' : os.path.join(os.path.dirname(__file__) , "static") ,
        'template_path' : os.path.join(os.path.dirname(__file__) , "template") ,
    }

    application = tornado.web.Application([
        (r"/" , MainHandler),
    ] , **settings)
    application.listen(8080)
    tornado.ioloop.IOLoop.instance().start()