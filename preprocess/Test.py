
from pyvi import ViTokenizer
import json
import re
from sklearn.feature_extraction.text import TfidfVectorizer
import pickle


## doc du lieu
input_file = open ('../CrawlData/CrawlData/careerbuild.json')
json_array = json.load(input_file)
data_for_idf = []

## lay ra truong title
for item in json_array:
    title = re.sub('[.\?\!\#\@\-\[\]\(\)\/\+]', "",item["title"])
    x = ViTokenizer.tokenize(title)
    data_for_idf.append(x)
## khoi tao , train va luu model idf
vectorizer = TfidfVectorizer()
vectorizer.fit(data_for_idf)
pickle.dump(vectorizer,open("tfidf","wb"))

## khoi tao du lieu cho k-mean. 
## transfrom idf
output_idf = vectorizer.transform(data_for_idf)
## train va test kmean
from sklearn.cluster import KMeans
kmeans = KMeans(n_clusters=20, random_state=0)

kmeans.fit(output_idf)
pickle.dump(kmeans,open("kmean","wb"))

from sklearn.pipeline import Pipeline

clustering_data_pipeline = Pipeline([
    ('tfidf', vectorizer()),
    ('cluster', kmeans()),
])


