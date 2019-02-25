from pyvi import ViTokenizer
# from sklearn.pipeline import Pipeline

import pickle
# idf_model = pickle.load(open("tfidf","rb"))
# kmean_model = pickle.load(open("kmean","rb"))
# clustering_data_pipeline = Pipeline([
#     ('tfidf', idf_model),
#     ('cluster', kmean_model),
# ])
# pickle.dump(clustering_data_pipeline,open("clustering","wb"))
clustering = pickle.load(open("clustering","rb"))

test_title = ViTokenizer.tokenize("Chuyên_viên Tuyển_dụng")
print (clustering.predict([test_title])[0])