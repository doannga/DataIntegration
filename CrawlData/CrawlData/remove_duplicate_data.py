from pyvi import ViTokenizer
import re
from py_stringmatching.similarity_measure.soft_tfidf import SoftTfIdf
from py_stringmatching.similarity_measure.cosine import Cosine
import json
import time
import os

class Check_duplicate:
	def __init__(self, no_fields, Y, jaccard_measure=0.8, similarity_threshold=0.9,frefix_filtering = 2):
		self.no_fields = no_fields
		self.jaccard_measure = jaccard_measure
		self.similarity_threshold = similarity_threshold
		self.frefix_filtering = frefix_filtering
		self.size = len(Y)

		#Tach tu va chuan hoa tap Y
		self.Y_normalize = []
		for y in Y:
			y_split = []
			for i in range(no_fields):
				y_split.append(self.word_nomalize(self.word_split(y[i])))
			self.Y_normalize.append(y_split)
		#xay dung chi muc nguoc cho tap Y
		self.Y_index = []
		Y_fields = [[] for i in range(no_fields)]
		for y in self.Y_normalize:
			for i in range(no_fields):
				Y_fields[i].append(y[i])
		for i in range(no_fields):
			self.Y_index.append(self.invert_index(Y_fields[i]))
		self.soft_tf_idf = []
		for i in range(no_fields):
			self.soft_tf_idf.append(SoftTfIdf(Y_fields[i]))	
	# Tach tu
	@staticmethod
	def word_split(text):
		return re.compile("[\\w_]+").findall(ViTokenizer.tokenize(text))

	# Chuan hoa tu
	@staticmethod
	def word_nomalize(text):
		return [word.lower() for word in text]

	# Xay dung chi muc nguoc
	@staticmethod
	def invert_index(str_list):
		inverted = {}
		for i, s in enumerate(str_list):
			for word in s:
				locations = inverted.setdefault(word, [])
				locations.append(i)
		return inverted

	def size_filtering(self,x, Y):
		up_bound = len(x) / self.jaccard_measure
		down_bound = len(x) * self.jaccard_measure
		return [y for y in Y if down_bound <= len(y) <= up_bound]

	def prefix_filtering(self, inverted_index, x, Y):
		if len(x) >= self.frefix_filtering:
			x_ = self.sort_by_frequency(inverted_index, x)
			Y_ = [self.sort_by_frequency(inverted_index, y)[:len(y) - self.f + 1] for y in Y if len(y) >= self.frefix_filtering]
			Y_inverted_index = self.invert_index(Y_)
			Y_filtered_id = []
			for x_j in x_[:len(x) - self.frefix_filtering + 1]:
				Y_filtered_id += Y_inverted_index.get(x_j) if Y_inverted_index.get(x_j) is not None else []
			Y_filtered_id = set(Y_filtered_id)
			return [Y[i] for i in Y_filtered_id]
		else:
			return []
	@staticmethod
	def sort_by_frequency(inverted_index, arr):
		return sorted(arr,key=lambda arr_i: len(inverted_index.get(arr_i) if inverted_index.get(arr_i) is not None else []))
	def is_match(self, x):
		x_normalize = [self.word_nomalize(self.word_split(x_)) for x_ in x]
		Y_size_filtering = self.size_filtering(x_normalize, self.Y_normalize)
		Y_candidates = self.prefix_filtering(self.Y_index, x, Y_size_filtering)
		flag = False
		for y in Y_candidates:
			inner_flag = True
			for i in range(self.no_fields):
				if self.soft_tf_idf[i].get_raw_score(x_normalize[i], y[i]) < self.similarity_threshold:
					inner_flag = False
					break
			if inner_flag:
				flag = True
		return flag