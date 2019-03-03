# -*- coding: utf-8 -*-
import scrapy
from CrawlData.items import JobItem
import re
import time
from pyvi import ViTokenizer
from py_stringmatching.similarity_measure.soft_tfidf import SoftTfIdf
from py_stringmatching.similarity_measure.cosine import Cosine
import os
import pymongo
from collections import defaultdict
import pybloom_live
class Vieclam24hQlSpider(scrapy.Spider):
    name = 'vieclam24h_QL'
    start_urls = [
        'https://vieclam24h.vn/tim-kiem-viec-lam-nhanh/?hdn_nganh_nghe_cap1=&hdn_dia_diem=&hdn_tu_khoa=&hdn_hinh_thuc=&hdn_cap_bac=',   
    ]
    ur = pybloom_live.BloomFilter(capacity=2097152, error_rate=0.005)
    cookies = [
        {
            'gate_nganh': 14,
            '_gid': 'GA1.2.188974254.1551174426',
            '_ga': 'GA1.2.1690984599.1551174426',
            'gate': 'vlql'
        },
        {
            'gate_nganh': 14,
            '_gid': 'GA1.2.188974254.1551174426',
            '_ga': 'GA1.2.1690984599.1551174426',
            'gate': 'vlcm'
        },
        {
            'gate_nganh': 14,
            '_gid': 'GA1.2.188974254.1551174426',
            '_ga': 'GA1.2.1690984599.1551174426',
            'gate': 'ldpt'
        },
        {
            'gate_nganh': 14,
            '_gid': 'GA1.2.188974254.1551174426',
            '_ga': 'GA1.2.1690984599.1551174426',
            'gate': 'sv'
        }

    ]
    collection_name = 'News'
    JACCARD_MEASURE = 0.7
    PREFIX_FILTERING = 2
    SIMILARITY_THRESHOLD = 0.75
    def parse(self, response):
        
        for tn in response.xpath('//div[@class="list-items "]/div/div/span'):
            src = tn.xpath('a/@href').extract_first()
            src = response.urljoin(src)
            add_url = self.ur.add(src)
            if add_url is False:
                yield scrapy.Request(src, callback=self.parse_src) 

        next_pages = response.xpath('//li[@class="next"]/a/@href').extract()
        next_page = next_pages[len(next_pages) - 1]
        
        if next_page is not None:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(next_page, callback=self.parse)
        # Time end
        print(time.strftime("%H:%M:%S", time.localtime()))
    def parse_src(self, response):
        self.item = JobItem()
        self.item["url"] = response.request.url
        title = response.xpath('//div[@class="col-xs-12"]/h1[@class="text_blue font28 mb_10 mt_20 fws title_big"]/text()').extract()
        x_title = title[0]
        #Cong ty
        company = response.xpath('//p[@class="font16"]//a[@class="text_grey3"]/text()').extract()
        x_company = company[0]
        #Connect MongoDB
        client = pymongo.MongoClient(self.settings.get("MONGO_URI"))
        db = client[self.settings.get("MONGO_DATABASE")]
        collection = db[self.collection_name]
        Y_title_in_db = list(collection.find({}, {"title":1,"company":1, "_id":0}))
        #Tach tu
        X_title_split = [self.word_split(x_title)]
        # Y_title_split = [self.word_split(y.get("title")) for y in Y_title_in_db]
        X_company_split = [self.word_split(x_company)]
        # Y_company_split = [self.word_split(y_c.get("company")) for y_c in Y_title_in_db]
        Y_company_split = []
        Y_title_split = []
        for y in Y_title_in_db:
            y_ti = y['title']
            y_title_split = self.word_split(y_ti)
            Y_title_split = y_title_split
            y_co = y["company"]
            y_company_split = self.word_split(y_co)
            Y_company_split = y_company_split

        #Chuan hoa ve chu thuong
        X_title_normalize = [self.word_normalize(x) for x in X_title_split]
        Y_title_normalize = [self.word_normalize(y) for y in Y_title_split]
        X_company_normalize = [self.word_normalize(x) for x in X_company_split]
        Y_company_normalize = [self.word_normalize(y) for y in Y_company_split]

        #Danh chi muc nguoc cho tap y
        Y_title_inverted_index = self.invert_index(Y_title_normalize)
        Y_company_inverted_index = self.invert_index(Y_company_normalize)

        #Tinh do tuong tu SoftTfIdf
        softTfIdf_title = SoftTfIdf(X_title_normalize + Y_title_normalize)
        softTfIdf_company = SoftTfIdf(X_company_normalize + Y_company_normalize)
        #Tim cac chuoi co x khop trong Y
        Y_size_filtering = self.size_filtering(X_title_normalize[0], Y_title_normalize, self.JACCARD_MEASURE)
        Y_size_filtering_company = self.size_filtering(X_company_normalize[0], Y_company_normalize, self.JACCARD_MEASURE)
        #Giam so ung cu vien trong tap Y khop voi x(Loai bo cac chuoi y trong Y ko khop voi x)
        Y_candidates = self.prefix_filtering(Y_title_inverted_index, X_title_normalize[0], Y_size_filtering, self.PREFIX_FILTERING)
        Y_company_candidates = self.prefix_filtering(Y_company_inverted_index, X_company_normalize[0], Y_size_filtering_company, self.PREFIX_FILTERING)
        flag_title = False
        for y in Y_candidates:
            if softTfIdf_title.get_raw_score(X_title_normalize[0],y) >= self.SIMILARITY_THRESHOLD:
                flag_title = True
                break
        if flag_title == True:
            flag_company = False
            for y in Y_company_candidates:
                if softTfIdf_company.get_raw_score(X_company_normalize[0], y) >= self.SIMILARITY_THRESHOLD:
                    flag_company = True
                    break
            if flag_company == False:
                self.item["title"] = x_title
                self.item["company"] = x_company
        else: 
            self.item["title"] = x_title
            self.item["company"] = x_company

        #Luong
        salary = response.xpath('//div[@class="col-xs-6"]/p')
        if len(salary) > 0:
            salary = salary[0].xpath('span[@class="pl_28"]/span/text()').extract()
            if len(salary) > 0:
                self.item["salary"] = re.sub(r'<.*?>', '. ',salary[0])

        #Kinh nghiem    
        experience = response.xpath('((//div[@class="col-xs-6"])[1]//span[@class="pl_28"])[2]//span[@class="job_value"]/text()').extract()
        if len(experience) > 0:
            self.item["experience"] = experience[0]
            pass
         # Bang cap
        diploma = response.xpath('((//div[@class="col-xs-6"])[1]//span[@class="pl_28"])[3]//span[@class="job_value"]/text()').extract()
        if len(diploma) > 0:
            self.item["diploma"] = diploma[0]
            pass
         #So luong
        amount = response.xpath('((//div[@class="col-xs-6"])[1]//span[@class="pl_28"])[4]//span[@class="job_value"]/text()').extract()
        if len(amount) > 0:
            self.item["amount"] = amount[0]
            pass
         #Nganh nghe
        career = response.xpath('//h2[@class="pl_28 font14 fwb"]//a[@class="job_value text_pink"]/text()').extract() 
        if len(career) > 0:
            self.item["career"] = career
            pass
         #Noi lam viec
        address = response.xpath('//span[@class="pl_28"]//a[@class="job_value text_pink"]/text()').extract()
        if len(address) > 0:
            self.item["address"] = address
            pass
         # Chuc vu   
        position = response.xpath('((//div[@class="col-xs-6"])[2]//span[@class="pl_28"])[2]//span[@class="job_value"]/text()').extract()
        if len(position) > 0:
            self.item["position"] = position[0]
            pass

        #Hinh thuc lam viec fulltime/parttime
        category = response.xpath('((//div[@class="col-xs-6"])[2]//span[@class="pl_28"])[3]//span[@class="job_value"]/text()').extract()
        if len(category) > 0:
            self.item["category"] = category[0]
            pass
        #Thoi gian thu viec
        trial_time = response.xpath('((//div[@class="col-xs-6"])[2]//span[@class="pl_28"])[4]//span[@class="job_value"]/text()').extract()
        if len(trial_time) > 0:
            self.item["trial_time"] = trial_time[0]
            pass
        #Yeu cau gioi tinh
        sex = response.xpath('((//div[@class="col-xs-6"])[2]//span[@class="pl_28"])[5]//span[@class="job_value"]/text()').extract()
        if len(sex) > 0:
            self.item["sex"] = sex[0]
            pass
        #Yeu cau tuoi 
        age = response.xpath('((//div[@class="col-xs-6"])[2]//span[@class="pl_28"])[6]//span[@class="job_value"]/text()').extract() 
        if len(age) > 0:
            self.item["age"] = age[0]
            pass

        #Mo ta
        description =  response.xpath('(//div[@id="ttd_detail"]//div[@class="item row"])[1]//p[@class="col-md-9 pr_0 mb_0 word_break"]/text()').extract()
        self.item["description"] = " ".join([des.strip() for des in description])
        #Quyen loi duoc huong
        benefits = response.xpath('(//div[@id="ttd_detail"]//div[@class="item row"])[2]//p[@class="col-md-9 pr_0 mb_0 word_break"]/text()').extract()
        self.item["benefits"] = " ".join([benefit.strip() for benefit in benefits])
        #Yeu cau khac
        require_skills = response.xpath('(//div[@id="ttd_detail"]//div[@class="item row"])[3]//p[@class="col-md-9 pr_0 mb_0 word_break"]/text()').extract()
        self.item["require_skill"] = " ".join([require_skill.strip() for require_skill in require_skills])
        #Thong tin lien he
        per_contact = response.xpath('(//div[@class="job_description bg_white pl_24 pr_24 mt_16 pb_18 box_shadow"]//div[@class="item row pt_14 pb_14"])[1]//p[@class="col-md-9 pr_0 mb_0"]/text()').extract()
        add_contact = response.xpath('(//div[@class="job_description bg_white pl_24 pr_24 mt_16 pb_18 box_shadow"]//div[@class="item row pt_14 pb_14"])[2]//p[@class="col-md-9 pr_0 mb_0"]/text()').extract()
        
        contact = u"Người liên hệ: " + per_contact[0].strip() + u" Địa chỉ liên hệ: " + add_contact[0].strip()
        self.item["contact"] = contact
        
        #Han nop ho so
        expired = response.xpath('(//span[@class="text_pink"])[1]/text()').extract() #Het han
        if len(expired) > 0:
            self.item["expired"] = expired[0]
            pass
        #Ngay tao hoso
        created = response.xpath('(//p[@class="text_grey2 font12 mt8 mb12"]//span)[3]/text()').extract() #Ngay tao
        if len(created) > 0:
            created_at = created[0][14:]
            self.item["created"] = created_at
        if self.item["title"] != None:
            yield self.item
    #Tach tu
    def word_split(self, text):
        return re.split("[^\w_]+", ViTokenizer.tokenize(text))
    # Chuan hoa tu ve chu thuong
    def word_normalize(self, text):
        return [word.lower() for word in text]
    # Danh chi muc nguoc cho string
    def invert_index(self, str_list):
        inverted = {}
        for i, s in enumerate(str_list):
            for word in s:
                locations = inverted.setdefault(word, [])
                locations.append(i)
        return inverted
    #Tinh size_filtering
    def size_filtering(self, x, Y, JACCARD_MEASURE):
        up_bound = len(x) / JACCARD_MEASURE
        down_bound = len(x) * JACCARD_MEASURE
        return [y for y in Y if down_bound <= len(y) <= up_bound]
    #Tinh prefix_filtering
    def prefix_filtering(self,inverted_index, x, Y, PREFIX_FILTERING):
        if len(x) >= PREFIX_FILTERING:
            # Sort x, y in Y
            x_ = self.sort_by_frequency(inverted_index, x)
            Y_ = [self.sort_by_frequency(inverted_index, y)[:len(y) - PREFIX_FILTERING + 1] for y in Y if
                  len(y) >= PREFIX_FILTERING]
            Y_inverted_index = self.invert_index(Y_)
            Y_filtered_id = []
            for x_j in x_[:len(x) - PREFIX_FILTERING + 1]:
                Y_filtered_id += Y_inverted_index.get(x_j) if Y_inverted_index.get(x_j) is not None else []
            Y_filtered_id = set(Y_filtered_id)
            return [Y[i] for i in Y_filtered_id]
        else:
            return []
    def sort_by_frequency(self,inverted_index, arr):
        return sorted(arr,key=lambda arr_i: len(inverted_index.get(arr_i) if inverted_index.get(arr_i) is not None else []))


        

        	
