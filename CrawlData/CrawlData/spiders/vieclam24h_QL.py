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
from CrawlData.remove_duplicate import DataReduction
from CrawlData.normalize_salary import Normalize_salary
from CrawlData.normalize_careers import Normalize_careers
import settings
class Vieclam24hQlSpider(scrapy.Spider):
    name = 'vieclam24h_QL'
    start_urls = [
        'https://vieclam24h.vn/tim-kiem-viec-lam-nhanh/?hdn_nganh_nghe_cap1=&hdn_dia_diem=&hdn_tu_khoa=&hdn_hinh_thuc=&hdn_cap_bac=',   
    ]
    ur = pybloom_live.BloomFilter(capacity=2097152, error_rate=0.005)  
    collection_name = 'News'
    client = pymongo.MongoClient(settings.MONGO_URI)
    db = client[settings.MONGO_DATABASE]
    collection = db[collection_name]
    Y_in_db = list(collection.find({}, {"title":1,"company":1, "address":1, "_id":0}))
    no_duplicate_items = 0
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
    def parse_src(self, response):
        self.item = JobItem()
        self.item["url"] = response.request.url
        title = response.xpath('//div[@class="col-xs-12"]/h1[@class="text_blue font28 mb_10 mt_20 fws title_big"]/text()').extract()
        x_title = title[0]
        self.item["title"] = x_title
        #Cong ty
        company = response.xpath('//p[@class="font16"]//a[@class="text_grey3"]/text()').extract()
        x_company = company[0]
        self.item['company'] = x_company
        #Noi lam viec
        addresses = response.xpath('//span[@class="pl_28"]//a[@class="job_value text_pink"]/text()').extract()
        address = ', '.join([address.replace("Việc làm", "").replace("TP.HCM", "Hồ Chí Minh").strip() for address in addresses])
        self.item['address'] = address

        #Check duplicate
        data_need_check = DataReduction(3, [[job['title'], job['company'], job['address']] for job in self.Y_in_db])                     
        if data_need_check.is_match([x_title, x_company, address]):
            self.no_duplicate_items += 1
            print(self.no_duplicate_items)
            return

        #Luong
        salary = response.xpath('//div[@class="col-xs-6"]//p//span[contains(text(),"Mức lương")]/span/text()').extract()
        if len(salary) > 0:
            salary_str = " ".join(salary)
            salary_need_normalize = Normalize_salary()
            salary_normalized = salary_need_normalize.normalize_salary(salary_str)
            self.item["salary"] = salary_normalized
        else:
            self.item["salary"] = 'Thỏa thuận'
            pass

        #Kinh nghiem    
        experience = response.xpath('//div[@class="col-xs-6"]//p//span[contains(text(),"Kinh nghiệm")]/span/text()').extract()
        if len(experience) > 0:
            self.item["experience"] = experience[0]
        else:
            self.item["experience"] = 'Không yêu cầu'
         # Bang cap
        diploma = response.xpath('//div[@class="col-xs-6"]//p//span[contains(text(),"Yêu cầu bằng cấp")]/span/text()').extract()
        if len(diploma) > 0:
            self.item["diploma"] = diploma[0]
        else:
            self.item['diploma'] = 'Không yêu cầu'
         #So luong
        amount = response.xpath('//div[@class="col-xs-6"]//p//span[contains(text(),"Số lượng cần tuyển")]/span/text()').extract()
        if len(amount) > 0:
            self.item["amount"] = amount[0]
        else:
            self.item['amount'] = 'Không yêu cầu'
         #Nganh nghe
        career = response.xpath('//div[@class="line-icon mb_12"]//h2[contains(text(),"Ngành nghề")]//a/text()').extract()      
        career_need_nomarlize = Normalize_careers()
        career_normalized = career_need_nomarlize.normalize_careers(career)
        self.item["career"] = career_normalized
        
         # Chuc vu   
        position = response.xpath('//div[@class="col-xs-6"]//p//span[contains(text(),"Chức vụ")]/span/text()').extract()
        if len(position) > 0:
            self.item["position"] = position[0]
        else:
            self.item['position'] = 'Không yêu cầu'

        #Hinh thuc lam viec fulltime/parttime
        category = response.xpath('//div[@class="col-xs-6"]//p//span[contains(text(),"Hình thức làm việc")]/span/text()').extract()
        if len(category) > 0:
            self.item["category"] = category[0]
        else:
            self.item['category'] = 'Không yêu cầu'
        #Thoi gian thu viec
        trial_time = response.xpath('//div[@class="col-xs-6"]//p//span[contains(text(),"Thời gian thử việc")]/span/text()').extract()
        if len(trial_time) > 0:
            self.item["trial_time"] = trial_time[0]
        else:
            self.item['trial_time'] = 'Không yêu cầu'
        #Yeu cau gioi tinh
        sex = response.xpath('//div[@class="col-xs-6"]//p//span[contains(text(),"Yêu cầu giới tính")]/span/text()').extract()
        if len(sex) > 0:
            self.item["sex"] = sex[0]
        else:
            self.item['sex'] = 'Không yêu cầu'
        #Yeu cau tuoi 
        age = response.xpath('//div[@class="col-xs-6"]//p//span[contains(text(),"Yêu cầu độ tuổi")]/span/text()').extract() 
        if len(age) > 0:
            self.item["age"] = age[0]
        else:
            self.item['age'] = 'Không yêu cầu'

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
        if self.item["title"] != "":
            yield self.item
   