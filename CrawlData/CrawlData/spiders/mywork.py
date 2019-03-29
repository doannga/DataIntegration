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
class MyworkSpider(scrapy.Spider):
    name = 'mywork'
    start_urls = ['https://mywork.com.vn/tuyen-dung/']
    collection_name = 'News'
    client = pymongo.MongoClient(settings.MONGO_URI)
    db = client[settings.MONGO_DATABASE]
    collection = db[collection_name]
    Y_in_db = list(collection.find({}, {"title":1,"company":1, "address":1, "_id":0}))
    no_duplicate_items = 0

    def parse(self, response):
        for tn in response.xpath('//div[@class="row job-item"]/div/div/div/p'):
            src = tn.xpath('a/@href').extract_first()
            src = response.urljoin(src)
            yield scrapy.Request(src, callback=self.parse_src)

        next_pages = response.xpath('//a[@class="page-link" and ./span[contains(text(),"Trang sau")]]/@href').extract()
        next_page = next_pages[len(next_pages)-1]
        if next_page is not None:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(next_page, callback=self.parse)
            pass
    def parse_src(self, response):
        self.item = JobItem()
        self.item["url"] = response.request.url
        title = response.xpath('//h1[@class="main-title"]//span/text()').extract()
        x_title = title[0].strip()
        if len(title) > 0:
            self.item["title"] = x_title

        #Cong ty
        company = response.xpath('(//h2[@class="desc-for-title mb-15"]//span)[1]/text()').extract()
        x_company = company[0].strip()
        if len(company) > 0:
            self.item["company"] = x_company
         #Noi lam viec
        address = response.xpath('//span[@class="el-tag location_tag el-tag--primary"]//a/text()').extract()
        x_address = ", ".join([add.strip() for add in address])
        if len(address) > 0:
            self.item["address"] = x_address
        #Check duplicate 
        data_need_check = DataReduction(3, [[job['title'], job['company'], job['address']] for job in self.Y_in_db])      
        if data_need_check.is_match([x_title, x_company, x_address]):
            self.no_duplicate_items += 1
            print(self.no_duplicate_items)
            return
        #Luong
        salary = response.xpath('//span[@class="text_red"]/text()')
        if len(salary) > 0:
            salary_str = " ".join(salary)
            salary_need_normalize = Normalize_salary()
            salary_normalized = salary_need_normalize.normalize_salary(salary_str)
            self.item["salary"] = salary_normalized
        else:
            self.item["salary"] = 'Thỏa thuận'
        #Nganh nghe
        career = response.xpath('//span[@class="el-tag el-tag--primary"]//a/text()').extract() 
        career_need_nomarlize = Normalize_careers()
        career_normalized = career_need_nomarlize.normalize_careers(career)
        self.item["career"] = career_normalized

        #Kinh nghiem    
        experience = response.xpath('(//div[@class="item item1"]//p)[1]/text()').extract()
        if len(experience) > 0:
            self.item["experience"] = experience[0].strip()
        else:
            self.item['experience'] = "Không yêu cầu"
        #Yeu cau khac
        require_skill1 = response.xpath('(//div[@class="mw-box-item"])[3]/text()').extract()
        require_skill2 = response.xpath('(//div[@class="mw-box-item"])[4]/text()').extract()
        require_skill = require_skill11.extend(require_skill2)
        if len(require_skill) > 0:
            self.item["require_skill"] = ", ".join([re_sk.strip() for re_sk in require_skill])
        else:
            self.item['require_skill'] = "Không yêu cầu"
        # Bang cap
        diploma = [info_other.strip() for info_other in require_skill if
                   "bằng cấp" in info_other.lower() or "tốt nghiệp" in info_other.lower()] 
        if len(diploma) > 0:
            self.item['diploma'] = diploma[0].split(':')[-1].strip()
        else:
            self.item['diploma'] = 'Không yêu cầu'
         #So luong
        amount = response.xpath('(//div[@class="item item1"]//p)[3]/text()').extract()
        if len(amount) > 0:
            self.item["amount"] = amount[0].strip()
        else:
            self.item['amount'] = 'Không yêu cầu'
        
        
         # Chuc vu   
        position = response.xpath('(//div[@class="item item2"]//p)[2]/text()').extract()
        if len(position) > 0:
            self.item["position"] = position[0].strip()
        else:
            self.item['amount'] = 'Không yêu cầu'

        #Hinh thuc lam viec fulltime/parttime
        category = response.xpath('(//div[@class="item item2"]//p)[1]/text()').extract()
        if len(category) > 0:
            self.item["category"] = category[0].strip()
        else:
            self.item['category'] = 'Không yêu cầu'
        #Thoi gian thu viec
        trial_time = ""
        self.item["trial_time"] = trial_time
        #Yeu cau gioi tinh
        sex = response.xpath('(//div[@class="item item2"]//p)[3]/text()').extract()
        if len(sex) > 0:
            self.item["sex"] = sex[0]
            pass
        #Yeu cau tuoi 
        age = ""
        self.item["age"] = age

        #Mo ta
        description =  response.xpath('(//div[@class="mw-box-item"])[1]/text()').extract()
        if len(description) > 0:
            self.item["description"] = description[0]
            pass
        #Quyen loi duoc huong
        benefits = response.xpath('(//div[@class="mw-box-item"])[2]/text()').extract()
        if len(benefits) > 0:
            self.item["benefits"] = benefits[0]
            pass
        
        #Thong tin lien he
        per_contact = response.xpath('(//div[@class="col-md-6 col-lg-9 item"]//span)[1]/text()').extract()
        add_contact = response.xpath('(//div[@class="col-md-6 col-lg-9 item"]//span)[2]/text()').extract()
        if len(per_contact) > 0 :
            pers_contact = re.sub(r'<.*?>', ' ', per_contact[0])
            pers_contact = re.sub(r'\n', ' ', pers_contact)
            pers_contact = re.sub(r'\r', ' ', pers_contact)
            pass
        else:
            pers_contact = ""
        if len(add_contact) > 0:
            addr_contact = re.sub(r'<.*?>', ' ', add_contact[0])
            addr_contact = re.sub(r'\n', ' ', addr_contact)
            addr_contact = re.sub(r'\r', ' ', addr_contact)
            pass
        else:
            addr_contact = ""
        # contact = pers_contact + "\n" +addr_contact
        contact = u"Người liên hệ: " + pers_contact + u"\n" +u"Địa chỉ liên hệ: " + addr_contact
        self.item["contact"] = contact
        
        #Han nop ho so
        expired = response.xpath('//div[@class="col-md-7"]//span[@class=""]/text()').extract() #Het han
        if len(expired) > 0:
            self.item["expired"] = expired[0]
            pass
        #Ngay tao hoso
        created = response.xpath('(//div[@class="content text-center pt-30"]//p)[2]/text()').extract() #Ngay tao
        if len(created) > 0:
            self.item["created"] = created[0]
        if self.item["title"] != "":
            yield self.item
            pass
