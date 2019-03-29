# -*- coding: utf-8 -*-
import scrapy
from CrawlData.items import JobItem
import re
import pybloom_live
import pymongo
from pyvi import ViTokenizer
import time
from py_stringmatching.similarity_measure.soft_tfidf import SoftTfIdf
from py_stringmatching.similarity_measure.cosine import Cosine
from collections import defaultdict
import os
from CrawlData.remove_duplicate import DataReduction
from CrawlData.normalize_salary import Normalize_salary
from CrawlData.normalize_careers import Normalize_careers
import settings


class CareerbuilderSpider(scrapy.Spider):
    name = 'careerbuilder'
    start_urls = ['https://careerbuilder.vn/viec-lam/tat-ca-viec-lam-vi.html/']
    ur = pybloom_live.BloomFilter(capacity=2097152, error_rate=0.005)
    collection_name = 'News'
    # Y_in_db = []
    client = pymongo.MongoClient(settings.MONGO_URI)
    db = client[settings.MONGO_DATABASE]
    collection = db[collection_name]
    Y_in_db = list(collection.find({}, {"title":1,"company":1, "address":1, "_id":0}))
    no_duplicate_items = 0
    def parse(self, response):
        # client = pymongo.MongoClient(self.settings.get("MONGO_URI"))
        # db = client[self.settings.get("MONGO_DATABASE")]
        # collection = db[self.collection_name]
        # self.Y_in_db = list(collection.find({}, {"title":1,"company":1, "address":1, "_id":0}))
        # Time start
        for tn in response.xpath('//h3[@class="job"]'):
            src = tn.xpath('a/@href').extract_first()
            src = response.urljoin(src)
            add_url = self.ur.add(src)
            if add_url is False:
                yield scrapy.Request(src, callback=self.parse_src) 
                pass


        next_pages = response.xpath('//a[@class="right"]/@href').extract()
        next_page = next_pages[len(next_pages) - 1]
        if next_page is not None:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(next_page, callback=self.parse)
    def parse_src(self, response):
        self.item = JobItem()
        self.item["url"] = response.request.url
         #Nganh nghe
        career= response.xpath("//div[@id='showScroll']/ul/li/p/span[contains(text(), 'Ngành nghề')]/following-sibling::b/a/text()").extract() 
        print(career)
        career_need_nomarlize = Normalize_careers()
        career_normalized = career_need_nomarlize.normalize_careers(career)
        self.item["career"] = career_normalized

        #Tieu de
        title = response.xpath('//div[@class="top-job-info"]/h1/text()').extract()
        x_title = title[0]
        self.item["title"] = x_title
        company = response.xpath('//div[@class="tit_company"]/text()').extract()
        x_company = company[0]
        self.item["company"] = x_company
         #Noi lam viec
        address = response.xpath("//div[@id='showScroll']/ul/li/p/span[contains(text(), 'Nơi làm việc')]/following-sibling::b/a/text()").extract()
        add = ", ".join(address)
        self.item["address"] = ", ".join(address)
         
        data_need_check = DataReduction(3, [[job['title'], job['company'], job['address']] for job in self.Y_in_db])      
        
        #Check duplicate
        if data_need_check.is_match([x_title, x_company, add]):
            self.no_duplicate_items += 1
            print(self.no_duplicate_items)
            return
        
    	# Luong
        salary = response.xpath("//div[@id='showScroll']/ul/li/p/span[contains(text(), 'Lương')]/following-sibling::label/text()").extract()

        if len(salary) > 0:
            salary_str = " ".join(salary)
            salary_need_normalize = Normalize_salary()
            salary_normalized = salary_need_normalize.normalize_salary(salary_str)
            self.item["salary"] = salary_normalized
        else:
            self.item["salary"] = 'Thỏa thuận'
            pass
    	#Kinh nghiem    
        experience = response.xpath("//div[@id='showScroll']/ul/li/p/span[contains(text(), 'Kinh nghiệm')]/../text()").extract()
        if len(experience) > 0:
            self.item["experience"] = experience[0].strip() 
        elif not experience:
            self.item["experience"] = "Không yêu cầu"
        descriptions =  response.xpath("//div[@class='MarBot20']/h4[contains(text(),'Mô tả Công việc')]/following-sibling::div[@class='content_fck']//text()").extract()
        self.item["description"] = ' '.join([description.replace('-', '').strip() for description in descriptions])
        #Thong tin khac bao gom bang cap, do tuoi, va hinh thuc lam viec
        info_others = response.xpath("//div[@class='MarBot20']/h4[contains(text(),'Thông tin khác')]/following-sibling::div[@class='content_fck']/ul/li//text()").extract()
        # Bang cap
        diploma = [info_other.strip() for info_other in info_others if
                   "bằng cấp" in info_other.lower() or "tốt nghiệp" in info_other.lower()] + [
                      description.strip() for description in descriptions if "bằng cấp" in description.lower() or "tốt nghiệp" in description.lower()]
        if len(diploma) > 0:
            self.item['diploma'] = diploma[0].split(':')[-1].strip()
        else:
            self.item['diploma'] = 'Không yêu cầu'
         #So 0
        amount = ""
        self.item["amount"] = amount
        
        
         # Chuc vu   
        position = response.xpath('(//p[@class="fl_right"])[1]//label/text()').extract()
        if len(position) > 0:
            self.item["position"] = position[0]
            pass

        #Hinh thuc lam viec fulltime/parttime
        category = [info_other.strip() for info_other in info_others if "hình thức" in info_other.lower()] + [description.strip() for description in descriptions if "hình thức" in description.lower()]
        if len(category) > 0:
            self.item['category'] = category[0].split(':')[-1].strip()
        else:
            self.item['category'] = 'Không yêu cầu'
        #Thoi gian thu viec
        trial_time = [info_other.strip() for info_other in info_others if "thời gian thử việc" in info_other.lower()]
        if len(trial_time) > 0:
            self.item["trial_time"] = trial_time
        else:
            self.item['trial_time'] = 'Không yêu cầu'
        #Yeu cau gioi tinh
        sex_male = "Nam" if len([info_other for info_other in info_others if "nam" in info_other.lower()]) > 0 else ""
        sex_female = "Nữ" if len([info_other for info_other in info_others if "nữ" in info_other.lower()]) > 0 else ""
        sex = sex_male + ("", "/")[sex_male != "" and sex_female != ""] + sex_female
        if sex == "":
            self.item['sex'] = "Không yêu cầu"
        else:
            self.item['sex'] = sex.strip()
        ages = [other.strip() for other in info_others if "tuổi" in other] + [description.strip() for description in
                                                                         descriptions if "tuổi" in description]
        if len(ages) > 0:
            self.item["age"] = ages[0].split(":")[-1].strip()
        else:
            self.item["age"] = 'Không yêu cầu'

        #Mo ta
        
        #Quyen loi duoc huong
        benefits = response.xpath("//div[@class='MarBot20 benefits-template']/h4[contains(text(),'Phúc lợi')]/following-sibling::ul/li/text()").extract()
        self.item["benefits"] = ', '.join(benefits).strip()
        #Yeu cau khac
        require_skills = response.xpath("//div[@class='MarBot20']/h4[contains(text(),'Yêu Cầu Công Việc')]/following-sibling::div[@class='content_fck']//text()").extract()
        self.item["require_skill"] = " ".join([skill.replace('-', '').strip() for skill in require_skills])

        #Thong tin lien he
        per_contact = response.xpath('(//p[@class="TitleDetailNew"]//label)[3]//strong/text()').extract()
        add_contact = response.xpath('(//p[@class="TitleDetailNew"]//label)[2]/text()').extract()
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
        contact = u"Người liên hệ: " + pers_contact.strip() + u" Địa chỉ liên hệ: " + addr_contact.strip()
        self.item["contact"] = contact
        
        #Han nop ho so
        expired = response.xpath('(//p[@class="fl_right"])[3]/text()').extract() #Het han
        if len(expired) > 0:
            self.item["expired"] = expired[0]
            pass
        #Ngay tao hoso
        created = response.xpath('//div[@class="datepost"]//span/text()').extract() #Ngay tao
        if len(created) > 0:
            self.item["created"] = created[0]
        if self.item["title"] != None:
            yield self.item
    
    


