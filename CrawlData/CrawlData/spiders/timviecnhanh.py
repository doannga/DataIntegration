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

class TimviecnhanhSpider(scrapy.Spider):
    name = 'timviecnhanh'
    start_urls = ['https://www.timviecnhanh.com/vieclam/timkiem?tu_khoa=&nganh_nghe%5B%5D=&tinh_thanh%5B%5D=']
    collection_name = 'News'
    client = pymongo.MongoClient(settings.MONGO_URI)
    db = client[settings.MONGO_DATABASE]
    collection = db[collection_name]
    Y_in_db = list(collection.find({}, {"title":1,"company":1, "address":1, "_id":0}))
    no_duplicate_items = 0
    def parse(self, response):

        for tn in response.xpath('//td[@class="block-item w55"]'):
            src = tn.xpath('a/@href').extract_first()
            src = response.urljoin(src)
            yield scrapy.Request(src, callback=self.parse_src)


        next_pages = response.xpath('//a[@class="next item"]/@href').extract()
        next_page = next_pages[len(next_pages) - 1]

        if next_page is not None:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(next_page, callback=self.parse)

    def parse_src(self, response):
        self.item = JobItem()
        self.item["url"] = response.request.url
        title = response.xpath('//h1[@class="title font-roboto text-primary"]//span/text()').extract()
        x_title = title[0].strip()
        if len(title) > 0:
            self.item["title"] = title[0]

        #Cong ty
        company = response.xpath('//div[@class="col-xs-6 p-r-10 offset10"]//h3//a/text()').extract()
        x_company = company[0].strip()
        if len(company) > 0:
            self.item["company"] = company[0].strip()
        else:
            self.item["company"] = "Không co"
         #Noi lam viec
        addresses = response.xpath('(//li[@class="m-b-5"])[4]//a/text()').extract()
        address = ', '.join([address.replace("Việc làm", "").replace("TP.HCM", "Hồ Chí Minh").strip() for address in addresses])
        self.item['address'] = address
        #Check duplicate
        data_need_check = DataReduction(3, [[job['title'], job['company'], job['address']] for job in self.Y_in_db])
        if data_need_check.is_match([x_title, x_company, address]):
            self.no_duplicate_items += 1
            print(self.no_duplicate_items)
            return
        #Luong
        salary = response.xpath('((//li[@class="m-b-5"])/text())[2]').extract()
        if len(salary) > 0:
            salary_ = salary[0].strip()
            salary_need_normalize = Normalize_salary()
            salary_normalized = salary_need_normalize.normalize_salary(salary_)
            self.item["salary"] = salary_normalized
        else:
            self.item["salary"] = 'Thỏa thuận'

        #Yeu cau khac
        require_skilles = response.xpath('(//article[@class="block-content"]//table//tr/td/p)[2]/text()').extract()
        if len(require_skilles) > 0:
            self.item["require_skill"] = ", ".join([ require_skill.replace("-"," ").strip() for require_skill in require_skilles])
            pass
        #Kinh nghiem    
        experience = response.xpath('((//li[@class="m-b-5"])[2]/text())[2]').extract()
        if len(experience) > 0:
            self.item["experience"] = experience[0].strip()
        else:
            self.item["salary"] = 'Không yêu cầu'
        # Bang cap
        diploma = response.xpath('((//li[@class="m-b-5"])[3]/text())[2]').extract()
        if len(diploma) > 0:
            self.item["diploma"] = diploma[0].strip()
        else:
            self.item["salary"] = 'Không yêu cầu'
         #So luong
        amount = response.xpath('((//li[@class="m-b-5"])[6]/text())[2]').extract()
        if len(amount) > 0:
            self.item["amount"] = amount[0].strip()
        else:
            self.item["salary"] = 'Không yêu cầu'
         #Nganh nghe
        career = response.xpath('(//li[@class="m-b-5"])[5]//a/text()').extract() 
        if len(career) > 0:
            career_need_nomarlize = Normalize_careers()
            career_normalized = career_need_nomarlize.normalize_careers(career)
            self.item["career"] = career_normalized
        else:
            self.item["salary"] = 'Ngành nghề khác'
        # Chuc vu   
        position = ""

        #Hinh thuc lam viec fulltime/parttime
        category = response.xpath('((//li[@class="m-b-5"])[9]/text())[2]').extract()
        if len(category) > 0:
            self.item["category"] = category[0].strip()
        #Thoi gian thu viec
        trial_time = " "
        self.item["trial_time"] = trial_time

        #Yeu cau gioi tinh
        sex_male = "Nam" if len([info_other for info_other in require_skilles if "nam" in info_other.lower()]) > 0 else ""
        sex_female = "Nữ" if len([info_other for info_other in require_skilles if "nữ" in info_other.lower()]) > 0 else ""
        sex = sex_male + ("", "/")[sex_male != "" and sex_female != ""] + sex_female
        if sex == "":
            self.item['sex'] = "Không yêu cầu"
        else:
            self.item['sex'] = sex.strip()
        #Yeu cau tuoi 
        age = [a.strip() for a in require_skilles if "tuổi" in a.lower()] 
        if len(age) > 0:
            age_number = re.sub('[^\d]',' ',age[0])
            age_number = age_number.replace(" ","").strip()
            print(age_number)
            self.item['age'] = age_number[:2] + '-' + age_number[2:4]
        else:
            self.item["age"] = 'Không yêu cầu'

        #Mo ta
        descriptions =  response.xpath('(//article[@class="block-content"]//table//tr/td/p)[1]/text()').extract()
        if len(descriptions) > 0:
            self.item["description"] = ", ".join([description.strip() for description in descriptions])
        else:
            self.item['description'] = 'Không có'
        #Quyen loi duoc huong
        benefits = response.xpath('(//article[@class="block-content"]//table//tr/td/p)[3]/text()').extract()
        if len(benefits) > 0:
            self.item["benefits"] = ", ".join([benefit.strip() for benefit in benefits])
        else:
            self.item['benefits'] = 'Không có'
        #Yeu cau khac
        
        #Thong tin lien he
        per_contact = response.xpath('(//table[@class="width-100"]//td//p)[1]/text()').extract()
        add_contact = response.xpath('(//table[@class="width-100"]//td//p)[2]/text()').extract()
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
        contact = u"Người liên hệ: " + pers_contact + u" Địa chỉ liên hệ: " + addr_contact
        self.item["contact"] = contact
        
        #Han nop ho so
        expired = response.xpath('(//b[@class="text-danger"])[1]/text()').extract() #Het han
        if len(expired) > 0:
            self.item["expired"] = expired[0][15:].strip()
            pass
        #Ngay tao hoso
        created = response.xpath('//time[@class="entry-date published"]/text()').extract() #Ngay tao
        if len(created) > 0:
            self.item["created"] = created[0].strip()
        if self.item["title"] != "":
            yield self.item

