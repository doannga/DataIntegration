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

class CareerbuilderSpider(scrapy.Spider):
    name = 'careerbuilder'
    start_urls = ['https://careerbuilder.vn/viec-lam/tat-ca-viec-lam-vi.html/']
    ur = pybloom_live.BloomFilter(capacity=2097152, error_rate=0.005)
    collection_name = 'News'
    JACCARD_MEASURE = 0.7
    PREFIX_FILTERING = 2
    SIMILARITY_THRESHOLD = 0.75
    
    def parse(self, response):
        # Time start
        print(time.strftime("%H:%M:%S", time.localtime()))
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
        # Time end
        print(time.strftime("%H:%M:%S", time.localtime()))
    def parse_src(self, response):
        self.item = JobItem()
        self.item["url"] = response.request.url
        #Tieu de
        title = response.xpath('//div[@class="top-job-info"]/h1/text()').extract()
        x_title = title[0]
        company = response.xpath('//div[@class="tit_company"]/text()').extract()
        x_company = company[0]
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
    	# Luong
        salary = response.xpath("//div[@id='showScroll']/ul/li/p/span[contains(text(), 'Lương')]/following-sibling::label/text()").extract()
        if len(salary) > 0:
            self.item["salary"] = salary[0]
        else:
            self.item["salary"] = ""
            pass
    	#Kinh nghiem    
        experience = response.xpath("//div[@id='showScroll']/ul/li/p/span[contains(text(), 'Kinh nghiệm')]/../text()").extract()
        self.item["experience"] = experience[0].strip() if experience[0] is not None else "Không yêu cầu"
        descriptions =  response.xpath("//div[@class='MarBot20']/h4[contains(text(),'Mô tả Công việc')]/following-sibling::div[@class='content_fck']//text()").extract()
        self.item["description"] = ' '.join([description.replace('-', '').strip() for description in descriptions])
        #Thong tin khac bao gom bang cap, do tuoi, va hinh thuc lam viec
        info_others = response.xpath("//div[@class='MarBot20']/h4[contains(text(),'Thông tin khác')]/following-sibling::div[@class='content_fck']/ul/li//text()").extract()
        print(info_others)
        # Bang cap
        diploma = [info_other.strip() for info_other in info_others if
                   "bằng cấp" in info_other.lower() or "tốt nghiệp" in info_other.lower()] + [
                      description.strip() for description in descriptions if "bằng cấp" in description.lower() or "tốt nghiệp" in description.lower()]
        if len(diploma) > 0:
            self.item['diploma'] = diploma[0].split(':')[-1].strip()
        else:
            self.item['diploma'] = ''
         #So 0
        amount = ""
        self.item["amount"] = amount
         #Nganh nghe
        career= response.xpath("//div[@id='showScroll']/ul/li/p/span[contains(text(), 'Ngành nghề')]/following-sibling::b/a/text()").extract() 
        self.item["career"] = ', '.join(career)
         #Noi lam viec
        address = response.xpath("//div[@id='showScroll']/ul/li/p/span[contains(text(), 'Nơi làm việc')]/following-sibling::b/a/text()").extract()
        if len(address) > 0:
            self.item["address"] = address
            pass
         # Chuc vu   
        position = response.xpath('(//p[@class="fl_right"])[1]//label/text()').extract()
        if len(position) > 0:
            self.item["position"] = position[0]
            pass

        #Hinh thuc lam viec fulltime/parttime
        category = response.xpath('(//div[@class="content_fck"]//ul//li)[3]/text()').extract()
        if len(category) > 0:
            self.item["category"] = category[0][14:]
            pass
        #Thoi gian thu viec
        trial_time = ""
        self.item["trial_time"] = trial_time
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
    # def jaccard(self, str1, str2):
    #     str1 = set(str1.split())
    #     str2 = set(str2.split())
    #     return float(len(str1 & str2)) / len(str1 | str2)
    def tach_tu(self, text):
        text_tokenize = ViTokenizer.tokenize(text)
        return text_tokenize
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

    


