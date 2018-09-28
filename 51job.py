import os
from bs4 import BeautifulSoup
import  requests
import time
import logging
from  gevent import monkey;monkey.patch_all()
# from gevent.pool import Pool
import gevent

import csv
import matplotlib.pyplot as plt
from wordcloud import WordCloud
import jieba
from queue import  Queue
from pprint import pprint

monkey.patch_all()


HEADERS={
    "X-Requested-With": "XMLHttpRequest",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36"
                  "(KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36",
}

START_URL=(
        "https://search.51job.com/list/080400,000000,0000,00,9,99,%25E4%25BC%259A%25E8%25AE%25A1,2,{}.html?lang=c&stype=1&postchannel=0000&workyear=99&cotype=99&degreefrom=99&jobterm=99&companysize=99&lonlat=0%2C0&radius=-1&ord_field=0&confirmdate=9&fromType=&dibiaoid=0&address=&line=&specialarea=00&from=&welfare="
)

# POOL_MAXSIZE=8


def log():
    logger=logging.getLogger("mylogger")
    logger.setLevel(logging.INFO)
    formatter=logging.Formatter("%(asctime)s - %(message)s")



    handler=logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger

write_log=log()


class get_data():

    def __init__(self):
        self.count=0
        self.queue_list=Queue()              # 线程池队列
        self.company_info=[]
        # self.pool=Pool(POOL_MAXSIZE)         #线程池管理线程,  最大协程数

    def scrapy_data(self):
        urls = [START_URL.format(p) for p in range(1,10)]

        for url in urls:
            # url=START_URL.format(page=i)

            write_log.info("开始爬取第 {} 个页面" .format(urls.index(url)+1))
            html=requests.get(url,headers=HEADERS).content.decode('gbk')
            bsobj=BeautifulSoup(html,'lxml').find("div",{"class":"dw_table"}).find_all("div",{"class":"el"})

            for b in bsobj:

                try:
                    href, work=b.find("a")["href"], b.find("a")["title"]
                    place=b.find("span",{"class":"t3"})
                    salary=b.find("span",{"class":"t4"})
                    # print(href,work)
                    company_info={
                        "href":href,"work":work,"place":place,"salary":salary
                    }
                    self.queue_list.put(href)
                    self.company_info.append(company_info)

                except:
                    pass

        write_log.info("队列长度为 {} ".format(self.queue_list.qsize()))

    def work_info_detail(self):
        '''打开href 查看岗位需求的条件'''

        while not self.queue_list.empty():
            # 从队列中取 url
            try:
                url=self.queue_list.get()
                response_status=requests.get(url,headers=HEADERS)
                # print(response_status)
                '''status_code   取出code的值'''
                if response_status.status_code == 200:
                    self.count += 1
                    write_log.info("开始爬取第 {} 条岗位信息".format(self.count))
                    html=requests.get(url,headers=HEADERS).content.decode('gbk')


                    write_log.info("队列长度为 {} ".format(self.queue_list.qsize()))

                else:
                    # self.queue_list.put(url)
                    continue

                try:
                    bsobj2=BeautifulSoup(html,'lxml').find("div",{"class":"tBorderTop_box"}).find_all("div",{"class":"bmsg job_msg inbox"})

                    for b2 in bsobj2:
                        # print(b2)

                        file=b2.text.replace("微信", "").replace("分享", "").replace("邮件", "").replace("职能类别", "")\
                            .replace("财务主管", "").replace("总账主管", "").replace("及", "") \
                            .replace("：", "").replace("/", "").replace("的", "").replace("关键字", "") .replace("的", "")\
                            .replace("财务专员", "").replace("财务经理", "").replace("会计专员", "").replace(" 会计主管", "") \
                            .replace("出纳专员", "").replace("经理/主管", '').replace("\t", "").replace("、", "").replace("；", "") \
                            .replace(".", "").replace("。", '').replace(" ", "").replace("部门", "").replace("\n", "") \
                            .replace("工作", "").replace('相关', '').replace("和", "").replace("等", "").replace("以上", "") \
                            .replace("具有", "").strip()

                        for ch in '!"#$%&()*+,-./:;<=>?@[\\]^_‘{|}~':
                            file = file.replace(ch, " ")  # 将文本中特殊字符替换为空格
                        for numb in range(10):
                            file = file.replace(str(numb), " ")
                        # print(file)
                        # exit()
                        with open(os.path.join("data","work_detail.txt"),'a',encoding='utf-8') as f:
                            f.write(file)
                    # self.queue_list.task_done()
                except Exception as e:
                    write_log.error(e)
                    write_log.warning(url)

            except:
                print("队列为空")
                break
    @staticmethod
    def word_cloud():
        '''启动词云开始画图'''
        counter = dict()
        with open(
                os.path.join("data", "work_detail_counter.csv"), "r", encoding="utf-8"
        ) as f:
            f_csv = csv.reader(f)
            # print(f_csv)
            # exit()
            for row in f_csv:
                if row == []:
                    pass
                else:
                    counter[row[0]] = counter.get(row[0], int(row[1]))
            pprint(counter)
        file_path = os.path.join("font", "msyh.ttf")

        wc = WordCloud(
            font_path=file_path, max_words=100, height=600, width=1200
        ).generate_from_frequencies(
            counter
        )
        plt.imshow(wc)
        plt.axis("off")
        plt.show()
        wc.to_file(os.path.join("images", "wc.jpg"))

    @staticmethod
    def work_detail_counter():
        '''岗位需求统计'''
        with open(os.path.join("data","work_detail.txt"),'r',encoding='utf-8') as f:
            work=f.read()
        # print(work)
        # exit()

        jieba.load_userdict(os.path.join("data","customize_dic.txt"))
        # cut_all=False 精确模式
        seg_list=jieba.cut(work,cut_all=False)
        '''seg是一个个分好的词'''
        counter=dict()
        for seg in seg_list:

            counter[seg]=counter.get(seg,1)+1

        counter_sort = sorted(counter.items(), key=lambda value: value[1], reverse=True)

        pprint(counter_sort)
        with  open(os.path.join("data","work_detail_counter.csv"),'w+',encoding='utf-8') as f:
            f_csv=csv.writer(f)
            f_csv.writerows(counter_sort)

            # print(counter_sort)
            # print(counter[seg])

    def run_multiple_task(self,target):

        gevent.joinall([
            gevent.spawn(target),
            gevent.spawn(target),
            gevent.spawn(target),
            gevent.spawn(target),
            gevent.spawn(target),
            gevent.spawn(target),
            gevent.spawn(target),
            gevent.spawn(target),
            ]
        )



    def run(self):
        self.scrapy_data()
        self.run_multiple_task(self.work_info_detail)




if __name__ == '__main__':
    run_get=get_data()
    start_time=time.time()
    run_get.run()
    run_get.work_detail_counter()
    run_get.word_cloud()
    write_log.info("总耗时 {} 秒".format(time.time() - start_time))