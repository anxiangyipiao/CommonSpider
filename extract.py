from bs4 import BeautifulSoup,Tag
import numpy as np
import requests 
import re  
from typing import List
import pymysql
from IPython.display import clear_output
from dateutil import parser
from datetime import datetime,timedelta


header = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36 Edge/16.16299'
}

#  替换字符串
def replace_str(source_str:str, regex:str, replace_str = '')->str:         
     
    str_info = re.compile(regex)         
    return str_info.sub(replace_str, source_str)

# 判断soup的子节点长度是否为1，是则递归调用，直到子节点长度不为1，返回soup
def is_single_child_data(soup:Tag)->List[Tag]:
    
    children = list(soup.children)
    num_children = [child for child in children if child.name is not None] 
         
    if len(num_children) == 1:                  
        return is_single_child_data(num_children[0])          
    else:          
        return num_children  

# 获得soup最长的孩子节点
def max_child_data(children:List[Tag])->Tag:
        
    max_child = None 
    max_length = 0 

    # 遍历soup的所有直接子节点     
    for child in children:         
        if isinstance(child, Tag):  
            # 确保它是一个标签     
            cleaned_text = re.sub(r"\s+", "", child.get_text(strip=True))        
            text_length = len(cleaned_text)  # 获取去除空白后的文本长度   
                 
            if text_length > max_length:  
                # 检查这是否是目前找到的最长的文本                 
                max_length = text_length                 
                max_child = child      
    
    return max_child  # 返回文本最长的子节点，如果没有找到则返回 None


# 递归处理soup
def recursive_process(soup:Tag,oldsoup:Tag)->Tag:     
    # 检查是否只有一个子节点，如果是则深入     
    soup = is_single_child_data(soup)  
    
    num = cal_item_std(soup)    # 计算newsoup标准差
    if oldsoup:
        oldnum = cal_item_std(oldsoup)  # 计算oldsoup标准差
        # 最后一层，返回
        if num == None:
            return oldsoup
        # num 和 oldnum 绝对值差小于1，返回
        if num > oldnum - 3 and oldnum < 50 :    # 如果soup标准差大于oldsoup标准差，返回
            return oldsoup
  

    # 在当前层找到最长的子节点     
    new_soup = max_child_data(soup)   
       
    return recursive_process(new_soup,soup)

# re正则处理html
def re_rules(response)->str:

    #如果含有body结构则正则获得body里的内容
    if re.search(r'<body.*</body>', response, re.S):
        html = re.search(r'<body.*</body>', response, re.S).group(0)
    else:
        html = response
    
    # 去掉script标签
    html = replace_str(html, '(?i)<script(.|\n)*?</script>') #(?i)忽略大小写   
    # 去掉style标签    
    html = replace_str(html, '(?i)<style(.|\n)*?</style>')
    # 去掉form标签(有些数据放到form里，坑！)
    # html = replace_str(html, '(?i)<form(.|\n)*?</form>') 
    # 去掉option标签
    html = replace_str(html, '(?i)<option(.|\n)*?</option>')
    # 去掉input标签
    html = replace_str(html, '(?i)<input(.|\n)*?>') 
    # 去掉img标签
    html = replace_str(html, '(?i)<img(.|\n)*?>')
    # 去掉注释    
    html = replace_str(html, '<!--(.|\n)*?-->')       
    # 去掉标签属性  
    html = replace_str(html, '(?!&[a-z]+=)&[a-z]+;?', ' ') 

    return html

# 处理soup
def process_soup(soup:Tag)->Tag:
    # 去掉文本内容小于7的标签 # 比如<li><a href="http://www.gdtzb.com/kefu/show/33/" id="top-1">入网指导</a></li>的的入网指导长度小于7，舍去
    for tag in soup.find_all('a'):
        
            # 但是文本里不包括数字
            if len(tag.get_text().strip()) < 6 and not re.search(r'\d', tag.get_text()):
                tag.decompose()

    # 去掉空的标签
    for tag in soup.find_all():

        if tag.get_text().strip() == '':         
            tag.decompose()



    return soup


# 判断是否是所需要的item
def cal_item_std(children:List[Tag])->float:
    
    if not children:
        return None

    # 检查文本长度相似性     
    text_lengths = [len(re.sub(r"\s+", "", child.get_text(strip=True))) for child in children]

    # 如果长度text_lengths大于5，省去最小的一个值,这个值可能是标题，分页等，干扰我们的判断
    if len(text_lengths) > 5:
        text_lengths.remove(min(text_lengths))

    num = np.std(text_lengths) 
    
    return num



#  获取网页源码 get请求
def get_response(url:str)->requests.models.Response:
    response = requests.get(url, headers=header)   
    response.encoding = 'utf-8'  # 设置编码格式为utf-8
    return response


# 从mysql数据库获取url
def urls_from_mysql()->List[str]:

        host = '127.0.0.1'      
        user = 'root'      
        password = '123456'      
        database = 'ceeg'     
   
        connection = pymysql.connect(         host=host,         user=user,         password=password,         database=database     )     
        
        cursor = connection.cursor()

        sql = "select root_url from url_params_test_more where id < 15000"  
        cursor.execute(sql)    
        urls = cursor.fetchall()

        cursor.close()
        connection.close()

        return urls


#  从文本中提取标题
def find_title(text):          
    title=re.findall('<title>(.+)</title>',text)          
    return title

# 添加http
def start_http(baseurl,url):         
    if url.startswith("http"):             
        return url        
    else:   
        if url.startswith("/"):  
            return baseurl + url[1:]   
        else:
            return baseurl + url


#  解析url
def parse_url(tag:Tag)->str:

    # 如果tag是a标签，则直接处理该a标签     
    if tag.name == 'a':         
            a_tag = tag     
    else:  
            # 否则查找tag内部的所有a标签         
            a_tag = tag.find_all('a') if tag.find_all('a') else None 

            if not a_tag:         
                return None,None

            # 获得a标签中文本最长的一个a的href                  
            a_tag = max(a_tag, key = lambda x: len(x.get_text(strip=True)))


    url = a_tag.get('href')
    # 不纯在href属性，返回None
    if not url:
        return None,None
     
    cleaned_link = re.sub(r"\s+", "", url)


    title = a_tag.get_text(strip=True)

    cleaned_title = re.sub(r"\s+", "", title)  # 获得文本最长的title


    return cleaned_link, cleaned_title

# parser.parse
def only_one_date(text:List)->datetime:

    extracted_date = None
    for key in text:         
            try:
                extracted_date = parser.parse(key, fuzzy=False).date()
                # if extracted_date < datetime.now().date():

                break         
            except:
                pass

    return extracted_date


# parser.parse
def many_date(text)->datetime:      
    
    extracted_date = []    
    for key in text:                  
        if(len(key) > 2):             
            try:                 
                extracted_date.append(parser.parse(key, fuzzy=False).date())    

            except:                 
                pass      
       
     # 返回最靠近目前时间的日期         
    now = datetime.now()        
    return min(extracted_date, key=lambda x: abs(x - now))



def remove_chinese_characters(text:List[str])->List[str]:     
    chinese_punctuation_pattern = r'[\u3000-\u303F\uFF01-\uFF0F\uFF1A-\uFF20\uFF3B-\uFF40\uFF5B-\uFF65]'  
   

    new_text = []
    for item in text:         
        # 正则表达式匹配汉字范围[\u4e00-\u9fa5]，使用re.sub函数替换匹配到的汉字为空字符串     
        item = re.sub(r'[\u4e00-\u9fa5]', '', item) 
        item = re.sub(chinese_punctuation_pattern, '', item) 
        item = item.replace('[', '').replace(']', '').replace('...', '')
        # 去掉空
        item = re.sub(r'\s+', '', item)
        # 去掉年份
        if len(item) == 4:
            item = re.sub(r'\d{4}', '', item)
        if len(item) == 3:             
            item = re.sub(r'\d{3}', '', item)
        if len(item) > 2 and item != '':             
            new_text.append(item)
    
    return new_text



# 正则匹配日期   年月日
def re_date(text:str)->datetime:
    
    pattern = r"""((?P<year>\d{2,4})[-/.])((?P<month>\d{1,2})[-/.])(?P<day>\d{1,2})"""
    regex = re.compile(pattern, re.VERBOSE)          
    
    # 匹配多个数据的话，返回最早的数据         
    data_list = []

    for match in regex.finditer(text):                    
        year = match.group('year')             
        month = match.group('month')             
        day = match.group('day')

        if year and month and day:
            data_list.append(datetime.strptime(f"{year}-{month}-{day}", "%Y-%m-%d"))

     # 返回最靠近目前时间的日期         
    now = datetime.now()         
    if data_list:              
        return min(data_list, key=lambda x: abs(x - now)).date()       
    else:             
        return None

# 正则匹配日期   月日,没有年份
def re_dates(text:str)->datetime: 

    pattern = r"""((?P<month>\d{1,2})[-/.])(?P<day>\d{1,2})"""     
    regex = re.compile(pattern, re.VERBOSE)                  
      # 匹配多个数据的话，返回最早的数据              
          
    for match in regex.finditer(text):

        year = match.group('year')                      
        month = match.group('month')                      
        day = match.group('day')          
        if year and month and day:             
            return datetime.strptime(f"{year}-{month}-{day}", "%Y-%m-%d").date()           
        


#  解析日期，title
def parse_date(tag:Tag)->datetime:


    text = tag.get_text()

#    年月日齐全
    date = re_date(text)
#   年月日不齐全，只有月日，没有年份
    if not date:
        text = tag.get_text().split()
        date = only_one_date(text)    


    # # 每个子标签的文本内容提取出来
    # text = [tag.get_text(strip=True) for tag in tag.find_all()] 
    # text = remove_chinese_characters(text)

    # extracted_date = only_one_date(text) 


    # if not extracted_date:
    #     # tag中有多个时间
    #     extracted_date = many_date(text)
  

    return date



# 获得url的base_url
def get_base_url(url:str)->str:
    #有http https

    pattern = re.compile(r'(http://|https://)(.*?)/')

    base_url = pattern.search(url).group(1) + pattern.search(url).group(2)+"/"

    return base_url
    

# search关键词,是否包含关键词，包含则返回True
def keyword(text:str)->bool:
    
    # 关键词列表
    list = ["油变","油浸","10KV","35KV","220KV","落后产能淘汰","主变","中性点","整流变","配电变压器","低压变压器","高压变压器","配变","电力变压器","变压器","厂用变","66KV","开关柜","环网柜","中置柜","高压柜","低压柜","变电站","配电柜","调压变压器","节能变压器","箱变","电气柜数据"]

    # 如果list里的关键词在text里，则返回True
    for i in list:
        if i in text:
            return True
            
    return False
        

# 过滤关键词,是否包含关键词，包含则返回True
def filter_keyword(text:str)->bool:
    
     # 废弃关键词列表     
    list =["s"]
    
    for i in list:
        if i in text:
            return True
    return False

#  时间范围,是否在时间范围内,在则返回True,不在则返回False
def time_range(date):     
    date_interval = 10     
    current_date = datetime.now().date()     
    target_date = date     
    if target_date < current_date - timedelta(days=date_interval):         
        return False    
    return True 

# 匹配关键字，有效信息返回True
def judge_content(text:str)->bool:
    
    # 是否包含关键词，不包含filter_keyword
    if keyword(text):
        if not filter_keyword(text):
            return True

    return False

def get_date():

    from playwright.sync_api import sync_playwright  
    STEALTH_PATH = 'stealth.min.js'    
    with sync_playwright() as p:    
        browser = p.chromium.launch(                  headless=False,                  
        chromium_sandbox=False,
        ignore_default_args=["--enable-automation"],                  channel="chrome",)
        ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36'          
        content = browser.new_context(user_agent=ua)     
        content.add_init_script(path=STEALTH_PATH)       
        page = content.new_page()          
        page.goto('http://www.chinaunicombidding.cn/bidInformation')     
        page.wait_for_timeout(5000)

        return page.content()



def run():
        
        
        # base_url = get_base_url(url)  # 获得url的base_url

        # response = get_response(url)  # 获取网页源码  
        
        response = get_date()
        html = re_rules(response)  # 正则处理html  
        soup = BeautifulSoup(html, 'html.parser')  # 创建soup对象  
        soup = process_soup(soup) # 处理soup,空白字符，空标签等  
        soup = recursive_process(soup,None) # 递归处理soup，找到最长的子节点，在递归处理，直到找到我们想要的item  
        
        for tag in soup:  
            
            # print(tag.preetify())
            url,title = parse_url(tag)  # 解析url   
            if not url:
                continue
            
            # url = start_http(base_url,url)  # 添加http

            date = parse_date(tag)  # 获得日期,和标题   

            # 如果日期为空，爬取详情页抓取时间
            if not date:
                continue
                    # # 解析下一页
                    # response = get_response(url)
                    # html = re_rules(response)
                    # date = re_dates(html)
               
            # # 如果爬取的日期太久，直接结束
            # if not time_range(date): 

            #     break
     
            # if not judge_content(title):
            #     continue


            print(url,title,date)


if __name__ == '__main__':
    run()