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
            text_length = len(child.get_text(strip=True))  # 获取去除空白后的文本长度             
            if text_length > max_length:  
                # 检查这是否是目前找到的最长的文本                 
                max_length = text_length                 
                max_child = child      
    
    return max_child  # 返回文本最长的子节点，如果没有找到则返回 None


# 递归处理soup
def recursive_process(soup:Tag,oldsoup:Tag)->Tag:     
    # 检查是否只有一个子节点，如果是则深入     
    soup = is_single_child_data(soup)

    num = get_max_link_text_density(soup)

    if oldsoup: 
        oldnum = get_max_link_text_density(oldsoup)        
        if num < oldnum:             
            return oldsoup        
       
  
    
    # 在当前层找到最长的子节点     
    new_soup = max_child_data(soup)   
       
    return recursive_process(new_soup,soup)

# re正则处理html
def re_rules(response:requests.models.Response)->str:

    #如果含有body结构则正则获得body里的内容
    if re.search(r'<body.*</body>', response.text, re.S):
        html = re.search(r'<body.*</body>', response.text, re.S).group(0)
    else:
        html = response.text
    
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
            if len(tag.get_text().strip()) < 7 and not re.search(r'\d', tag.get_text()):
                tag.decompose()

    # 去掉空的标签
    for tag in soup.find_all():

        if tag.get_text().strip() == '':         
            tag.decompose()

    return soup


# 判断是否是所需要的item
def is_item(children:List[Tag])->bool:
    

    link_tag_density = get_max_link_text_density(children)

    if  max_link_tag_density > link_tag_density :
        return True
    else:
        max_link_tag_density = link_tag_density


def get_max_link_text_density(children:List[Tag]):
    # 初始化集合，用于存储不同的标签类型
    tag_set = set()
    # 初始化链接标签数量
    link_tag_count = 0
    num_unique_tags = 0
    # 遍历子节点，记录不同的标签类型
    for child in children:
        # 获取child的直接子节点
        if child.name == 'a':
            link_tag_count += 1
        sub_children = child.find_all()
        # 记录子节点中的标签类型
        for sub_child in sub_children:
            tag_set.add(sub_child.name)
            if sub_child.name == 'a':  # 如果是链接标签
                link_tag_count += 1
            # 输出不同的标签类型数量
    
    num_unique_tags = len(tag_set)
    #链接标签密度
    if num_unique_tags == 0:
        num_unique_tags = 1            


    link_tag_density = link_tag_count / num_unique_tags
    return link_tag_density


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

        sql = "select root_url from url_params_more where id < 15000"  
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
    cleaned_link = re.sub(r"\s+", "", url)       
    title = a_tag.get_text(strip=True)      
    cleaned_title = re.sub(r"\s+", "", title)  # 获得文本最长的title       
    return cleaned_link, cleaned_title


#  解析日期，title
def parse_date(tag:Tag)->datetime:

    # 获得tag里每一个标签内的文本，放入list中 
    text = list(set(i.get_text().strip() for i in tag.find_all() if i.get_text() != ''))


    title = None
    extracted_date = None
 
    for key in text:
        
        # 日期提取
        try:
            extracted_date = parser.parse(key, fuzzy=False).date()         
            break
        except:
            pass
  
    # title是list中最长的文本
    title = max(text, key = lambda x: len(x)) 

    return extracted_date

# 获得url的base_url
def get_base_url(url:str)->str:
    #有http https

    pattern = re.compile(r'(http://|https://)(.*?)/')

    base_url = pattern.search(url).group(1) + pattern.search(url).group(2)+"/"

    return base_url



#  时间范围,是否在时间范围内,在则返回True,不在则返回False
def time_range(date):     
    date_interval = 10     
    current_date = datetime.now().date()     
    target_date = date     
    if target_date < current_date - timedelta(days=date_interval):         
        return False    
    return True 

# # 匹配关键字，有效信息返回True
# def judge_content(text:str)->bool:
#
#     # 是否包含关键词，不包含filter_keyword
#     if keyword(text):
#         if not filter_keyword(text):
#             return True
#
#     return False


def run(url:str):

        base_url = get_base_url(url)  # 获得url的base_url

        response = get_response(url)  # 获取网页源码  
        html = re_rules(response)  # 正则处理html  
        soup = BeautifulSoup(html, 'html.parser')  # 创建soup对象  
        soup = process_soup(soup) # 处理soup,空白字符，空标签等  
        soup = recursive_process(soup,None) # 递归处理soup，找到最长的子节点，在递归处理，直到找到我们想要的item  
        
        for tag in soup:  

            # print(tag.prettify())

            # 首先解析url，通过url访问内容，获得标题，时间，正文等信息，对于正文需要进行内容过滤，去掉广告，导航，分页等干扰信息，对于标题，时间等信息，需要进行提取，对于时间，需要进行格式化，对于标题，需要进行去除空白字符等处理，

            url,title= parse_url(tag)  # 解析url   
            if not url:
                continue
            
            url = start_http(base_url,url)  # 添加http

            date = parse_date(tag)  # 获得日期,和标题   

            # # 如果日期为空，爬取详情页抓取时间
            # if not date:
            #         # 解析下一页
            #         pass
            #
            # # 如果爬取的日期太久，直接结束
            # if not time_range(date):
            #
            #     break
            #
            # if not judge_content(title):
            #     continue


            print(url,title,date)


        # clear_output()  # 清空输出



# urls = urls_from_mysql()     
# for url in urls:         
#     url = url[0]    

url = "http://www.sczazb.com/index.php/tender/bid"
url = "http://www.ewindpower.cn/news/list-htm-catid-19.html"
# url = "https://www.gc-zb.com/search/index.html?keyword=%E5%8F%98%E5%8E%8B%E5%99%A8&h_lx=9&date=90&search_field=0&vague=0&h_province=0&submit=+"
# url = 'http://www.sxylcz.cn/list.php?cla=2&hy=0&addr=0'
# url = "https://www.bidnews.cn/search/"
# url = "http://www.gdtzb.com/zb/search.php?kw=%E5%8F%98%E5%8E%8B%E5%99%A8&areaid=0&type=0"
# url = "https://guangfu.bjx.com.cn/zb/"
# url = "https://www.bidding-crmsc.com.cn/bid"
# url = "https://www.tssgroup.com.cn/ts-news/news1/"
run(url)