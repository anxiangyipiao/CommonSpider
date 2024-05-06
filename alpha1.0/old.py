
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
def recursive_process(soup:Tag)->Tag:     
    # 检查是否只有一个子节点，如果是则深入     
    soup = is_single_child_data(soup)  

    if is_item(soup):         
        # 如果是我们想要的item，则返回soup         
        return soup
  
    
    # 在当前层找到最长的子节点     
    new_soup = max_child_data(soup)   
       
    return recursive_process(new_soup)

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
   
    # 检查文本长度相似性     
    text_lengths = [len(child.get_text(strip=True)) for child in children]

    # 如果长度text_lengths大于5，省去最小的一个值,这个值可能是标题，分页等，干扰我们的判断
    if len(text_lengths) > 5:
        text_lengths.remove(min(text_lengths))

    num = np.std(text_lengths) 
    
    # 15 待定
    if np.std(text_lengths) >  10:
        return False 
    else:
        return True


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

    # 获得tag里的所有a标签             
    a_tag = tag.find_all('a') if tag.find_all('a') else None             

    # 如果没有a标签，返回None 过滤标题等
    if not a_tag:
        return None
                
    # 获得a标签中文本最长的一个a的href             
    a_tag = max(a_tag, key = lambda x: len(x.get_text(strip=True)))
    
    url = a_tag.get('href') 
    cleaned_link = re.sub(r"\s+", "", url)

    return cleaned_link

#  解析日期，title
def parse_date(tag:Tag)->(str,datetime):

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

    return title,extracted_date

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
    list =["中标","成交","候选人","结果","合同","南网","南方电网","国网","国家电网","招标失败","采购失败","环境影响","环评","废标","流标","评标","开标","入围","监理","水土保持","竣工","单一来源","单源直接采购","勘察设计","工程勘察","验收","备案","审计","审批","评审","核准","受理","批复","政府采购意向","地质灾害","灾害危险性评估","可研性","可研初设","可研设计","可行性研究","可行性报告","预防性试验","预防性检测","预防性测试","完成公示","中止公告","终止公告","终止招标","中选公告","失败公告","终止","编制","开标记录","设计服务","技术服务","安全评估","安全评价","安全防护","影响评价","消防","可研报告","维护检修","外委维护","检修维护","运行维护","运维","维保","维修","代维","干式变压器","干式配电变压器","干式电力变压器","控制变压器","变压器处置","挂牌","转让","拍卖","变卖","竞拍","出让","招租","租赁","医疗设备","出租","零星物资","结算","保险","保洁","劳保","劳务分包","劳务外包","劳务招标","外包项目","服务外包","施工专业承包","消缺工","程工程分包","工程专业分包","工程设计","工程咨询","工程造价咨询","工程招标代理","工程采购公示","改造设计","改造施工","施工材料","标识","批前公示","零星材料","评审公示","评价报告","社会稳定风险评估","水土保持报告","影响评估","泰开","成套公司","送变电公司","电力电子公司","物业管理","空调","监控","电梯","路灯专用","灯具","刀具","夹具","金具","工具","锁具","家具","五金","消防物资","配件","附件","硬件","线材","辅材","耗材","管材","资产评估","风险评估","废旧物资处置","无功补偿","补偿装置","补偿设备","预算审核","UPS","GIS","SVG","AIS","PHC","AVC","MPP","KKS","采购与安装","采购及安装","办公用品","异常公告","处置公告","评标报告","在线监测","在线温度监测","监测项目","监测装置","监测系统","非物资","风机塔筒","医用","体检","餐饮","射线","导线","母线","装修","工程类","服务类","迁改工程","送出工程","拆除工程","电缆工程","隔离开关","矿用","赛迪集团","开滦集团","内燃柴电机组","灭火物资","灭火系统","灭火装置","防火材料","防火物资","防火槽盒", "碎石机"]
    
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


def run(url:str):
        
        
        base_url = get_base_url(url)  # 获得url的base_url

        response = get_response(url)  # 获取网页源码  
        html = re_rules(response)  # 正则处理html  
        soup = BeautifulSoup(html, 'html.parser')  # 创建soup对象  
        soup = process_soup(soup) # 处理soup,空白字符，空标签等  
        soup = recursive_process(soup) # 递归处理soup，找到最长的子节点，在递归处理，直到找到我们想要的item  
        
        for tag in soup:  

            # print(tag.prettify())

            # 首先解析url，通过url访问内容，获得标题，时间，正文等信息，对于正文需要进行内容过滤，去掉广告，导航，分页等干扰信息，对于标题，时间等信息，需要进行提取，对于时间，需要进行格式化，对于标题，需要进行去除空白字符等处理，

            url= parse_url(tag)  # 解析url   
            if not url:
                continue
            
            url = start_http(base_url,url)  # 添加http

            title,date = parse_date(tag)  # 获得日期,和标题   

            # 如果日期为空，爬取详情页抓取时间
            if not date:
                    # 解析下一页
                    pass
               
            # 如果爬取的日期太久，直接结束
            if not time_range(date): 

                break
     
            if not judge_content(title):
                continue


            print(url,title,date)

           

# url = "https://ec.powerchina.cn/" 
# url = "https://www.gc-zb.com/search/index.html?keyword=%E5%8F%98%E5%8E%8B%E5%99%A8&h_lx=9&date=90&search_field=0&vague=0&h_province=0&submit=+" # url = 'http://www.sxylcz.cn/list.php?cla=2&hy=0&addr=0' # url = "https://www.bidnews.cn/search/" # url = "http://www.gdtzb.com/zb/search.php?kw=%E5%8F%98%E5%8E%8B%E5%99%A8&areaid=0&type=0" # url = "https://guangfu.bjx.com.cn/zb/" # url = "https://www.bidding-crmsc.com.cn/bid" # url = "https://www.tssgroup.com.cn/ts-news/news1/"

urls = urls_from_mysql()     
for url in urls:         
    url = url[0]    

    run(url)