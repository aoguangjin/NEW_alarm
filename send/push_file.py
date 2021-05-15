import time
import os
import requests
from configparser import ConfigParser

#初始化类
cp = ConfigParser()
host_path = os.path.abspath(os.path.abspath('.'))
config_file_name = '\config.cfg'
config_path = host_path + config_file_name
cp.read(config_path)
#得到所有的section，以列表的形式返回
section = cp.sections()[0]
#得到该section中的option的值，返回为string类型
ip = cp.get(section, "ip")
port = cp.get(section, "port")
ip_port = ip+':'+port
print('ipipip',ip_port)
# 请求端口发送图片
def wx_push_img(group_name, info):
    try:
        url = 'http://{}//WX_send_img'.format(ip_port)
        data = {
            'group_name': group_name,
            'push_info': info
        }
        r = requests.post(url, data=data).text
        t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print('发送后返回：%s%s'% (t,r))
    except Exception as e:
        t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print('%s群==%s消息发送失败'%(group_name,t))


# 请求端口发送文字信息
def send_file(group_name,send_info):
    try:
        url = 'http://{}//WX_send'.format(ip_port)
        garas = {
            'group_name': group_name,
            'push_info': send_info
        }
        r = requests.post(url, data=garas).text
        t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print('发送后返回：%s%s'% (t,r))
    except Exception as e:
        t = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print('%s==%s消息发送失败'%(group_name,t))



def wx_push(group_name, info):
    url = 'http://172.16.9.15:8675//WX_send'
    data = {
        'group_name': group_name,
        'push_info': info
    }
    r = requests.post(url, data=data).text
    print(r)


def SMSSend(sms_info):
    url = "http://172.16.9.15:8000/SendSMS"
    data = sms_info
    res = requests.post(url,json=data).json()
    print(res)
