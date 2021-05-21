import requests
import ssl
import urllib3
import csv
import json
import os
import time
import re
import MyThread
import shutil

ssl._create_default_https_context = ssl._create_unverified_context

Token = "token"
Token_head = "head"
# Cookie = "JSESSIONID_MDAS=3A49C332BB0FFE07835037335AB28575"
Frame = "c0 01 81 00 03 00 00 60 06 04 ff 02 00"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
    "Host": "smartmeter.nea.org.np",
    "Authorization": Token_head + Token,
    # "Cookie" : Cookie,
    # "Referer": "https://smartmeter.nea.org.np/hes/",
    # "Origin": "https://smartmeter.nea.org.np",

    # "Sec-Fetch-Site":"same-origin",
    # "Sec-Fetch-Mode":"cors",
    # "Sec-Fetch-Dest":"empty",
}


def load_token(path):
    global Token,Token_head,headers
    try:
        with open(path,'r',encoding='utf8') as fp:
            file_list = fp.readlines()
            if len(file_list) > 1:
                Token_head = file_list[0].split('\n')[0]
                Token = file_list[1].split('\n')[0]
                headers['Authorization'] = Token_head + Token

    except:
        exit_with_error("加载Auth文件出错，按任意键退出")



def load_meter_list(path):
    meter_list = []
    try:
        with open(path, 'r', encoding='utf8') as fp:
            reader = csv.reader(fp)
            for row in reader:
                meter_list.append(row[0])
        meter_list.pop(0)
    except Exception as e:
        print(e)
        return []
    return meter_list


def load_meter_database(meter_list, path):
    non_index_list = []
    name_list = []
    db_list = []
    try:
        with open(path, 'r', encoding='utf8') as fp:
            content = fp.read()
            db_list = json.loads(content)
            for db_data in db_list:  # 索引所有的已检索的表号
                t_no = db_data['mpedName'].split('-')[-1]
                name_list.append(t_no)
            for t_meter in meter_list:
                if t_meter not in name_list:
                    non_index_list.append(t_meter)
            return non_index_list,db_list
    except Exception as e:
        print(e)
        return meter_list,db_list


def search_meter_msg_db(meter_no,meter_db):
    for t_db in meter_db:
        if meter_no == t_db['mpedName'].split('-')[-1]:
            return t_db
    return None


def exit_with_error(msg):
    print(msg)
    input()
    exit(0)


def save_json_data(data,path):
    with open(path,'w',encoding='utf8') as fp:
        fp.write(json.dumps(data))


def create_meter_db(meter_list, meter_db, path):
    for t_meter in meter_list:
        for retry in range(3):
            try:
                t_json = query_meter_msg(t_meter)

                if t_json is not None:
                    meter_db.append(t_json)
                    print("获取表号：" + t_meter + "数据成功")
                    break
                else:
                    print("网络异常，重试...")
                    continue
            except:
                print("网络异常，重试...")
                continue
        else:
            exit_with_error("网络异常，重试次数过多，按任意键退出...")
    save_json_data(meter_db,path)
    return meter_db


def query_meter_msg(meter_no):
    query_url = "https://smartmeter.nea.org.np/hes/performanceManagement/remoteManagement/meterConfigGate/queryPara"
    query_str = "pageNo=1&pageSize=50&protocolCode=39&protocolType=98&queryType=02&mpedName=" + meter_no + "&paraType=0"
    header_bkp = headers.copy()
    header_bkp['Content-Type'] = "application/x-www-form-urlencoded;charset=UTF-8"
    urllib3.disable_warnings()
    res = requests.post(url=query_url, headers=header_bkp, data=query_str, verify=False)
    print(len(res.text))
    res = res.json()
    print(res)
    if res['code'] != "200":
        return None
    else:
        return res['data']['dataList'][0]


def get_post_data(db_data):
    termID = db_data['termId']
    post_data = {}
    post_data['clientId'] = Token + str(db_data['mpedId']) + str(db_data.get('meterIndex',0))
    post_data['termId'] = termID
    post_data['name'] = db_data['mpedName']
    post_data['fn'] = "METER"
    post_data['dataList'] = []
    temp_dic = {}
    temp_dic['cpName'] = post_data['name'] + "(Gate Parameter)"
    temp_dic['gatePara'] = Frame
    temp_dic['id'] = db_data['id']
    temp_dic['mpedId'] = db_data['mpedId']
    temp_dic['mpedName'] = post_data['name']
    temp_dic['paraType'] = db_data['paraType']
    temp_dic['protocolCode'] = db_data['protocolCode']
    temp_dic['protocolType'] = db_data['protocolType']
    temp_dic['termId'] = termID
    post_data['dataList'].append(temp_dic)
    return post_data


def get_pull_data():
    pull_data = {}
    pull_data['topic'] = "GatherManualTaskInfo"
    return pull_data


def send_request(db_data):
    post_url = "https://smartmeter.nea.org.np/hes/performanceManagement/remoteManagement/meterConfigGate/sendPara"
    post_data = get_post_data(db_data)
    urllib3.disable_warnings()
    res = requests.post(url=post_url, headers=headers, json=post_data, verify=False).json()
    temp_str = res.get('code',"")
    #print(res)
    if temp_str == "200":
        return True
    else:
        return False


def convert_value(raw_frame):
    #Rx: execute success;Result Content: [7] C4 01 81 00 12 0E 6E
    pattern = r"Rx:execute success;Result Content:\[\d{1,3}\](.*)"
    matches = re.findall(pattern,raw_frame)
    res = matches[0]
    value_list = res.split(' ')
    t_str = value_list[-3]+value_list[-2]
    a_bytes = bytes.fromhex(t_str)
    value = int.from_bytes(a_bytes,byteorder='big',signed=False)
    return str(value)


def extract_valid_response(response_data):
    req_list = []
    res_list = []
    for t_data in response_data:
        try:
            t_body = t_data.get('body',"")
            if t_body == "":
                continue
            t_mpedName = t_body.get('mpedName',"")
            if t_mpedName != "":
                task_id = t_body.get('taskId',"")
                t_dic = {}
                t_dic['MeterNo'] = t_mpedName.split('-')[-1][:-1]
                t_dic['TaskID'] = task_id.split(':')[0]
                req_list.append(t_dic)
                continue
            else:
                task_id = t_body.get('taskId', "")
                ret_str = t_body.get('text',"")
                t_dic = {}
                t_dic['RawFrame'] = ret_str
                t_dic['TaskID'] = task_id.split(':')[0]
                t_dic['Value'] = convert_value(ret_str)
                res_list.append(t_dic)
                continue
        except Exception as e:
            print(e)
            continue
    return req_list,res_list


def send_pull_message(report_list):
    pull_url = "https://smartmeter.nea.org.np/hes/pullMessage/pullMessage"
    pull_data = get_pull_data()
    urllib3.disable_warnings()
    res = requests.post(url=pull_url, headers=headers, json=pull_data, verify=False).json()


    try:
        if res['code'] == "200":
            t_data = res['data']
            if len(t_data) > 0:
                #print(res)
                print("接收到有效数据，处理中...")
                req_list,res_list = extract_valid_response(t_data)
                fresh_request_report(report_list,req_list)
                fresh_response_report(report_list,res_list)
                temp_str = get_report_process(report_list)
                print(temp_str)
                return True
            else:
                print("Pull 数据为空",end=" ")
                return False
        else:
            return False
    except Exception as e:
        print(e)
        return False


def init_report(report_list=[],meter_list=[]):
    for t_meter in meter_list:
        t_dic = {}
        t_dic['MeterNo'] = t_meter
        t_dic['RequestStatus'] = ""
        t_dic['ResponseStatus'] = ""
        t_dic['RawFrame'] = ""
        t_dic['Value'] = ""
        t_dic['TaskID'] = ""
        report_list.append(t_dic)


def fresh_response_report(report_list,response_list):
    for t_res in response_list:
        for t_list in report_list:
            if t_res['TaskID'] == t_list['TaskID']:
                t_list['ResponseStatus'] = "OK"
                t_list['RawFrame'] = t_res['RawFrame']
                t_list['Value'] = t_res['Value']


def fresh_request_report(report_list,request_list):
    for t_req in request_list:
        for t_list in report_list:
            if t_req['MeterNo'] == t_list['MeterNo']:
                t_list['RequestStatus'] = "OK"
                t_list['TaskID'] = t_req['TaskID']


def save_report_to_csv(report_list):
    dir_path = 'Output'
    file_name = 'report.csv'
    file_time = time.strftime("%Y-%m-%d %H_%M_%S")
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    if os.path.exists(dir_path+os.sep+file_name):
        src_file = dir_path+os.sep+file_name
        dst_file = dir_path+os.sep+ file_time + file_name
        shutil.move(src_file, dst_file)
    with open('Output'+os.sep+'report.csv','w+') as fp:
        writer = csv.writer(fp)
        writer.writerow(['MeterNo','Value','RawFrame','TaskID'])
        for t_dic in report_list:
            writer.writerow([t_dic['MeterNo'],t_dic['Value'],t_dic['RawFrame'],t_dic['TaskID']])



def get_report_process(report_list):
    total = len(report_list)
    req_no = 0
    respon_no = 0
    for t_list in report_list:
        if t_list['RequestStatus'] != "":
            req_no = req_no + 1
        if t_list['ResponseStatus'] != "":
            respon_no = respon_no + 1
    temp_str = "统计信息  发送请求{req}/{total},请求应答{res}/{total}".format(req=req_no,res=respon_no,total=total)

    return temp_str


def pull_meter_data(report_list):
    count = 0
    while True and count < 15:
        res = send_pull_message(report_list)
        if res:
            count = 0
        else:
            count = count + 1
            temp_str = "Pull数据为空 {count}/{total}".format(count=count,total=15)
            print(temp_str)
        time.sleep(3)
    save_report_to_csv(report_list)
    print("Pull线程退出")


def read_meter_data(meter_list,meter_db):
    for t_meter in meter_list:
        db_msg = search_meter_msg_db(t_meter,meter_db)
        if db_msg is not None:
            for i in range(3):
                try:
                    res = send_request(db_msg)
                    if res == True:
                        print(t_meter + "请求发送成功")
                        time.sleep(4)
                        break
                    else:
                        continue
                except Exception as e:
                    print(e)
                    continue
            else:
                print(t_meter+"请求发送失败")


def detect_resume(path):
    res_list = []
    try:
        with open(path,'r',encoding='utf8') as fp:
            reader = csv.reader(fp)
            for row in reader:
                if len(row) == 0:
                    continue
                if row[0] != "" and row[1] == "":
                    res_list.append(row[0])
        return res_list
    except:
        return []


def main():
    print("\n************************ 尼泊尔 HES 数据自动采集脚本 release 2021-05-20**************************\n")
    load_token('Auth.txt')

    resume_list = detect_resume('Output' + os.sep + 'report.csv')
    if len(resume_list) > 0:
        print("\n检测到上次生成的报告中，存在未抄读的数据项\n")
        print("表号是：")
        print(resume_list)
        cmd = input("是否继续抄读？0 否 1 是")
        if cmd == "1":
            meter_list = resume_list
        else:
            print("加载电表表号列表......")
            meter_list = load_meter_list('MeterList.csv')
    else:
        print("加载电表表号列表......")
        meter_list = load_meter_list('MeterList.csv')



    if len(meter_list) > 0:
        print("加载表号成功")
    else:
        print("加载表号失败，按任意键程序退出...")
        input()
        exit(0)

    report_list = []
    init_report(meter_list=meter_list,report_list=report_list)

    non_index_list,meter_db = load_meter_database(meter_list, 'MeterDB' + os.sep + 'DataBase.json')
    if len(non_index_list) > 0:
        print("存在未索引的表号，共" + str(len(non_index_list)) + "个")
        print(non_index_list)
        print("对以上表号重新访问HES，建立索引")
        create_meter_db(non_index_list,meter_db,'MeterDB' + os.sep + 'DataBase.json')
    else:
        print("加载表号索引成功")

    print("启动pull response线程")
    t2 = MyThread.MyThread("pull_hes", pull_meter_data, report_list)
    print("启动HES post请求线程")
    t1 = MyThread.MyThread("post_hes", read_meter_data, meter_list,meter_db)
    t2.start()
    t1.start()
    t2.join()
    t1.join()


    #query_meter_msg("10000302")
    # send_request("C001")
    # time.sleep(4)
    # send_pull_message()
    return


if __name__ == "__main__":
    main()
    print('\nfinished\n')
