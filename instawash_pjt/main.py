# -*- coding: utf-8 -*-
# 패키지 loading

import pandas as pd
import numpy as np
import datetime
import requests
import constant_v
import gurobi_models
from urllib.request import urlopen, Request
import json
import os 


# def db_connect(host, user, password, db):
#     pd.options.display.max_rows = 1000
#     pd.set_option('display.max_columns', None)

#     # SQL접속
#     conn = pymysql.connect(host=host,
#                            user=user,
#                            password=password,
#                            db=db)

#     curs = conn.cursor(pymysql.cursors.DictCursor)

#     # 쿼리
#     sql = "SELECT TB_USER_ID, TB_USER_ID_EMPLOYEE, ORDER_NO, ORDER_START_DATE, ORDER_END_DATE,ADJUST_PRICE, \
#     CAST(AES_DECRYPT (USER_LAT, 'INSTA' ) AS DECIMAL(10,6)) as USER_LAT, \
#     CAST(AES_DECRYPT (USER_LNG, 'INSTA' ) AS DECIMAL(10,6)) as USER_LNG,\
#     total_duration, price, Status FROM tb_order;"

#     # 실행
#     curs.execute(sql)
#     result = curs.fetchall()
#     result = pd.DataFrame(result)
#     return result


# def data_cleaning(result):
#     df_order = result.copy()

#     df_order = df_order[(df_order['Status'] == '03') | (df_order['Status'] == '04')]

#     # datetime타입변환
#     df_order["ORDER_START_DATE"] = pd.to_datetime(df_order["ORDER_START_DATE"])
#     df_order["ORDER_END_DATE"] = pd.to_datetime(df_order["ORDER_END_DATE"])

#     # 예약 시간범위설정
#     time_window_a = "2020-09-26 10:00:00"
#     time_window_b = "2020-09-26 18:30:00"

#     # 정시선택
#     df_order = df_order[(df_order.ORDER_START_DATE >= time_window_a)
#                         & (df_order.ORDER_START_DATE <= time_window_b)]

#     # 시간순 정렬
#     df_order = df_order.sort_values(by='ORDER_START_DATE', ascending=True)

#     # 서울 근교지역 위도경도 필터링
#     df_order = df_order.loc[(df_order["USER_LAT"] < 37.787346)
#                             & (df_order["USER_LAT"] > 37.023035) &
#                             (df_order["USER_LNG"] < 127.500868) &
#                             (df_order["USER_LNG"] > 126.309627)]

#     # 고객의 수
#     cust_number = len(df_order["ORDER_END_DATE"])
#     # 테크니션 수
#     tech_number = df_order.TB_USER_ID_EMPLOYEE.nunique()

#     return df_order, cust_number, tech_number

def data_loading(path): 
    df_order = pd.read_csv(path + "sample_data_insta_r2.csv")
    return df_order

def create_address(df_order):
    # 네이버 API로 집주소 위치 확인하기
    # 네이버 API로 예측하기
    # 네이버 API 사용
    # 위도, 경도축출

    address1 = []
    address2 = []

    # 주소변수 생성
    df_order["address"] = np.nan
    df_order["address2"] = np.nan

    # Naver api 변경필요
    client_id = constant_v.naverID
    client_pw = constant_v.naverPW
    cnt = 0

    for k, i in zip(df_order["USER_LAT"], df_order["USER_LNG"]):
        try:

            naver_url = "https://naveropenapi.apigw.ntruss.com/map-reversegeocode/v2/gc"
            gc_url = f"?request=coordsToaddr&coords={i},{k}&sourcecrs=epsg:4326&orders=admcode,legalcode,addr,roadaddr&output=json"
            sum_url = naver_url + gc_url
            naver_headers = {
                "X-NCP-APIGW-API-KEY-ID": client_id,
                "X-NCP-APIGW-API-KEY": client_pw
            }

            naver_api_test = requests.get(sum_url, headers=naver_headers)
            naver_url_text = json.loads(naver_api_test.text)

            address1.append(
                naver_url_text['results'][0]["region"]["area2"]["name"])
            address2.append(
                naver_url_text['results'][0]["region"]["area3"]["name"])

            df_order["address"][cnt] = naver_url_text['results'][0]["region"][
                "area2"]["name"]
            df_order["address2"][cnt] = naver_url_text['results'][0]["region"][
                "area3"]["name"]

            print(cnt, df_order["address"][cnt], df_order["address2"][cnt])
            cnt += 1

        except:
            continue

    # 주소삽입
    df_order["address"] = address1
    df_order["address2"] = address2

    return df_order

def divide_location(df_order, path):
    # 지역구분 dictionary 
    reg_div = pd.read_csv(path + "지역구분.csv", header= None)
    reg = {i:x for i, x in zip(reg_div[0], reg_div[1])}
    reg = pd.concat([pd.DataFrame(reg_div[0]), reg_div[1].str.split(',', expand = True)], axis = 1)
    reg.columns = ["지역", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    reg = pd.DataFrame(reg.set_index("지역").stack()).reset_index()[["지역", 0]]
    reg = {k:i for i, k in zip(reg["지역"], reg[0])}
    df_order["지역구분"] = df_order["address"].map(reg)
    print(df_order["지역구분"].value_counts())

    return reg, df_order

def create_techician(path):
    df = pd.read_csv(path + "테크니션_revise1.csv", header=None)
    df.columns = ["name", "location"]
    df["location"] = df["location"].str.split(",")
    
    index = []
    for k, i in enumerate(df["location"]): 
        if "강남구" in i:
            index.append(k)
    
    df = df.loc[index]
    df1 = pd.DataFrame(index=df["name"], columns=set(df["location"].sum()))
    
    for i, k in zip(df["name"], df["location"]):
        df1.loc[i, k] = 1
    df1 = df1.fillna(0)

    return df, df1

def choose_customer(df_order, df):
    # 강남3구, 용산 임의선택
    order = []
    for k, i in enumerate(df_order["지역구분"]):
        if "1지역" in i:
            order.append(k)

    df_order = df_order.iloc[order]
    # 고객의 수
    cust_number = len(df_order)
    # 테크니션 수(테스트를 위한 임의조정)
    
    tech_number = tech_number = len(df) 
    # df_order.TB_USER_ID_EMPLOYEE.nunique()

    return df_order, cust_number, tech_number


def cal_duration_naver(df_order, tech_number):
    # 위경도 데이터타입변경 
    df_order['USER_LAT'] = df_order['USER_LAT'].astype(float)
    df_order['USER_LNG'] = df_order['USER_LNG'].astype(float)
    
    # 데이터 시간순 재정렬 
    df_order = df_order.sort_values(by = "ORDER_START_DATE")
    df_order = pd.concat([df_order[:tech_number], df_order], axis=0)
    # 테크니션 위치 위도/경도는 사용하지 않으므로 임의로 0으로 지정 
    df_order[:tech_number] = 0   
    
    # 네이버 API 사용
    # 위도, 경도축출
    import time
    from urllib.request import urlopen, Request
    import json
    distance = []
    duration = []
    
    cnt = 0
    error = 0
    index = []
    situ = []
    
    # # Naver api (변경필요)
    client_id = "5nq2hgq4ct"
    client_pw = "eAcaE6E3aJaqTTNK1dtZaQy6XkmzwtcwQGDxul0H"
    
    distance = []
    for k, i in zip(df_order['USER_LAT'], df_order["USER_LNG"]):
        for kk, ii in zip(df_order['USER_LAT'], df_order["USER_LNG"]):
            distance.append([k, i, kk, ii])
            cnt += 1
            print(k, i, kk, ii, "in progress")
    
            # 위경도 동일할시 이동시간은 0 
            if (k == kk) & (i == ii):
                distance.append(0)
                duration.append(0)
                situ.append([k, i, kk, ii])
                
            # 위경도가 0을 포함할 시 이동시간은 0 
            elif (k == 0) | (kk == 0):
                distance.append(0)
                duration.append(0)
                situ.append([k, i, kk, ii])
    
            else:
                #print(k, i, kk, ii)
                url = f"https://naveropenapi.apigw.ntruss.com/map-direction/v1/driving?start={i},{k}&goal={ii},{kk}&option=trafast"
    
                request = Request(url)
                request.add_header("X-NCP-APIGW-API-KEY-ID", client_id)
                request.add_header("X-NCP-APIGW-API-KEY", client_pw)
    
                response = urlopen(request)
    
                response_body = response.read().decode('utf-8')
                response_body = json.loads(response_body)
    
                situ.append([k, i, kk, ii])
    
                duration.append(
                    round(
                        response_body['route']["trafast"][0]["summary"]["duration"]
                        / 1000 / 60))
                print(
                    round(
                        response_body['route']["trafast"][0]["summary"]["duration"]
                        / 1000 / 60))
                distance.append(
                    response_body['route']["trafast"][0]["summary"]["distance"] /
                    1000)

    distance = np.array(duration)
    distance = distance.reshape(len(df_order), -1)
    
    colname = []
    for i in range(len(df_order)):
        colname.append(f"Cust{i+1}")
    
    dist = pd.DataFrame(columns=colname, data=distance, index=colname)
    
    
    addtional_duration = 10 
    
    dist[dist != 0] += addtional_duration 
    
    # 삼각행렬 만들기
    for i, l1 in enumerate(dist):
        for j, l2 in enumerate(dist):
            if i < j:  
                dist.loc[l1, l2] = 0 
    dist = dist.T
    print(dist)
    return dist

def create_schedule(df_order, cust_number, tech_number, dist, df1):
    
    df_order.ORDER_START_DATE =  pd.to_datetime(df_order.ORDER_START_DATE)

    customer_name = []
    for i in range(cust_number):
        customer_name.append("C" + str(i + 1) + ":" + "point" +
                             str(i + tech_number))
    
    point_bag = []
    for i in range(len(df_order)): 
        point_bag.append(f'point{i+1}')
        
    customer_match = [] 
    point_list = []
    
    # 고객이름:위치 매치 만들기 
    for i in range(cust_number):
        customer_match.append("C" + str(i + 1) + ":" + "point" + str(i + tech_number + 1))
        point_list.append("point" + str(tech_number + i +1))
    
    # 고객위치 
    C_bag = ["C" + str(i + 1) for i in range(cust_number)]
    customer_location = point_list
    
    schedule = pd.DataFrame(
        columns=[customer_match],
        index=["service type", "Sales", "Duration", "Start time", "End time", "rsv", "address"])
    
    schedule.loc["service type"] =  df_order.total_duration.values
    schedule.loc["Sales"]  = df_order["price"].values
    schedule.loc["Duration"] = df_order.total_duration.values
    
    schedule.loc["Start time"] = df_order.ORDER_START_DATE.dt.time
    schedule.loc["Start time"] = str(df_order.ORDER_START_DATE.dt.time)
    timelist = [str(i) for i in df_order.ORDER_START_DATE.dt.time]
    schedule.loc["Start time"] = timelist
    schedule.loc["Start time"] = pd.to_datetime(schedule.loc["Start time"]).dt.time
    
    # 근무시작시간
    w_start_time = 10 * 60
    
    schedule.loc["Start time"] = schedule.loc["Start time"].apply(
        lambda x: 60 * x.hour + x.minute - w_start_time)
    schedule.loc["End time"] = schedule.loc["Start time"]
    schedule.loc["Due time"] = schedule.loc["End time"] + schedule.loc["Duration"]
    
    customer_name = C_bag
    
    schedule.loc["location"] = customer_location
    schedule.loc["names"] = customer_name
    schedule.loc["rsv"] = df_order.ORDER_START_DATE.dt.time.values
    schedule.loc["address"] = df_order.address.values
    
    schedule = schedule.reindex([
        'names', 'location', 'service type', 'Sales', 'Start time', 'End time',
        'Due time', 'Duration', "rsv", "address"
    ])

    # 유연화
    time_flex = 30
    schedule.loc["Start time"] = schedule.loc["Start time"].apply(lambda x: x - time_flex if x !=0 else x)
    schedule = schedule.T
    tmp = pd.DataFrame(
        schedule[schedule["rsv"].values >= datetime.time(17, 30, 00)]
        ["location"])["location"].apply(lambda x: x[5:]).astype(int).values
    schedule = schedule.T

    # 출퇴근시간 해당 예약타임
    dist[dist.iloc[:, tmp.min()-1:tmp.max()] != 0] = dist[dist.iloc[:, tmp.min()-1:tmp.max()] != 0] * 2
    
    schedule.drop("rsv", axis = 0, inplace = True)
    
    tech_name = []
    for i in range(tech_number):
        tech_name.append(f"technician{i+1}")

    depot = point_bag[:tech_number]

    tech = pd.DataFrame(columns=tech_name,
                        index=["Minutes", "Depot"])

    # 총근무시간
    working_time = constant_v.workingTime

    tech.loc["Minutes"] = working_time
    tech.loc["Depot"] = depot

    # 테크니션 이름 딕셔너리 관리
    tech_to_names = {i: k for i in df1.index for k in tech.columns}
    name_to_techs = {k: i for i in df1.index for k in tech.columns}

    df1.index = tech.columns

    schedule = schedule.T
    
    
    # 지역배정설정
    schedule["coveredby"] = np.nan

    tech_avail = []
    for i in range(len(schedule)): 
        loc = schedule["address"][i]
        tech_avail.append(list(df1[df1[loc] == 1].index))
        #schedule["available_technician"][i] = tech_as
    
    schedule["coveredby"] = tech_avail

    schedule = schedule.T

    named = []
    for k, i in enumerate(schedule.loc["address"]):
        named.append(df1[df1[i] == 1].index)
    
    service_list = {}
    for k, i in enumerate(schedule.loc["service type"].unique()):
        service_list.update({k: i})
    
    duration_backup = schedule.loc["service type"].values
    
    schedule.loc["service type"] = schedule.loc["names"] + schedule.loc["address"]

    canCover = {i: k for i, k in zip(schedule.loc["names"], schedule.loc["coveredby"])}

    product = pd.DataFrame(index=schedule.loc["names"],
                           columns=["Duration"],
                           data=schedule.loc["Duration"].values)

    # 일 우선순위 부여하기
    product["Priority"] = 1

    # 상품명
    # service_list.keys()case의 경우 선호도는 모두 동일하므로 1로 설정해 준다.
    product.index = schedule.loc["service type"].values

    return product, schedule, canCover, tech, tech_name, point_bag



def main():
    os.environ['GRB_LICENSE_FILE'] = constant_v.license_location
    splitLine = '--------------------------------------------------------------------------------'
    warningLine = '********************************************************************************'

    print("Start")
    try:
        df_order = data_loading(constant_v.path)

    except Exception as e:
        print(e)
        raise

    # data backup & cleaning
#    df_order, cust_number, tech_number = data_cleaning(result)
#   print(splitLine)
#    print(f"총 고객의 수 : {cust_number} 명")
#    print(f"총 테크니션 수 : {tech_number} 명")
#    print(splitLine)

    df_order = create_address(df_order)
    df_order["address"].value_counts()
    reg, df_order = divide_location(df_order, constant_v.path)
    df_order["지역구분"].value_counts()
    df, df1 = create_techician(constant_v.path)

    df_order, cust_number, tech_number = choose_customer(df_order, df)
    print(splitLine)
    print(f"총 고객의 수 : {cust_number} 명")
    print(f"총 테크니션 수 : {tech_number} 명")
    print(splitLine)

    dist = cal_duration_naver(df_order, tech_number)

    product, schedule, canCover, tech, tech_name, point_bag = \
        create_schedule(df_order, cust_number, tech_number, dist, df1)
    print('Creating schedule matrix completed.')
    print(splitLine)
    schedule
    print(splitLine)

    # prepare input datas
    print('Run gurobi')

    current_util, jobStrList, routeDic, totCap, totUsed, notUsedT, notAssined, startCorrectedAssined, \
    endCorrectedAssined, lateAssined, routeList = gurobi_models.run_model \
        (schedule, product, dist, tech, tech_name, point_bag, canCover)

    print(splitLine)
    print("------------------------------------RESULT--------------------------------------")
    print(splitLine)
    print("current util: " + str(current_util) + '\n')
    print("job str List: " + str(jobStrList) + '\n')
    print("routeDic: " + str(routeDic) + '\n')
    print(warningLine + '\n')
    print(warningLine + '\n')
    print("not Assiend job: " + str(notAssined) + '\n')
    print("not used technicians: " + str(notUsedT) + '\n')
    print("late start job: " + str(lateAssined) + '\n')
    print("start time corrected: " + str(startCorrectedAssined) + '\n')
    print("end time corrected: " + str(endCorrectedAssined) + '\n')

    print(routeList)
    techUnique = routeList['name'].unique()
    for i in techUnique:
        curTechDf = routeList[routeList['name'] == i]
        for j in range(len(curTechDf) - 1):
            cur_route = curTechDf.iloc[j]
            nxt_route = curTechDf.iloc[j + 1]
            print(str(cur_route['name']) + '은 ' + str(cur_route['location']) + '에서 다음까지 ' +
                  str(int(nxt_route['start']) - int(cur_route['end'])) + '분 시간이 남음' + '\n')


if __name__ == "__main__":
    main()
