# coding=utf-8
# ☯ Author: SDLPython
# ☯ Email : aoguangjin@chsdl.com
# ☯ Date  : 2021/3/23 15:32
# ☯ File  : test5.py


def run(self):
    AlarmInfo_sql = "SELECT a.DGIMN,a.PollutantCode,a.AlarmRule,a.AlarmType,a.IsDevice,a.TimeInterval,a.RunTime,b.IsPush,b.PushType,b.PushObject,b.PushInterval FROM Base_AlarmRuleInfo a LEFT JOIN Base_PushRuleInfo b  on a.DGIMN = b.DGIMN and a.AlarmType = b.AlarmType"
    df = pd.read_sql(AlarmInfo_sql, con=conn)
    all_data = []
    job_id = 0
    for name, group in df.groupby('RunTime'):
        d = group.to_dict(orient='records')
        for i in range(0, len(d), 200):
            item = {}
            b = d[i:i + 200]
            job_id += 1
            item['job_id'] = job_id
            item['time_interval'] = name
            item['data'] = b
            all_data.append(item)
    print(all_data)