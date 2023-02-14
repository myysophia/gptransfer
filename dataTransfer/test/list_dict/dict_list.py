'''
城市信息展示
（省市级联显示）
第一种--使用for循环----
'''
dict_city = {'陕西':['西安','咸阳','榆林','铜川'],
             '河南':['郑州','开封','安阳','商丘'],
             '湖北':['武汉','黄冈','周口','禹州']}

for i in dict_city.keys():
    print('----',i,'----')
    for val in dict_city[i]:
        print('|-',val)

'''
城市信息展示
（省市级联显示）
第二种--使用迭代器----
'''
dict_city = {'陕西':['西安','咸阳','榆林','铜川'],
             '河南':['郑州','开封','安阳','商丘'],
             '湖北':['武汉','黄冈','周口','禹州']}

dict_iter = iter(dict_city)
dict_val = iter(dict_city.values())
while True:
    try:
        pro_name = next(dict_iter)
        print('--%s--'%pro_name)
        val = next(dict_val)
        val_name = iter(val)
        while True:
            try:
                print('|--%s'%next(val_name))
            except StopIteration:
                print('--'*20)
                break

    except StopIteration:
        print('结束')
        break



