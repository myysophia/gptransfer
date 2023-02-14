'''
循环列表中字典元素
'''
info_list = [
    {'name':'zhao','age':'22','hight':'171'},
    {'name':'qian','age':'23','hight':'165'},
    {'name':'sun','age':'24','hight':'148'},
    {'name':'li','age':'25','hight':'166'}
]

# 第一种方式
index = 0
while index < len(info_list):
    print('name:%s\nage:%s\nhight:%s'%(info_list[index]['name'],info_list[index]['age'],info_list[index]['hight']))
    index +=1

# 第二种方式
print('-'*30)
for i in info_list:
    print('name:%s\nage:%s\nhight:%s'%(i['name'],i['age'],i['hight']))