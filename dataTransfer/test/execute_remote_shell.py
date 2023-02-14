# coding:utf-8
import time,paramiko,re
from io import StringIO

def exec_shell(command):
    '''
        command：传入的要执行的shell命令
    '''
    f = StringIO.StringIO()
    header_match = '(\[.+?@.+?\s.+?\]\$)'
    ssh.send(command+'\n')
    while True:
        out = ssh.recv(1024)
        print(out)
        f.write(out)
        header_list = re.findall(header_match, out)
        if header_list and out.strip().endswith(header_list[-1]):
            break
    return f

def check_ip(content):
    '''
        从content中取出所有符合xx.120.xx.xx格式的ip地址（xx代表任意多数字）并返回
    '''
    ips = re.findall('\d+\.120\.\d+\.\d+',content)
    return ips

if __name__ == '__main__':
    '''
        host：对应要连接的服务器ip
        port：对应连接服务器的端口
        username：对应访问服务器的用户名
    '''
    host = '10.50.10.170'
    port = 22
    username = 'root'
    passwd = 'chot123'
    '''
        key_file为secureCRT对应的OpenSSH格式的私钥文件
        可以在secureCRT的'Tools->Convert Private Key to OpenSSH Format...'选择相应的私钥文件转化为OpenSSH格式
        例如：在Windows下保存到'E:\keys\'路径下，保存文件名为'id_rsa'
    '''
    # key_file = 'E:\\keys\\id_rsa'
    # key = paramiko.RSAKey.from_private_key_file(key_file)
    s = paramiko.SSHClient()
    s.load_system_host_keys()
    s.set_missing_host_key_policy(paramiko.AutoAddPolicy()) # 自动接受远程服务器host key
    # s.connect(host, port, username, pkey=key)
    s.connect(host, port, username, passwd)
    # ssh = s.invoke_shell()
    '''
        下面对应在secureCRT上执行命令的过程
    '''
    start = '2020-10-13 14:00:00'
    end = '2020-10-13 16:00:00'
    tablename = 'wpp_adefect_f_n'
    # stdin, stdout, stderr = ssh.exec_command('sh dataTrs.sh' + start + end + tablename)  # 远程服务器要执行的命令
    stdin, stdout, stderr = ssh.exec_command('df -TH')  # 远程服务器要执行的命令
    for line in stdout:
        print(line)
    ssh.close()  # 关闭ssh连接
    # exec_shell('cd /home/scripts/dataTransfer')
    # out = exec_shell('ls')
    # ips = check_ip(out.getvalue())
    # exec_shell('cat dataTrs.sh')