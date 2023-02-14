import paramiko
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # 自动接受远程服务器host key

ssh.connect('10.50.10.170', 22, 'root', 'chot123')  # 远程主机IP、端口、用户名、密码
start = '2020-10-13 14:00:00'
end = '2020-10-13 16:00:00'
tablename = 'wpp_adefect_f_n'

# psql -c "copy (select * from sor.wpp_adefect_f_n where evt_timestamp >= '2020-10-13 14:00:00' and evt_timestamp <= '2020-10-13 15:00:00') to PROGRAM 'gzip > /mnt/wpp_adefect_f_n_1019.csv.gz'"

command = 'sh /home/scripts/dataTransfer/dataTrs_1021.sh ' + " '" + start + "' '" + end + "' '"+ tablename + "'"
command1 = 'df -TH'

print( command )

print( command1 )
stdin, stdout, stderr = ssh.exec_command(command)  # 远程服务器要执行的命令
channel = stdout.channel
status = channel.recv_exit_status()
print(status)
# result1 = stdout.read().decode("utf-8","ignore")
# print(result1)
# for line in stdout:
#     print(line)
ssh.close()  # 关闭ssh连接