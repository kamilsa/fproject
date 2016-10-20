# Spark and Hadoop usage manual

На данный момент у нас есть Спарк сервер и HDFS хранилище, работающие на двух компьютерах. Здесь приведены основные инструкции по тому как:

1. Запустить сервер
2. Запустить IPython вместе с PySparkом
3. Починить сервер, если IP поменялись

P.S. в данном мануале будет использоваться IP сервера 10.90.131.33. Возможно, на момент прочтения он уже поменяется

## Запуск Спарк и Хадуп сервера:

### Spark

1. Подулючиться к компу Артура (мастер нода) через ssh:
```
ssh hduser@10.90.131.33
# password "123"
```
Если подключиться не удается, значит либо ip устарел, либо компьютер не работает, либо Камиль сейчас играет там в Фифу

2. Перейти в папку со спарком:
```
cd /usr/local/spark
```
3. Собственно, запустить Спарк:
```
./sbin/start-all.sh
```
4. Удостовериться, что сервер работает и все ноды "видны". Для этого перейти вбить в браузере ip мастер ноды с портом 8080:
```
10.90.131.33:8888
```
На момент написания, у нас там должно появиться 2 ноды

5. Если появиться надобность выключить спарк, то можно вызвать команду:
```
./sbin/stop-all.sh
```

### Хадуп (для HDFS)
Чтобы работать со спарком необходимо, чтобы данные были распределены по всем нодам. Самый правильный способ добиться этого, это хранить данные в HDFS, как пользоваться HDFS можно прочитать по ссылке [здесь](http://www.bogotobogo.com/Hadoop/BigData_Hadoop_fs_commands_list.php)
 1. Быть подключенным к мастер ноде (см. выше как подключиться)
 2. Запустить HDFS:
 ```
 start-dfs.sh
 ```
 3. Проверить, что все заработало, перейдя в WEB-UI для HDFS:
 ```
 10.90.131.33:50070
 ```
 Если все работает верно, то мы увидим информацию, что у нас там 2 ноды и configured memory больше 0 и составляет несколько гигабайт.

 4. Если понадобиться выключить HDFS, то можно написать:
 ```
 stop-dfs.sh
 ```

 P.S. Я бы не советовал хранить особо важные данные в HDFS, если вдруг что-то сломается, данные могут потеряться, поэтому лучше периодически переносить оттуда результаты в обычное хранилище на диске

## Запустить IPython с PySpark

Prerequisites:

1. Иметь запущенным Спарк сервер

Порядок действий:
1. Перейти в папку, где вы хотите создать/запустить тетрадку
2. Подключиться к любой виртуальной среде с юпитером, например:
```
source activate venv
```
3. Запустить alias:
```
sparkjupyter
```
Этот alias запускает следующую команду:
```
PYSPARK_DRIVER_PYTHON=ipython PYSPARK_DRIVER_PYTHON_OPTS="notebook --no-browser --port=7777" pyspark --master spark://$(gethostip -d hadoop-master):7077 --executor-memory 3000M --driver-memory 3000M
```

То есть, вместо алиаса можно запускать эту команду, и менять конфигурации запуска вашего приложения. Здесь их только 2:
1. executor memory -- сколько оперативы каждый воркер отдаст приложению
2. driver memory -- сколько оперативы отдаст мастер
Но вы, конечно, можете добавлять любые другие конфигурации [отсюда](http://spark.apache.org/docs/latest/configuration.html)

4. Чтобы иметь доступ из своего браузера, необходимо "пробить порты" с сервера:
```
ssh -NL 8157:localhost:7777 hduser@10.90.131.33
```

7777 -- это порт в котором запускается юпитер на сервере
5. Теперь, можно открыть юпитер по адресу:
```
localhost:8157
```

P.S. По окончании работы, не забывайте выключать ваши юпитеры, чтобы не тратить впустую ресурсы

## Пофиксить измененные IP

На каждой ноде (мастер и воркеры) открыть файл ```/etc/hosts``` любым редактором и поменять там ip адреса на новые. Сейчас файл с ip адресами выглядит так:
```
127.0.0.1       localhost
127.0.1.1       dslab-1

# The following lines are desirable for IPv6 capable hosts
::1     ip6-localhost ip6-loopback
fe00::0 ip6-localnet
ff00::0 ip6-mcastprefix
ff02::1 ip6-allnodes
ff02::2 ip6-allrouters

10.90.131.33 hadoop-master
10.90.131.33 hadoop-slave-1
10.90.131.46 hadoop-slave-2
```

Нас интересуют последние 3 строчки. И да, сейчас у нас мастер нода -- это один воркеров. Будет больше машин, исправим. А так на данный момент у нас:
* hadoop-slave-1 -- комп Артура (он же мастер (он -- это комп, не Артур))
* hadoop-slave-2 -- комп Ильдара
* В будущем раздобудем больше компов, добавим новые ноды
