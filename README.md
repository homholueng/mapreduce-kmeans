# mapreduce-kmeans

## 配置 HBase

在项目启动前请先将 HBase 目录下的 `conf/hbase-site.xml` 配置文件放置到 `src/main/resources` 文件夹下。

## 配置 /etc/hosts

在 `/etc/hosts` 中加入以下内容:

```
116.56.140.83 namenode01
116.56.140.84 namenode02
116.56.140.85 datanode01
116.56.140.86 datanode02
116.56.140.87 datanode03
116.56.140.88 datanode04
116.56.140.90 server01
```
