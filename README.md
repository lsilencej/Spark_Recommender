# Spark 推荐系统

## 技术栈

### 数据处理

Spark Core + Spark SQL + MongoDB

### 离线推荐

- 静态数据处理：Spark Core + Spark SQL
- 推荐服务：Spark Core + Spark MLlib

### 在线推荐

- 获取消息服务：Redis + Kafka
- 推荐服务：Spark Streaming

## 数据集格式

### 商品数据集（Product）

|   字段名   |  类型  |   描述   |    说明    |
| :--------: | :----: | :------: | :--------: |
|    _id     |        |          |  自动生成  |
| productId  |  int   | 商品 id  |            |
|    name    | String | 商品名称 |            |
|  imageUrl  | String | 商品图片 |            |
| categories | String | 商品分类 | 由 \| 分隔 |

### 评分数据集（Rating）

|  字段名   |  类型  |      描述      |   说明   |
| :-------: | :----: | :------------: | :------: |
|    _id    |        |                | 自动生成 |
|  userId   |  int   |    用户 id     |          |
| productId |  int   |    商品 id     |          |
|   score   | double |    用户评分    |          |
| timestamp |  int   | 评分时的时间戳 |          |

### 用户数据集（User）

|  字段名  |  类型  |   描述   |   说明   |
| :------: | :----: | :------: | :------: |
|   _id    |        |          | 自动生成 |
|  userId  |  int   | 用户 id  |          |
| username | String | 用户账号 |          |
| password | String | 用户密码 |          |

## 功能模块

### DataLoader

- SparkContext 加载初始数据
- 构造 DataFrame
- 写入 MongoDB

## 离线服务

### 静态数据处理（StaticRecommender）

#### 最近热门商品（RecentlyHotProducts）

- 修改时间戳

  ```sql
  select productId, score, changeDate(timestamp) as time from ratings
  ```

- 通过 SQL 命令进行筛选排序

  ```sql
  select productId, count(productId) as count, time from ratingByTimeTemp group by time, productId order by time desc, count desc
  ```

|  字段名   | 类型 |      描述      |     说明     |
| :-------: | :--: | :------------: | :----------: |
|    _id    |      |                |   自动生成   |
| productId | int  |     商品id     |              |
|   count   | int  | 最近评价的人数 | 由多到少排序 |
|   time    | date |   评价的时间   |  按最近排序  |

#### 好评商品（AverageRateProducts）

- 计算每个商品的平均分，降序得到结果

  ```sql
  select productId, avg(score) as avg from ratingTemp group by productId order by avg desc
  ```

  |  字段名   |  类型  |     描述     |     说明     |
  | :-------: | :----: | :----------: | :----------: |
  |    _id    |        |              |   自动生成   |
  | productId |  int   |    商品id    |              |
  |    avg    | double | 商品平均评分 | 由大到小排序 |

### 离线推荐（OfflineRecommender）

采用 ALS 算法作为协同过滤算法，根据用户评分表计算每个用户的推荐列表以及商品的相似度矩阵

使用 MLlib 下的 ALS.train() 进行训练，model.predict() 进行预测

- 加载用户评分数据
- 将数据分为0.8/0.2两部分分别作为训练数据以及测试数据
- 训练模型找到满足于 rmse 最小的 rank 和 lambda

​		rmse 计算公式：

![clip_image002](C:\Users\23100\Desktop\Recommender\assets\clip_image002.png)

​		observed(t) 为观测值，即给定的

​		predicted(t) 为预测值，即根据训练的模型进行预测得到的值

- 将评分数据表中的用户和商品作笛卡尔积并通过模型进行预测用户评分，并写入 MongoDB

  基于用户的推荐（OfflineUserRecommends）

  |   字段名   |            类型             |            描述            |   说明   |
  | :--------: | :-------------------------: | :------------------------: | :------: |
  |    _id     |                             |                            | 自动生成 |
  |   userId   |             int             |           用户id           |          |
  | recommends | (Array)（productId, score） | 推荐商品（商品Id, 相似度） |          |

- 通过 model.productFeatures 获得各个商品的特征向量

- 将特征向量与自身作笛卡尔积，过滤掉与自身相同的商品，得到商品与其他商品间的相似度关系

- 计算余弦相似度并根据商品 id 进行分组，将推荐结果写入到 MongoDB

  基于商品的推荐（ProductRecommends）

  |   字段名   |            类型             |            描述            |   说明   |
  | :--------: | :-------------------------: | :------------------------: | :------: |
  |    _id     |                             |                            | 自动生成 |
  | productId  |             int             |           商品id           |          |
  | recommends | (Array)（productId, score） | 推荐商品（商品Id, 相似度） |          |

## 在线服务

### 在线推荐（OnlineRecommender）

根据用户的行为，比如给商品打分进行实时推荐

- 加载 ProductRecommends 中的数据作为实时计算的基础数据

- 用户 u 对商品 q 进行一次评分触发一次计算

- 从 ProductRecommends 中选出与商品 q 最相似的 N 个商品作为集合 S，其中需要过滤用户未进行打分的商品

- 从 Redis 中获取用户最近时间内的 K 条评分，包含本次评分作为集合 RK

- 计算商品的推荐优先级，产生推荐结果集合

  推荐优先级计算公式：

  ![clip_image002](C:\Users\23100\Desktop\Recommender\assets\clip_image002.jpg)

  uq 表示用户 u 对商品 q 进行评分

  RK 表示一段时间内用户的所有评分

  sim(q, r) 表示商品 q 与商品 r 的余弦相似度

  R(r) 表示用户对商品 r 的评分

  sim_sum 表示 q 与 RK 中商品相似度大于最小阙值的个数

  incount 表示 RK 中与商品 q 相似的集合中，评分大于等于 3 的商品个数

  recount 表示 RK 中与商品 q 相似的集合中，评分小于 3 的商品个数

- 将推荐结果集合写入 MongoDB

  实时推荐（OnlineUserRecommends）

  |   字段名   |            类型             |            描述            |   说明   |
  | :--------: | :-------------------------: | :------------------------: | :------: |
  |    _id     |                             |                            | 自动生成 |
  |   userId   |             int             |           用户id           |          |
  | recommends | (Array)（productId, score） | 推荐商品（商品Id, 相似度） |          |

