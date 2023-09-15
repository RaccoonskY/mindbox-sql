from pyspark.sql import *
from pyspark.sql.functions import  asc

spark = (
    SparkSession.builder
    .master("local")
    .appName("mindbox")
    .getOrCreate()
)

products = spark.createDataFrame([
    {"id": 1, "name": "bread"},
    {"id": 2, "name": "bread2"},
    {"id": 3, "name": "milk"},
    {"id": 4, "name": "human"},
    {"id": 5, "name": "monkey"},
    {"id": 6, "name": "cucumber"},
    {"id": 7, "name": "egg"},
    {"id": 8, "name": "shovel"},
])

categories = spark.createDataFrame([
    {"id": 1, "category_name": "eatable"},
    {"id": 2, "category_name": "bakery"},
    {"id": 3, "category_name": "creature"},
    {"id": 4, "category_name": "uneatable"},
    {"id": 5, "category_name": "stuff"}
])

# for many-to-many relations there must be additional entity to represent connections ( with fk_key (pr_key1), pr_key)
product_categories = spark.createDataFrame([
    {"pr_id": 1, "cat_id": 1},
    {"pr_id": 1, "cat_id": 2},
    {"pr_id": 2, "cat_id": 1},
    {"pr_id": 2, "cat_id": 2},
    {"pr_id": 3, "cat_id": 1},
    {"pr_id": 4, "cat_id": 3},
    {"pr_id": 4, "cat_id": 4},
    {"pr_id": 5, "cat_id": 3},
    {"pr_id": 5, "cat_id": 4},
    {"pr_id": 6, "cat_id": 1},
    {"pr_id": 7, "cat_id": 1},
])

products.\
    join(product_categories, product_categories.pr_id == products.id, "outer").\
    join(categories, product_categories.cat_id == categories.id, "left").\
    select(products.name, categories.category_name).\
    orderBy(asc("name")).\
    fillna("undefined").\
    show()


