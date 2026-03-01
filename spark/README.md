# **Spark**


## Spark Submit

```
    docker exec -it spark-master bash
```

```
spark-submit \
  --master spark://spark-master:7077 \
  spark_test.py
```