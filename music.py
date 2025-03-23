from pyspark.sql import SparkSession
import json

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("people")\
        .getOrCreate()

    print("read dataset.csv ... ")
    path_people = "dataset.csv"
    df_people = spark.read.csv(path_people, header=True, inferSchema=True)
    df_people = df_people.withColumnRenamed("year", "fechalanzamiento")
    df_people.createOrReplaceTempView("music")
    query = 'DESCRIBE music'
    spark.sql(query).show(20)

    # Corrección de la consulta: escapar el nombre de la columna ?Format y usar = en lugar de ==
    query = """SELECT `?Format`, fechalanzamiento 
               FROM music 
               WHERE fechalanzamiento = 1975 
               ORDER BY `fechalanzamiento`"""
    df_people_names = spark.sql(query)
    df_people_names.show(20)

    # Segunda consulta: funciona bien, pero ajustamos para consistencia
    query = """SELECT `?Format`, `fechalanzamiento` 
               FROM music 
               WHERE `fechalanzamiento` BETWEEN 1973 AND 2000 
               ORDER BY `fechalanzamiento`"""
    df_people_1903_1906 = spark.sql(query)
    df_people_1903_1906.show(20)
    
    # Guardar resultados
    results = df_people_1903_1906.toJSON().collect()
    df_people_1903_1906.write.mode("overwrite").json("results")
    
    with open('results/data.json', 'w') as file:
        json.dump(results, file)
    
    spark.stop()