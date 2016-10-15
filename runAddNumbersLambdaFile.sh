# Argumento: Ruta de HDFS
#spark-submit --class "org.rhcalero.bigdata.module1.addnumbersspark.AddNumbersLambdaFile" --master local[8] target/addnumbersspark-0.0.1-SNAPSHOT-jar-with-dependencies.jar #workspace/inputs/addnumbers/numbers/

# Argumento: Ruta local
spark-submit --class "org.rhcalero.bigdata.module1.addnumbersspark.AddNumbersLambdaFile" --master local[8] target/addnumbersspark-0.0.1-SNAPSHOT-jar-with-dependencies.jar $HOME/workspace/01_Inputs/addnumbers/numbers/
