rm -r out*
cp /home/ankitkap/workspace/PigProject/src/ankit/* ~/dic/hw3/experiments/ankit
javac -cp pig.jar ankit/*.java
jar -cf udf_ankit.jar ankit

