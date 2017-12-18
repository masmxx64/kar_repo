#!/bin/bash

rm -r output_salary
rm -r output_abroad
`printenv SPARK_PATH`/bin/spark-submit --class "Salary" --master $1 Salary/target/Salary-1.0-jar-with-dependencies.jar
