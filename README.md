# bd-flights

Big Data programming course project.
We downloaded the Kaggle Airlines Delay dataset (https://www.kaggle.com/giovamata/airlinedelaycauses) and we performed the following queries:
* Order the companies in descending order based on the average arrival delay, consider only non-canceled and non-hijacked flights
* For each airport, indicate the average taxi-out delay, grouped by the time slots created

We use performed them using:
* Hadoop
* Spark
* SparkSQL

In the end we used MLlib to train some Machine learning algorithms to understand, from the characteristics of the flight, if it could be subject to delay.
We depicted the results with Tableu.
