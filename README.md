# CS441 - Homework 3 - Aditya Sawant
#
##### Description: Create a Spark program for parallel processing of the predictive engine for stock portfolio losses using Monte Carlo simulation in Spark. Make use of a  [web service to obtain historical securities information.](https://www.alphavantage.co/documentation/) 

## Overview
As part of this assignment, a selection of 5 random stocks/stock symbols is made. Then a call is made to the web-service to obtain historical data of the stock prices. The Monte Carlo Simulation code is implemented with spark. 

First we feed the REST url for each stock symbol into spark in order to create a dataframe from it. Then we operate over this dataframe to calculate various metrics such as standard deviation and volatility. Using these metrics and the earliest price of the stock, we start to generate multiple projections, from which we calculate the maximized profit for each simulation and also create a data frame which consists of the projection points for each Monte Carlo Simulation.

## Instructions 
**SBT** is needed to build a jar for Spark. Additionally, **Hadoop** should also be setup.   
Once cloned from the repository, open the terminal or command prompt, cd to the directory where the project is cloned and then run the following command:  
#
```sbt clean compile assembly```    
#
Run the following command in the directory where the jar has been generated/stored: 
#
``` spark-submit --class Driver --master yarn --deploy-mode client ./aditya_sawant-hw3-assembly-0.1.jar <outPutPath> ```
#
**P.S:** 
The output for the given job will be created and stored in the directory mentioned in the args.
The output for each Monte Carlo Simulation for each stock is a file which contains the maximum profit that can be obtained for each projection in that stock's Monte Carlo Simulation run.

**For AWS EMR** visit the following channel: 
[AWS EMR Execution Steps](https://www.youtube.com/channel/UCjewyw72Ed-_y94u9nDLXQw)
