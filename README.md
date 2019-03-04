# Python/Pandas Coding Challenge

## Overview
Using Quandl's python API, I pulled down continuous futures contracts from the CHRIS database.  These included:
<ul>
<li>S&P E-mini (CME_ES)</li>
<li>Nasdaq 100 E-mini (CME_NQ)</li>
<li>Crude Oil (CME_CL)</li>
<li>Natural Gas (CME_NG)</li>
<li>Gold (CME_GC)</li></ul>

I stored these in a MySQL database in a table format.  Unfortunately, this method took almost a full minute of load time with
each call. To increase the speed of queries, I converted these to CSV files and then pickled the data.  The .pkl files are available in this repository.

I then created functions to calculate volatility, annualized volatility, trailing 1-year volatility, largest single day return, 
and largest annual return.  

## Learning
Before starting this challenge, I had briefly begun teaching myself python.  This was my first time using the Quandl, Pandas, Matplotlib, and
MySQL Workbench. Hence, I dedicated a lot of my time during the past week to educating myself in these new technologies through tutorials and documentation.
This was also my first time using development to dive deeply into data analysis.

## Possible Improvements
There are some ways I would like to improve my code:

<ul>
<li>Make API calls without lag so that my data is live instead of saved to a file.</li>
<li>Use input from user to reduce amout of code.  Example:</li>
<a href="https://imgur.com/uhJl1Rt"><img src="https://i.imgur.com/uhJl1Rt.png?1" title="source: imgur.com" /></a><br>
Here, you can see that I have multiple lines of repetetive code.  I attempted to have the user input replace the contract roots in these lines,
but since user input is returned as a string, I was unable to concatenate it into the code.<br><br>
     I would like to learn the most efficient method, as I could reduce 18 lines of code into 4!
</ul>

## Instructions
Run the python code, and it will ask for a contract root (Example: CME_ES).  This will lead the user through annualized volatility,
trailing one year volatility, largest single day return, and annual return (amount, not percent).  

## Technologies Used
Python, Pandas, Pycharm, Quandl, Matplotlib, MySQL, MySQL Workbench, Stack Overflow
