# Task1-2-2-plot code
import matplotlib.pyplot as plt
# Using readlines()
file1 = open('task1_2_2_plot.txt', 'r')
Lines = file1.readlines()
 
y2009_number = []
y2010_number = []
y2011_number = []
y2009_month = []
y2010_month = []
y2011_month = []
for line in Lines:
   
    line_ =line
    line_ = line_.replace('"', ' ')
    line_ = line_.replace(',', ' ')
    line_ = line_.replace('[', ' ')
    line_ = line_.replace(']', ' ')
    line_ = line_.replace('  ', ' ')
    line_list = line_.split()
    if (line_list[0]== '2009'):
        y2009_number.append(line_list[1])
        y2009_month.append(line_list[2])
    if (line_list[0]== '2010'):
        y2010_number.append(line_list[1])
        y2010_month.append(line_list[2])  
    if (line_list[0]== '2011'):
        y2011_number.append(line_list[1])
        y2011_month.append(line_list[2])      
   

title_list =['2009','2010','2011']

for i in  title_list:
    if i == '2009' :   
        # x axis values
        x = y2009_month
        # corresponding y axis values
        y = y2009_number
    if i == '2010' :   
        # x axis values
        x = y2010_month
        # corresponding y axis values
        y = y2010_number
    if i == '2011' :   
        # x axis values
        x = y2011_month
        # corresponding y axis values
        y = y2011_number

    # plotting the bars
    plt.bar(x, y)
    
    # naming the x axis
    plt.xlabel('Month')
    # naming the y axis
    plt.ylabel('Visit')
    
    # giving a title to my plot
    plt.title(i)
    
    # function to show the plot
    plt.show()