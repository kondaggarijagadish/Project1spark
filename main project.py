#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
from os import system
clear = lambda: system('cls')
import platform
import pandas as p
import matplotlib.pyplot as plt
import seaborn as sb
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from pyspark.sql.functions import *
df=spark.read.csv("C:\\Users\\konda\\Downloads\\python_notes\\insurance1.csv",inferSchema=True,header=True)
pandas_df=df.toPandas()
print("-------------------------------------------------------------------- ")
print("---*** WELCOME SHIELD INSURANCE CORPORATION---***")
print("---------------------------------------------------------------------")
A_name1="Shield@gmail.com"
A_password1="Shield@33"
while True:
    A_name=input("Enter Username ")
    A_password=input("Enter Password ")
    if (A_name==A_name1 and A_password==A_password1):
        print( " ...login was successfull... " )
        def datasetInfo():
            df.show()
            df.printSchema()
        def userchoiceToSelectRow():
            inp=int(input("Enter the number of rows you want to display : "))
            if inp<=1338:
                df.show(inp)
            else:
                print("You are exceding the dataset limit! can't fetch the data try again !")
        def availabledatacount():
            print("counting of all data according to users choice:--")
            print("""
                    1.for counting for female in data
                    2.for counting for male in data
                    3.for counting for region wise data
                    4.for counting of smokers data """)
            jk=int(input("Eneter your choice...."))
            if(jk==1):
                print(" female count in data: ",df.select("sex").filter(df['sex']=="female").count())
            if(jk==2):
                print("male count in data:- ", df.select("sex").filter(df['sex']=="female").count())
            elif(jk==3):
                print("Region wise data :- ",pandas_df['region'].value_counts())
            elif(jk==4):
                print("Smokers count:-",pandas_df['smoker'].value_counts(),df.select("smoker").filter(df['smoker']=='yes').count())
            elif(jk==5):
                print("Non smokers count:- ",df.select("smoker").filter(df['smoker']=='no').count())
            else:
                print("choose correct option") 
        def AgeandSexGraph():
            str=int(input('''1.Enter 1 for show in Bar graph (age,bmi)\n2.Enter 2 for Pie Chart (age,bmi)\n3.Enter 3 for kde Chart ("sex","age") \nPlease Enter your choice :'''))
            if str==1:
                df.select("age","bmi").toPandas().plot(kind="hist",x="age",y="bmi",title="SMOKER")
                plt.savefig("smokker.png")
            elif str==2:
                df.select("age","bmi").toPandas().plot(kind="area",x="age",y="bmi",title="SMOKER")
                plt.savefig("smokker.png")
            elif str==3:
                df.select("sex","age").toPandas().plot(kind="kde",x="sex",y="age",title="SMOKER",legend=False)
                plt.savefig("smokker.png")
            else:
                print("Enter valid option")
                AgeandSexGraph()

        def specificDetailsOfAgeSelection():
            while True:
                print("Minimum Age in the dataset is : ",df.groupby().min("age").first().asDict()["min(age)"])
                print("Maximum Age in the dataset is : ",df.groupby().max("age").first().asDict()["max(age)"])
                print("Age should be greater than 17 and less than 65")
                age=int(input("Enter Age that you want to fetch information from dataset : "))
                if age<18:
                    print("Provide age greater than 15 because dataset does not contain age group less than 15 !")
                    break
                elif age>=65:
                    print("Provide age less than 65 because dataset does not contain age group greater than 64 !")
                    break
                else:
                    print("Dataset range lies between 18 and 64 only try with those values : ")
                print()
                print("1. Enter 1 to see number of adults available in data set : ")
                print("2. Enter 2 to view the number of older adults available in the dataset : ")
                print("3. press 3 to exit : ")
                choice1 = int(input('select your option  : '))
                if choice1 == 1:
                    df.select("age", "sex", "bmi",  "children","smoker").filter((df["age"]>=18) & (df["age"]<=59)).alias("Adults").show()                                                                                      
                    print("Number of Adults Available in the dataset are : ",df.select("age", "sex", "bmi",  "children","smoker").filter((df["age"]>=18) & (df["age"]<=59)).alias("Adults").count())
                elif choice1 == 2:
                    df.select("age", "sex", "bmi",  "children","smoker").filter(df["age"]>=60).alias("Children").show()
                    print("Number of Older Adults Available in the dataset are : ",df.select("age", "sex", "bmi",  "children","smoker").filter(df["age"]>=60).alias("Older AdULTS").count())
                else:
                    break
        def sorting():
            str=int(input('''1.Enter 1 for show AGE in sorted order \n2.Enter 2 for show SEX in sorted order\n3.Enter 3 for show SMOKER in sorted order \nPlease Enter your choice :'''))
            if str==1:
                n=int(input("How many records you want :"))
                a=df.sort(asc("age"))
                a.show(n)
            elif str==2:
                n=int(input("How many records you want :"))
                b=df.sort(asc("sex"),asc("age"))
                b.show(n)
            elif str==3:
                n=int(input("How many records you want"))
                c=df.sort(asc("smoker"))
                c.show(n)
        
            else:
                print("Enter valid option")
                sorting()

        def MaleAdultSmokerInfo():
            str=input("Enter '1' for number of adult males who are smoking and '11' for adult males who are non smokers  : ").lower()
            age=int(input("enter age in which you want to see smokers list :"))     
            if age<=18:
                print("provide age greater than 17")
            if str=="1" :
                df.select("sex","age","bmi").filter((df['sex']=="male") & (df['smoker']=="yes") & (df['age']>=18)).alias("Adult Male Smokers").show()
                print("Number of Male Adults who are smokers are ",df.filter((df['sex']=="male") & (df['smoker']=="yes") & (df['age']>=18)).alias("Adult Male Smokers count").count())

            elif str=="11" :
                df.select("sex","age","bmi").filter((df['sex']=="male") & (df['smoker']=="yes") & (df['age']>=18)).alias("Adult Male Smokers").show()
                print("Number of Male Adults who are Non smokers are ",df.filter((df['sex']=="male") & (df['smoker']=="no") & (df['age']>=18)).alias("Adult Male Smokers count").count())
            else:
                print("Try Again! Provide the input which is correct as per dataset requirment")


        def MaleOlderAdultSmokerInfo():
            str=input("Enter '3' for number of Older adult males who are smoking and '33' for older adult males who are non smokers  : ").lower()
            age=int(input("enter age in which you want to see smokers list"))
            if age<=60:
                print("provide age greater than 45")
            if str=='3' :
                df.select("sex","age","bmi").filter((df['sex']=="male") & (df['smoker']=="yes") & (df['age']>=60)).alias("Adult Male Non Smokers").show()
                print("Number of Older Male Adults who are Smokers are ",df.filter((df['sex']=="male") & (df['smoker']=="yes") & (df['age']>=60)).alias("Older Adult Male Smokers count").count())
            elif str=="33" :
                df.select("sex","age","bmi").filter((df['sex']=="male") & (df['smoker']=="no") & (df['age']>=60)).alias("Older Adult Male Non Smokers").show()
                print("Number of Older Male Adults who are Non Smokers are ",df.filter( (df['sex']=="male") & (df['smoker']=="no") & (df['age']>=45)).alias("Older Adult Male Non Smokers count").count())
            else:
                print("Try Again! Provide the input which is correct as per dataset requirment")

        def FemaleAdultSmokerInfo():
            str=input("Enter '5' for number of adult females who are smoking and '55' for adult females who are non smokers  : ").lower()
            age=int(input("enter age in which you want to see smokers list"))
            if age<=18:
                print("provide age greater than 17")
            if str=='5' :
        
                df.select("sex","age","bmi").filter((df['sex']=="female") & (df['smoker']=="yes") & (df['age']>=18)).alias("Adult Female Smokers").show()
                print("Number of Female Adults who are Smokers are ",df.filter((df['sex']=="female") & (df['smoker']=="yes") & (df['age']>=18)).alias("Adult Female Smokers count").count())
            elif str=="55" :
                df.select("sex","age","bmi").filter((df['sex']=="female") & (df['smoker']=="no") & (df['age']>=18)).alias("Adult Female Non Smokers").show()
                print("Number of Female Adults who are Non Smokers are ",df.filter((df['sex']=="female") & (df['smoker']=="no") & (df['age']>=18)).alias("Adult Female Non Smokers count").count())
            else:
                print("Try Again! Provide the input which is correct as per dataset requirment")
        def FemaleOlderAdultSmokerInfo():
            str=input("Enter '6' for number of female older adult females who are smoking and '69' for female older adult females who are non smokers  : ").lower()
            age=int(input("enter age in which you want to see smokers list"))
            if age<=60:
                print("provide age greater than 45")
            if str=='6' :
                df.select("sex","age","bmi").filter((df['sex']=="female") & (df['smoker']=="yes") & (df['age']>=60)).alias("Older Adult Female Smokers").show()
                print("Number of Older Female Adults who are Smokers are ",df.filter((df['sex']=="female") & (df['smoker']=="yes") & (df['age']>=60)).alias("Older Adult Female Smokers count").count())
            elif str=="69" :
                df.select("sex","age","bmi").filter((df['sex']=="female") & (df['smoker']=="no") & (df['age']>=60)).alias("Older Adult Female Non Smokers").show()
                print("Number of Older Female Adults who are Non Smokers are ",df.filter((df['sex']=="female") & (df['smoker']=="no") & (df['age']>=60)).alias("Older Adult Female Non Smokers count").count())
            else:
                 print("Try Again! Provide the input which is correct as per dataset requirment")
        def GroupBySmoker():
            print("""
                    1.smokers information with maximum age 
                    2.smokers information with minimum age
                    3.smokers information with avarage of age
                    4.smokers information with avarage of bmi""")
            pp=int(input("choose your option "))
            if(pp==1):
                print("smokers information with maximum age ",df.groupBy("smoker").agg(count("charges"),count("sex"),max("age")).show())
            elif(pp==2):
                print("mokers information with minimum age",df.groupBy("smoker").agg(count("bmi"),count("sex"),min("age")).show())
            elif(pp==3):
                a=df.groupBy("smoker").avg("age")
                print("smokers information with avarage of age",a.show())
            elif(pp==4):
                b=df.groupBy("smoker").avg("bmi")
                print("smokers information with avarage of bmi",b.show())
            else:
                print("Valid option for seeing Smokers Information...")
        def BMIAnalysis():
            print("Minimum BMI in the dataset is : ",df.groupby().min("bmi").first().asDict()["min(bmi)"])
            print("Maximum BMI in the dataset is : ",df.groupby().max("bmi").first().asDict()["max(bmi)"])   
            while True:
                print("The BMI ranges from (<18.5), (>18.5,<=24.9), (>=25.0,<=29.9), (>=30.0,<=53.13)")
                bmi=float(input("Enter the bmi you want to analyse : "))
                n=int(input("enter number of rows you want to view : "))
                if (bmi<18.5):
                    df.select("sex","age","bmi").filter(df['bmi']<18.5).alias("Under Weight").show(n)
                    print("Number of people Who are UNDERWEIGHTED ",df.filter(df['bmi']<18.5).alias("Under Weighted").count())
                    print("The Types of Diseases which may be faced by under weight people are : ")
                    print("1. Malnutrition\n2. Osteoporosis\n3. Decreased Muscle Strength\n4. Hypothermia and lowered immunity")
                elif ((bmi>=18.5) & (bmi<=24.9)):
                    df.select("sex","age","bmi").filter((df['bmi']>=18.5) & (df['bmi']<=24.9)).alias("Normal BMI").show(n)
                    print("Number of people Who have Normal BMI ",df.filter((df['bmi']>=18.5) & (df['bmi']<=24.9)).alias("Normal BMI").count())
                    print("They won't face much health issues as there BMI is as per their Height And Weight ")
                elif ((bmi>=25.0) & (bmi<=29.9)):
                    df.select("sex","age","bmi").filter((df['bmi']>=25.0) & (df['bmi']<=29.9)).alias("Over Weight").show(n)
                    print("Number of people Who are Over Weight ",df.filter((df['bmi']>=25.0) & (df['bmi']<=29.9)).alias("Over Weighted").count())
                    print("The Types of Diseases which may be faced by Over Weight people are : ")
                    print("1. Heart Disease\n2. High Blood Pressure\n3. Type 2 Diabetes\n4. Allstones\n5. Breathing problems\n6. Certain cancers")
                elif ((bmi>=30.0) & (bmi<=53.13)):
                    df.select("sex","age","bmi").filter(df['bmi']>=30.0).alias("Obesity").show(n)
                    print("Number of people Who are Obesity ",df.filter(df['bmi']>=30.0).alias("Obesity").count())
                    print("The Types of Diseases which may be faced by Obesity people are : ")
                    print("1. Cardiovascular Disease (mainly heart disease and stroke)\n2. Type 2 Diabetes\n3. Musculoskeletal Disorders like Osteoarthritis\n4. Some cancers(Endometrial, Breast and Colon)")        
                else:
                    print("Provide BMI Value which is available in the dataset for Analysis ! Provide Greater Than 53.13")
                    break
        def southwestinfo():
            str=int(input('''1.Enter 1 for number of SouthWest and SouthEast \n2.Enter 2 for number of NorthEast and NorthWest \nPlease Enter your choice'''))
            if str==1:
                df.select("age","sex","bmi","region").filter((df["region"]=="southwest") | (df["region"]=="southeast")).alias("South").show()
                print("Number of South People",df.select("age","sex","bmi","region").filter((df["region"]=="southwest") | (df["region"]=="southeast")).alias("South side Count").count())
              
            elif str==2:
                df.select("age","sex","bmi","region").filter(((df["region"]=="northwest")) | (df["region"]=="northeast")).alias("North").show()
                print("Number of North People",df.select("age","sex","bmi","region").filter((df["region"]=="northwest") | (df["region"]=="northeast")).alias("North side Count").count())

            else:
                print("Enter valid option")
                southwestinfo() 
        def aggeragateOfRegion():
            print("""
                    1.for calculating maximum value of charges
                    2.for calculating minimum value of charges
                    3.for calculating avarage of charges
                    4.for calculating avarage of age
                    5.for calculating mean of charges and age""")
            kj=int(input("choose which function u want:- "))
            if kj==1:
                df.groupBy("region").agg(count("bmi"),count("sex"),max("charges")).show()
            elif kj==2:
                df.groupBy("region").agg(count("bmi"),count("sex"),min("charges")).show()
            elif kj==3:
                df.groupBy("region").agg(count("bmi"),count("sex"),avg("charges")).show()
            elif kj==5:
                df.groupBy("region").agg(count("bmi"),count("sex"),avg("age")).show()
            elif kj==4:
                df.groupBy("region").agg(count("bmi"),count("sex"),mean("charges"),mean("age")).show()
            else:
                print("choose valid option")
        def visualization():
            print("""
                    1.visualization of smokers vs charges in graph
                    2.visualizatiof of children vs charges in graph
                    3.visualization of sex vs charges in graph
                    4.visualization of age vs charges in graph
                    5.Visualization of age vs bmi in graph""")
            red=int(input("choose which plot u want see :- "))
            if(red==1):
                plt.figure(figsize = (8, 8))
                sb.barplot(x = 'smoker', y = 'charges', data = pandas_df)
                plt.title("smoker vs Charges")
            elif(red==2):
                plt.figure(figsize = (6, 6))
                sb.barplot(x = 'children', y = 'charges', data = pandas_df)
                plt.title("children vs Charges")
            elif(red==3):
                plt.figure(figsize = (3, 6))
                sb.barplot(x = 'sex', y = 'charges', data = pandas_df)
                plt.title('sex vs charges')
            elif(red==4):
                plt.figure(figsize = (5, 8))
                sb.barplot(x = 'age', y = 'charges', data = pandas_df)
                plt.title("Age vs Charges")
            elif(red==5):
                df.select("age","bmi").toPandas().plot.hist()
                plt.savefig("all.png")
            else:
                print("choose valid option")
        def quit():
            k=input("do you want log out enter ok..or enter notok :-")
            if k=='ok':
                print("Succesfully existed/ logout successfully")
            else:
                print("Run again ")
        def Menuset():
            print("enter 1  : To Show data")
            print("enter 2  : To view selected data")
            print("enter 3  : to view counting of data ")
            print("enter 4  : for viewing data in Age and gender graph ")
            print("enter 5  : for specificDetailsOfAgeSelection ")
            print("enter 6  : for sorting of data ")
            print("enter 7  : for Male AdultSmoker Information")
            print("enter 8  : for Male Older AdultSmoker Information")
            print("enter 9  : for feMale AdultSmoker Information")
            print("enter 10 : for feMale Older AdultSmoker Information")
            print("enter 11 : for Smoker information ")
            print("enter 12 : for BMI Analysis")
            print("enter 13 : for Region wise information..")
            print("enter 14 : To viewing Avarage data in Charges")
            print("enter 15 : To viewing visualization of data")
            print("enter 16 : for exit:")
            try:
                userinput = int(input("please select an above option:"))
            except ValueError:
                exit("\n hi thats not a number")

            #userinput = int(input("enter your choice"))
            if (userinput == 1):
                datasetInfo()
            elif (userinput == 2):
                userchoiceToSelectRow()
            elif (userinput == 3):
                availabledatacount()
            elif (userinput == 4):
                AgeandSexGraph()
            elif (userinput == 5):
                specificDetailsOfAgeSelection()
            elif (userinput == 6):
                sorting()
            elif (userinput == 7):
                MaleAdultSmokerInfo()
            elif (userinput == 8):
                MaleOlderAdultSmokerInfo()
            elif (userinput == 9):
                FemaleAdultSmokerInfo()
            elif (userinput == 10):
                FemaleOlderAdultSmokerInfo()
            elif (userinput == 11):
                GroupBySmoker()
            elif (userinput == 12):
                BMIAnalysis()
            elif (userinput==13):
                southwestinfo()
            elif (userinput == 14):
                aggeragateOfRegion()
            elif (userinput==15):
                visualization()
            elif (userinput == 16):
                quit()
            else:
                print("enter correct choice")
        Menuset()


        def runagain():
            runagn = input("\n want to run again y/n:")
            while (runagn.lower() == 'y'):
                if (platform.system() == "windows"):
                    print(os.system('cls'))
                else:
                    print(os.system('clear'))
                Menuset()
        
        runagain()
    else:
        print( " --::--Login was Failed....Try Again..--::-- " )


# In[ ]:





# In[ ]:




