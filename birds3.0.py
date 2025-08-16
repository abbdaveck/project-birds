# -*- coding: utf-8 -*-

"""
Group 3 - 4
"""
import pandas as pd
from datetime import datetime, timedelta
import pytz, os
from collections import defaultdict
import matplotlib.pyplot as plt
from astral.sun import sun
from astral import LocationInfo


# Skriver filen till en lista, gör i slutet det till en data frame
def read_data_as_list(file_path, tz_local="Europe/Stockholm"):
    """
    Läser näst data från fil och konverterar det till en lista med rätt tidszon,
    retur lista 

    :param file_path: placeringen av filen i fil systemet
    :type file_path: str
    :param tz_local: den tidszonen som man vill konvertera till, def stockholm
    :type tz_local: str
    """
    rows = []
    tz = pytz.timezone(tz_local)

    with open(file_path, "r") as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) < 3:
                continue
            try:
                dt = datetime.strptime(parts[0] + " " + parts[1], "%Y-%m-%d %H:%M:%S.%f")
                dt = pytz.UTC.localize(dt).astimezone(tz)  # konverterar tidszonen
                count = int(parts[2])
                rows.append((dt, count))
                
                #hoppar vid fel
            except Exception: 
                continue

    return rows


# Fixa dåliga värden 
def fix_incomplete_counts_list(data):
    """
    Fixar värden i listan som inte stämmer med föregående samt efterkommande värde
    retur lista

    :param data: lista med datan som ska korriigeras
    :type data: list
    """
    corrected = [data[0]] # lägger till det första värdet då är första referenspunkten 70

    for i in range(1, len(data) - 1):
        prev_dt, prev_c = corrected[-1]
        curr_dt, curr_c = data[i]
        next_dt, next_c = data[i + 1]

        # Kolla om current är lägre än både före och efter
        if curr_c < prev_c and curr_c < next_c:
            # ersätt med föregående värde
            corrected.append((curr_dt, prev_c))
        else:
            corrected.append((curr_dt, curr_c))

    # Lägg till sista raden utan att kolla den, bör ändras då det inte är säkert att detta värdet stämmer
    corrected.append(data[-1])
    return corrected



# missad tid (hade kunnat kasnke använda ffill från dataframes)
def fill_missing_data_list(data, freq_minutes=2.1):
    """
    Läser näst data från lista och kollar om det finns missad data i intervallet +2.1 min frammåt

    :param data: lista med datan som ska korrigeras
    :type data: str
    :param freq_minutes: max tiden mellan två mätningar, def 2.1 min
    :type freq_minutes: float
    """
    filled = []
    
    for i in range(len(data) - 1):
        dt, count = data[i] # (tid, 20 count)
        filled.append((dt, count))
        next_dt, _ = data[i + 1]

        
        while dt + timedelta(minutes=freq_minutes) < next_dt:
            dt = dt + timedelta(minutes=freq_minutes)
            filled.append((dt, count))

    filled.append(data[-1])  
    return filled


#begränsar rörelsen

def limit_bird_movements_list(data, max_per_minute=8):
    """
    Läser näst data från lista och kollar om antalet passeringar har ändrat sig mer än +8 från föregående tillfälle

    :param data: lista med datan som ska korriigeras
    :type data: str
    :param max_per_minutes: max antalet inpasseringa som får lov att göras def 4
    :type max_per_minute: int
    """
    
    limited = []
    
    prev_count = data[0][1] # hela listan men vi väljer nu specifict första raden andra elementet = count 
    running_total = prev_count

    for i, (dt, count) in enumerate(data):
        if i == 0:
            limited.append((dt, count))
            continue

        delta = count - prev_count   #71 - 70 = 1 
        
        delta = max(0, min(delta, max_per_minute)) # max av ( 0 eller minsta av ( 1 eller 4) ) 
        
        running_total += delta # föregående värde = föregående värde + delta  "" nuvarande plats = 70 + 1
        limited.append((dt, running_total))
        prev_count = count

    return limited

def preprocess_bird_data_list(file_path):
    """
    Läser in näst data från fil och konverterar det till en lista med rätt tidszon, samt gör korigeringar
    vad gällande felaktig räkning, missad data, max antal rörelser samt konverterar allt till en dataframe

    :param file_path: placeringen av filen i fil systemet
    :type file_path: str
    
    :return: Behandlad dataframe
    :rtype: dataframe
    """
    
    data = read_data_as_list(file_path)  # hämtar info från birds_data, sparar i "data"
    data = fix_incomplete_counts_list(data) # skickar in "data" fr förra raden, i 2a funktionen där den rättas till, och sparar resultatet i "data". förvirring *
    df = pd.DataFrame(data, columns=['timestamp', 'value'])# konverterar till data frame #df blir en tabell (dataframe)
    #Använder ej limit_bird_movements_list() då jag begränsar antalet värden per 2 miuter senare i koden
    return df

def filter_data(df, start_date, days):
    '''
    Filtrerar datan och tar bort de värderna innan startdatumet och efter tidsintervallet är slut
    :param df: Den behandlade datan från textfilen
    :type df: dataframe
    :param start_date: Startdatumet
    :type start_date: datetime
    :param days: Antal dagar som ska visas
    :type days: int
    
    :return: Filtrerad dataframe
    :rtype: dataframe
    '''
    end_date = start_date + timedelta(days=days)
    return df[(df["timestamp"] >= start_date) & (df["timestamp"] < end_date)]

def group_values(df, interval):
    '''
    Retunerar en dictionary som delar upp datan i intervall och summerar alla värden under det intervallet.
    Nycklarna är tider och värderna är summan under intervallet.
    :param df: Den behandlade datan från textfilen
    :type df: dataframe
    :param interval: Val av interval (timme, dag eller vecka)
    :type interval: str
    
    :return: Dictionary med intervallet
    :rtype: dict[datetime, int]
    '''

    summed_values = defaultdict(int)                            #Skapar ett dictionary där värdena under varje intervall summeras 

    if interval == "h":
        for row in df.itertuples():
            dt = row.timestamp
            value = row.movements
            hour_key = dt.replace(minute=0, second=0, microsecond=0)
            summed_values[hour_key] += value                      #Använder timmen som en nyckel
        bar_width = 0.03
    elif interval == "d":
        for row in df.itertuples():
            dt = row.timestamp
            value = row.movements
            day_key = dt.replace(hour=0, minute=0, second=0, microsecond=0)
            summed_values[day_key] += value                       #Avänder datumet som en nyckel
        bar_width = 0.6
    elif interval == "w":
        for row in df.itertuples():
            dt = row.timestamp
            value = row.movements
            week_start = dt - timedelta(days=dt.weekday())          #Jag sätter week start till måndag varje gång
            week_key = week_start.replace(hour=0, minute=0, second=0, microsecond=0)
            summed_values[week_key] += value                          #Använder måndagen i veckan som en nyckel
        bar_width = 3.0

    return summed_values, bar_width

def get_labels(x_list, interval):
    '''
    Hämtar namnen som ska sättas till markörerna på x-axeln
    :param x_list: Lista med värderna för x-axeln
    :type x_list: list[datetime]
    :param interval: Val av interval (timme, dag eller vecka)
    :type interval: str
    
    :return: Lista med namnet till markörerna på x-axeln
    :rtype: list[string]
    '''

    x_labels = []
    for dt in x_list:                                         
            if interval == "h":                                 #För tim-intervall ska både datumen och timmar vara med
                if dt.hour == 1:
                    label = dt.strftime("%Y-%m-%d             ")
                elif dt.hour % 2 == 0:
                    label = dt.strftime("%H:%M")
                else:
                    label = ""
            elif interval == "d":                               #Bara datum visas
                label = dt.strftime("%Y-%m-%d")
            elif interval == "w":
                label = "Vecka nr. " + dt.strftime("%W")
            x_labels.append(label)
    return x_labels

def set_sun_rise(dt_list, axis):
    '''
    Färgar grafen gul under de timmar solen är uppe
    :param x_list: Lista med tidsobjekten för x-axeln
    :type x_list: list[datetime]
    :param axis: Axes-objekt där grafen ritas
    :type axis: matplotlib.axes._axes.Axes
    '''

    tz_local="Europe/Stockholm"
    tz = pytz.timezone(tz_local)
    city = LocationInfo("Södra Sandby", "Sweden", "Europe/Stockholm", 55.717813, 13.347003)

    first_dt = min(dt_list)
    last_dt = max(dt_list)

    for dt in dt_list:
        date = dt.date()
        s = sun(city.observer, date=date, tzinfo=tz)
        sunrise = s['sunrise'].replace(minute=0, second=0, microsecond=0)
        sunset = s['sunset'].replace(minute=0, second=0, microsecond=0)

        day_start = datetime.combine(date, datetime.min.time()).replace(tzinfo=tz)
        day_end = datetime.combine(date, datetime.max.time()).replace(hour=23, minute=59, second=59, microsecond=999999, tzinfo=tz)
        
        a = datetime.combine(date, datetime.min.time())
        b = datetime.combine(date, datetime.max.time())

        if day_start < last_dt and day_end > first_dt:
            axis.axvspan(max(sunrise, first_dt), min(sunset, last_dt), facecolor='lightyellow', alpha=0.3)

def plot_values(df, startDate, timeSpan, interval):
    """
    Ritar ut datan i grafen

    :param txtfile: Txt-fil med namnet på filen 
    :type txtfile: string
    :param startDate: Första dagen ska visas i grafen
    :type startDate: datetime
    :param timeSpan: Hur många dagar som ska visas i grafen
    :type timeSpan: int
    :param interval: Vad av typ av intervall (timme, dag eller vecka)
    :type interval: string
    """

    #df = preprocess_bird_data_list(txt_file)
    shortList = filter_data(df, startDate, timeSpan)
    shortList["movements"] = shortList["value"].diff().clip(lower=0, upper=8)               #Räknar ut differansen mellan värdet innan samt sorterar bort negativa och för stora värden.

    group, bar_width = group_values(shortList, interval)                                    #Delar upp datan i intervaller, hämtar också storlekten på staplarna

    result = list(group.items())                                                            #Sätter nycklarna (tidsobjekten) till x-axeln och summorna till y-axeln
    x_values = []
    y_values = []
    for i in result:
        x_values.append(i[0])
        y_values.append(i[1])
    
    x_labels = get_labels(x_values, interval)                                               #Hämtar en lista med namen för markörerna på x-axeln
    fig, ax = plt.subplots(figsize=(12, 6))

    if interval == "h":                                                                     #Färgar bakgrunden om timmar ska visas
        set_sun_rise(x_values, ax)

    plt.bar(x_values, y_values, width=bar_width)
    plt.ylabel('Antal in och utgångar')
    plt.grid(True)
    plt.xticks(ticks=x_values, labels=x_labels, rotation=90)
    plt.tight_layout()
    plt.show()

def get_user_selection(df):
    """
    Hämtar information från användaren via terminalen
    
    :return: Startdatumet
    :rtype: datetime
    :return: Tidsspannet
    :rtype: int
    :return: Intervallet (timme, dag eller vecka)
    :rtype: string
    """

    os.system('cls' ) # Clear the terminal screen
    print("Welcome, this program will plot the bird movements (in/out) in a garage in Södra Sandby")
    print("Please input the start date (from 2015-01-25 to 2016-01-16)")

    tz = pytz.timezone("Europe/Stockholm")    
    
    while True:
        year = input("Year: ")
        month = input("Month (number): ")
        day = input("Day: ")
        try:
            date = datetime.strptime(str(year + "-" + month + "-" + day + " 00:00:00.000000"), "%Y-%m-%d %H:%M:%S.%f")
            date = pytz.UTC.localize(date).astimezone(tz)
            if date.date() >= df["timestamp"].min().date() and date.date() <= df["timestamp"].max().date():
                break
            else:
                print("The date is not in the data. Please try again. Data from 2015-01-25 to 2016-01-16.")
        except ValueError:
            print("Invalid date. Please try again. Year(XXXX), month(XX) and day(XX) must be valid numbers/date.")


    print("What type of interval would you like? (Write 'h' for hours, 'd' for days, 'w' for weeks)")
    while True:
        interval = input("Interval: ")
        if interval in ["h", "d", "w"]:
            break
        else:
            print("Invalid interval. Please enter 'h' for hours, 'd' for days, or 'w' for weeks.")

    while True:    
        timespan = input("How many days do you want to display? ")
        try:
            timespan = int(timespan)
            if timespan > 0:
                break
            else:
                print("Please enter a positive integer for the number of days.")
        except ValueError:
            print("Invalid input. Please enter a positive integer")

    return date, timespan, interval

tz = pytz.timezone("Europe/Stockholm")
testDate1 = datetime.strptime("2015" + "-" + "4" + "-" + "1" + " 00:00:00.000000", "%Y-%m-%d %H:%M:%S.%f")
testDate1 = pytz.UTC.localize(testDate1).astimezone(tz)
testTimeSpan1 = 6
testInterval1 = "h"
# plot_values("bird_data.txt", testDate1, testTimeSpan1, testInterval1)

df = preprocess_bird_data_list("bird_data.txt")
start_date, timespan, interval = get_user_selection(df)
plot_values(df, start_date, timespan, interval)



