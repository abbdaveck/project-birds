# -*- coding: utf-8 -*-

"""
Group 3 - 4
"""
import pandas as pd
from datetime import datetime, timedelta
import pytz

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



# missad tid
def fill_missing_data_list(data, freq_minutes=2.1):
    """
    Läser näst data från lista och kollar om det finns missad data i intervallet +2.1 min frammåt

    :param data: lista med datan som ska korriigeras
    :type data: str
    :param freq_minutes: max tiden mellan två mätningar, def 2.1 min
    :type freq_minutes: float
    """
    filled = []
    
    for i in range(len(data) - 1):
        dt, count = data[i] # (tid, 20 count)
        filled.append((dt, count))
        next_dt, _ = data[i + 1]

        # 
        while dt + timedelta(minutes=freq_minutes) < next_dt:
            dt = dt + timedelta(minutes=freq_minutes)
            filled.append((dt, count))  # 

    filled.append(data[-1])  
    return filled


#begränsar rörelsen

def limit_bird_movements_list(data, max_per_minute=8):
    """
    Läser näst data från lista och kollar om antalet passeringar har ändrat sig mer än +4 från föregående tillfälle

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


#kör allt

def preprocess_bird_data_list(file_path):
    """
    Läser in näst data från fil och konverterar det till en lista med rätt tidszon, samt gör korigeringar
    vad gällande felaktig räkning, missad data, max antal rörelser samt konverterar allt till en dataframe

    :param file_path: placeringen av filen i fil systemet
    :type file_path: str
    """
    
    data = read_data_as_list(file_path)
    data = fix_incomplete_counts_list(data)
    data = fill_missing_data_list(data)
    data = limit_bird_movements_list(data)
    df = pd.DataFrame(data, columns=['timestamp', 'value'])# konverterar till data frame 
    return df


# Example run
clean_frame = preprocess_bird_data_list("bird_data.txt")


#for i in range(24704, 24715):
#    print(clean_list.iloc[i])

#for i in range(0, 10):
#    print(clean_list.iloc[i])

#for row in clean_list[131220:131235]:
#    print(row)

