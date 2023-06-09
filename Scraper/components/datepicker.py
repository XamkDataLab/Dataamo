from datetime import datetime, timedelta, date
import re

def month():
    currDate = date.today()
    startingDate = datetime(currDate.year, currDate.month, 1)
    #28 days in any month, but there isn't 32 in any. Returns next month date
    next_month = currDate.replace(day=28) + timedelta(days=4)
    return {"start": startingDate.strftime("%d.%m.%Y"), "end": (next_month - timedelta(days=next_month.day)).strftime("%d.%m.%Y")}

def week():
    #Get this day
    currDate = date.today()
    #Get the starting and ending date of the week
    startDay = currDate - timedelta(days=currDate.weekday())
    endDay = startDay + timedelta(days = 6)
    #strftime to return the date in format of DD.MM.YYYY instead of datetime object
    return {"start": startDay.strftime("%d.%m.%Y"), "end": endDay.strftime("%d.%m.%Y")}

def customDate():
    reg_pattern = re.compile(r'^(0[1-9]|[12][0-9]|3[01]).(0[1-9]|1[012]).(19|20)\d\d$')

    dates = {}

    for i in range(1,3):
        while True:
            pvm_input = input(f'''
Syötä {"alkamispäivämäärä" if i == 1 else "loppumispäivämäärä"} muodossa DD.MM.YYYY:
''')
            if reg_pattern.match(pvm_input):
                dates["start" if i == 1 else "end"] = pvm_input
                break
            else:
                print("\nVirheellinen syöte. \nSyötä päivämäärä muodossa DD.MM.YYYY - Esim: 12.07.2023")
    return dates

def datepicker():
    while True:
        try:
            option = int(input('''
Valitse aikaväli josta dataa haetaan:
    1. Tämä viikko
    2. Tämä kuukausi
    3. Oma valinta
    4. Lopeta
'''))
            match option:
                case 1:
                    return week()
                case 2:
                    return month()
                case 3:
                    return customDate()
                case 4:
                    quit()
        except ValueError:
            print("\nValitse numeroina 1-4.")
        else:
            if option > 3 or option < 1:
                print("\nVäärä valinta, valitse 1-4.")