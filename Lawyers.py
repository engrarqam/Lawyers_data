from formatter import test
from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
import time
import asyncio

start_time = time.time()

async def Link():
    for page in range(2, 302):
        driver = webdriver.Chrome()

        driver.get(f'https://www.californiatoplawyers.com/lawyer/search?section=Worker%27s+Compensation&page={page}')

        links = driver.find_elements(By.XPATH, '//table[@class="table table-striped"]//td//a')
        Cities = driver.find_elements(By.XPATH, '//table[@class="table table-striped"]//tbody//tr//td[3]')

        print('links**************', len(links))
        for l, c in zip(links, Cities):
            link = l.get_attribute('href')
            Name = l.text
            City = c.text
            # await asyncio.sleep(2)
            print(link)
            # asyncio.create_task(BASF(link))

            await asyncio.create_task(BASF(link, City, Name, page))

        driver.close()

async def BASF(link, City, Name, page):
    driver = webdriver.Chrome()

    driver.get(link)
    
    await asyncio.sleep(2)

    try:
        Firm_name = driver.find_element(By.XPATH, '//dt[text()="Firm / org"]/following-sibling::dd//a').text
    except:
        Firm_name = ''

    await asyncio.sleep(2)

    try:
        Full_Name = Name

        First_Name = Full_Name.split(", ")[0]
        First_Name

        Last_Name = Full_Name.split(", ")[1]
        Last_Name

    except:
        Full_Name = ''
        First_Name = ''
        Last_Name = ''
        

    await asyncio.sleep(2)

    try:
        Email = driver.find_element(By.XPATH, '//dt[text()="Email"]/following-sibling::dd//a').text
    except:
        Email = ''

    await asyncio.sleep(2)

    try:
        Phone = driver.find_element(By.XPATH, '//dt[text()="Phone"]/following-sibling::dd//a').text
    except:
        Phone = ''

    await asyncio.sleep(2)

    try:
        Full_Address = driver.find_element(By.XPATH, '//dt[text()="Address"]/following-sibling::dd').text

        Full_Address = Full_Address.replace(f'{City}', '')

        Street_Address = Full_Address.split(", ")[0].split(" ")[:-1]
        Street_Address = ' '.join(Street_Address)
        Street_Address

        State = Full_Address.split(", ")[1].split(" ")[0]
        State

        Zip = Full_Address.split(", ")[1].split(" ")[1]
        Zip

    except:
        Full_Address = ''
        Street_Address = ''
        City = ''
        State = ''
        Zip = ''

    await asyncio.sleep(2)

    try:
        County = driver.find_element(By.XPATH, '//dt[text()="County"]/following-sibling::dd').text
    except:
        County = ''

    await asyncio.sleep(2)  

    try:
        Status = driver.find_element(By.XPATH, '//dt[text()="Status*"]/following-sibling::dd').text
    except:
        Status = ''

    try:
        Effective_status_date = driver.find_element(By.XPATH, '//table[@class="table panel-body"]//tbody//td[contains(text(),"/")]').text
    except:
        Effective_status_date = ''

    try:
        Bar_number = driver.find_element(By.XPATH, '//dt[text()="Bar number"]/following-sibling::dd').text
    except:
        Bar_number = ''

    try:
        Classification_section = driver.find_element(By.XPATH, '//dt[text()="Sections"]/following-sibling::dd').text
    except:
        Classification_section = ''

    await asyncio.sleep(2)
    
    
    

    print('Firm_name==', Firm_name)
    # print(Full_Name)
    # print(Email)
    # print(Phone)
    # print(Full_Address)
    # print(County)
    # print(Status)
    # print(Bar_number)
    # print(Classification_section)
   

    data = pd.DataFrame({
    'Firm Name': [Firm_name], 
    'First Name': [First_Name],
    'Last Name': [Last_Name],
    'Email': [Email],
    'Phone': [Phone],
    'Street_Address': [Street_Address],
    'City': [City],
    'State': [State],
    'Zip': [Zip],
    'County': [County],
    'Status': [Status],
    'Effective_status_date': [Effective_status_date],
    'Bar_number': [Bar_number],
    'Classification_section': [Classification_section]
        })

    print(data)
    await asyncio.sleep(2)

    lawyer = pd.read_csv(f'lawyer.csv')

    await asyncio.sleep(2)

    data.to_csv(f'{page}_{Full_Name}.csv', index=False)

    await asyncio.sleep(2)

    New = pd.read_csv(f'{page}_{Full_Name}.csv')
    
    result = pd.concat([lawyer, New], axis=0)
    print(result)

    await asyncio.sleep(2)

    result.to_csv('test.csv', index=False)

    await asyncio.sleep(2)
        

asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
asyncio.run(Link())

print("--- %s seconds ---" % (time.time() - start_time))
