"""
Algorithm for 'PDF_drive scrapping by searching for a key word':
    (1) Accept an input from user which must be a string. # getting key word.
    (2) Send a request to PDF_drive server to get search result first page. # getting pages.
        (2.1) get the last page number.
        (2.2) get a list of pages depending on the last page number.
    (3) scrap all pages in parallel and when a page finish store its data in CSV file.
"""
########################################################################################

#imporing required libraries for the program.
import asyncio,requests,aiohttp,bs4,csv,os

#book queue
queue=[]

#define a function accepting the key word from user
def accept_as_string()->str:
    keyword=input("Please, enter the search word: ")
    return keyword

#defining a trial wrapper
def trial(func):
    def wrapper(*args,**kwargs):
        try:
            return func(*args,**kwargs)
        except Exception as e:
            print(str(e))
            return None
    return wrapper


#define a function getting first page in search result
@trial
def get_search_result(url:str)->bs4.element.Tag:
    #getting site contents if request is successful
    site=requests.get(url).content if requests.get(url).status_code==200 else None
    if site:
        return bs4.BeautifulSoup(site,"html.parser")
    else:
        return None


#define a function to get list of urls 
def get_urls(first_page:bs4.element.Tag,keyword:str)->list:
    soup=first_page
    last_page=soup.select("div[class='Zebra_Pagination']")[0].select("a")[-2]
    search='%20'.join(keyword.split(' '))
    return [f"https://www.pdfdrive.com/search?q={search}&pagecount=&pubyear=&searchin=&page={page}" for page in range(1,int(last_page.text)+1)]

#define asynchronous scraper
@trial
async def scraper(url:str)->bs4.element.Tag:
    async with aiohttp.client.ClientSession() as session:
        async with session.get(url) as response:
            return bs4.BeautifulSoup(await response.text(),"html.parser")
        
#def a function add CSV
def add_to_csv():
    global queue
    headers=list(queue[0].keys())
    file_name='books.csv'
    with open(file_name,'a',encoding='utf-8',newline='') as file:
        if os.path.getsize(file_name)==0:
            writer=csv.DictWriter(file,fieldnames=headers)
            writer.writeheader()
        writer=csv.DictWriter(file,fieldnames=headers)
        writer.writerows(queue)
        queue=[]

#def a function to get data and store it ti csv 5 books at a time
def add_data(book:bs4.element.Tag, queue_limit=5):
    global queue
    queue.append(
        {
            "title":book.select("h2")[0].text if book.select("h2") else "none",
            "url":f"https://www.pdfdrive.com/{book.select('a')[0]['href']}" if book.select("a") else "none",
            "pages":book.select("span[class='fi-pagecount']")[0].text if book.select("span[class='fi-pagecount']") else "none",
            "year":book.select("span[class='fi-year']")[0].text if book.select("span[class='fi-year']") else "none",
            "size":book.select("span[class='fi-size']")[0].text if book.select("span[class='fi-size']") else "none"
        }
    )
    if len(queue)==queue_limit:
        #adding to csv function
        add_to_csv()
    

#define main function for operation
async def main():
    global queue
    #get keyword
    keyword=accept_as_string()
    #get first search result
    first_page=get_search_result(
        url=f"https://www.pdfdrive.com/{'-'.join(f"{keyword} books".split(' '))}.html"
        )
    if first_page:
        #getting list of urls
        urls=get_urls(first_page=first_page,keyword=keyword)
        result= await asyncio.gather(*[scraper(url) for url in urls])
        for page in result:
            books=page.select("div[class='row']")
            for book in books:
                add_data(book)
        # adding books in queue if existed
        if queue:
            add_to_csv()
    else:
        raise "request process wasn't successful."
    return

#running the program
if __name__=="__main__":
    asyncio.run(main())
