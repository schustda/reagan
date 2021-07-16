from reagan.subclass import Subclass
import requests
from bs4 import BeautifulSoup
from textblob import TextBlob
import pandas as pd


class Ihub(Subclass):
    
    def __init__(self):
        self.headers = {
            "User-Agent": 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
        }

    def page_response(self, message_id, timeout = 60):
        '''
        Use the requests and beautifulsoup modules to make the request to the website
        '''
        payload = {'message_id' : message_id}
        url = f"https://investorshub.advfn.com/boards/read_msg.aspx"
        r = requests.get(url=url, timeout=timeout, headers=self.headers, params = payload)
        return BeautifulSoup(r.content, "lxml")

    def get_message_data(self, message_id):
        """ Web scrapes post information from the specified message_id

        Parameters: message_id (int): The message number to extract data from
        """

        site_id_codes = {
            "active": { 
                # This is where the data from the post will originate from
                'message' : "ctl00_CP1_mbdy_dv"
            
                # Where to extract the post number for the specific board the message is on
                ,'post_number' : "ctl00_CP1_mh1_tbPost"
            
                # Finds the date the message was posted
                ,'message_date' : "ctl00_CP1_mh1_lblDate"
            
                # extracting the code of the specific ticker or board
                ,'ihub_code' : "ctl00_CP1_bbc1_hlBoard"
            }
            ,"error": { 
                # If this id has any data, it means the post is erroneous
                'missing_post' : "ctl00_CP1_L1"

                # If a message was deleted, the message will appear here
                ,'deleted_post' : 'ctl00_CP1_na'
            }
        }

        try:
            soup = self.page_response(message_id)
        except requests.exceptions.Timeout:
            return {}

        output = {'message_id': message_id, 'status': 'Active'}

        for id_type, id_code in site_id_codes['error'].items():
            data = soup.find(id=id_code)
            if data:
                if id_type == 'missing_post':
                    output['status'] = 'Error'
                    output['error_message'] = data.text
                    return output
                if id_type == 'deleted_post':
                    output['status'] = 'Error'
                    output['error_message'] = data.text
                    return output

        for id_type, id_code in site_id_codes['active'].items():
            data = soup.find(id=id_code)
            if data:
                if id_type == 'message':
                    body = data.text
                    tb = TextBlob(body)
                    output['sentiment_polarity'], output['sentiment_subjectivity'] = tb.sentiment
                if id_type == 'post_number':
                    output['post_number'] = int(data.get('value',0))
                if id_type == 'message_date':
                    output['message_date'] = pd.to_datetime(data.text)
                if id_type == 'ihub_code':
                    output['ihub_code'] = data["href"]
        return {k:v.strip().replace('/','').replace('%','') if type(v) == str else v for k,v in output.items()}


if __name__ == "__main__":
    pass
    # deleted post
    # message_id = 144689607
    # good post
    # message_id = 144689616
    # non-existant-post
    # message_id=254689616
    # other board
    # message_id = 144802665
    # message_id = 77864629
    message_id = 159246431

    s = Ihub()
    # z = s.page_response(message_id)
    # id_code = 'ctl00_CP1_bbc1_hlBoard'
    # data = z.find(id=id_code)
    # text = data['href']
    i = s.get_message_data(message_id)