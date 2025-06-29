is_paper_trading = True   #모의투자 여부: False 또는 True
api_key= "m_juBirUMUqLahKaDN5sQNFZoHZwSrEGoWtoXKLjNGI"  #API KEY
api_secret_key = "4SP13Pk6SWnyo4NXp2TJlqdHZmRdCuJYNPkJ_krgbd4"  # API SECRET KEY

host = "https://mockapi.kiwoom.com" if is_paper_trading else "https://api.kiwoom.com"
websocket_url = "wss://mockapi.kiwoom.com:10000/api/dostk/websocket" if is_paper_trading else "wss://api.kiwoom.com:10000/api/dostk/websocket"
