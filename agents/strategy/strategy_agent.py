class StrategyAgent:
    def __init__(self,settings,kc):
        self.settings = settings
        self.kc = kc
        # Additional initialization code here
        self.tickers=[]
        self.redis_toConsume=settings['streams']['one_min_candle_data']
        self.redis_toPublish=settings['streams']['trade_signal']
    
    # subscribe to toConsume stream and upon receiving data, start the strategy agent - call the run() function
    def start(self):
        # Code to start the strategy agent
        pass
    
    def run(self):
        # Main logic for the strategy agent
        pass
    
    # get all the ORB ranges from database
    def get_orb_ranges(self):
        pass
    
    # check for breakout
    def check_breakout(self):
        pass
    
    # check for fvg confluence after breakout
    def check_fvg_confluence(self):
        pass
    
    # generate buy/sell signals
    def generate_signals(self):
        pass
    
    