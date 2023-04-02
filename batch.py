import os

from datetime import datetime
from h2ox.reducer.slackbot import SlackMessenger

from main import main_loop

if __name__=="__main__":
    
    token=os.environ.get("SLACKBOT_TOKEN")
    target=os.environ.get("SLACKBOT_TARGET")
    
    if token is not None and target is not None:

        slackmessenger = SlackMessenger(
            token=token,
            target=target,
            name="h2ox-reducer",
        )
    else:
        slackmessenger=None
        
    main_loop(today=datetime.now(), slackmessenger=slackmessenger)