from migames.pakquiz.table_type.pakquiz_questions import trivia
from migames.pakquiz.table_type.pakquiz_events import event
from datetime import timedelta, datetime

yesterday = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
trivia(yesterday)
event(yesterday)

