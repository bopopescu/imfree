import json


def json_categ(jsonfile):
    game = {}

    with open(jsonfile, encoding='utf8') as json_file:
        data = json.load(json_file)
        game['gamecode'] = data['gamecode']
        categ = data['categories']
        for i, d in enumerate(categ):
            d = str(d).replace("\'", "\'\'")
            game[d] = i

    return game


def json_qna(jsonfile, index):
    game = {}
    question = {}
    correct_answer = {}
    game[0] = 'questions_1'
    game[1] = 'questions_2'
    game[2] = 'questions_3'

    with open(jsonfile, encoding='utf-8') as json_file:
        data = json.load(json_file)

        for i in range(1, 11):
            counter = i - 1
            question[i] = str(data[game[index]][counter]['question']).strip()
            correct_answer[i] = str(data[game[index]][counter]['correct_answer']).strip()
        
    return game, correct_answer, question
