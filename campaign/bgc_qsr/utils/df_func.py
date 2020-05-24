def qsr_valid_user_details(df):
    user = {}

    for k, v in df.iterrows():
        first_name = v['first_name'] if v['first_name'] is not None else 'NULL'
        version = v['app_user_version'] if v['app_user_version'] is not None else 'NULL'
        city = v['city'] if v['city'] is not None else 'NULL'
        mobile_number = v['mobile_number'] if v['mobile_number'] is not None else 'NULL'
        val = first_name + '|' + str(version) + '|' + city + '|' + str(mobile_number)
        user[v['app_user_id']] = val

    return user


def qsr_excel_to_dict(df, valid_answer, excel_col, key, value):
    user = {}
    for k, v in df.items():
        user_ans = str(v[excel_col]).lower()
        user_ans = user_ans.strip()
        if user_ans == valid_answer.lower():
            user[v[key]] = value

    return user
