# import datetime
# import iso8601

end = u'2006-12-31t13:00:00.000z'

# from dateutil.parser import parse

#print(iso8601.parse_date(end))

# get_date_obj = parse(end)
# print get_date_obj


from datetime import datetime

t = datetime.strptime(end, "%Y-%m-%dt%H:%M:%S.%fz")
print(t)