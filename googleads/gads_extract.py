job = {'first': 123 , 'second':{'nested':123}}

job['second'].update({"nested":1234})
print(job)
print('ABCDEF')