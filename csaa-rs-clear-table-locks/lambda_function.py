## Copy Remedy Data from Oracle to Redshift
# 
# First install:
# pip install psycopg2

import psycopg2
import boto3

sql_query_list = [
		"set query_group to 'superuser'; select distinct terminate_sql from intraday.clear_intraday_lock_v2;"
	, 	"set query_group to 'superuser'; select distinct terminate_sql from intraday.clear_long_curses_v;"
	, 	"set query_group to 'superuser'; select distinct terminate_sql from intraday.clear_long_connections_v;"
	]

def parameter_value(param_name):
    # reads a secure parameter from AWS' SSM service.
    ssm = boto3.client('ssm')
    response = ssm.get_parameters(
        Names=[param_name],
        WithDecryption=True
    )
    return response['Parameters'][0]['Value']

def connect_to_redshift(env_stub):
	# connect to redshift using environment variables
	# return the redshift database connection
	red_host = parameter_value(env_stub+'_host')
	red_db   = parameter_value(env_stub+'_db')
	red_user = parameter_value(env_stub+'_user')
	red_word = parameter_value(env_stub+'_word')
	# print(' red_host:'+red_host+'\n red_db:'+red_db+'\n red_user:'+red_user+'\n red_word:'+red_word)
	redshift = psycopg2.connect(host=red_host, port="5439", database=red_db, user=red_user, password=red_word, sslmode="require")
	return redshift

	
def sql_to_str(con, sql, col_end=' ', row_end='\n'):
	return_str = ""
	with con.cursor() as curs:
		curs.execute(sql)
		if (curs.description is not None):
			for row in curs:
				for cell in row:
					return_str += str(cell) + col_end
				return_str += row_end
	return return_str

def sql_to_list(con, sql, col_end=' '):
	return_list = []
	with con.cursor() as curs:
		curs.execute(sql)
		if (curs.description is not None):
			for row in curs:
				return_str = ''
				for cell in row:
					return_str += str(cell) + col_end
				return_list.append(return_str.strip())
	return return_list

# lazy connect once to redshift
redshift = None  

def lambda_handler(event, context):
	global redshift 
	if redshift is None or redshift.closed:
		print('connecting to redshift')
		redshift = connect_to_redshift('red')
		print(redshift)
	for sql_query in sql_query_list:
		print(sql_query)
		lock_clearing_list = sql_to_list(redshift, sql_query)
		if lock_clearing_list and len(lock_clearing_list) > 0:
			for lock_clearing_sql in lock_clearing_list:
				try:
					print(lock_clearing_sql)
					locks_cleared_str = sql_to_str(redshift, lock_clearing_sql).strip()
					print(locks_cleared_str)
				except Exception as e:
					print("Error: "+ ' : '.join(str(a) for a in e.args) )
		else:
			print("nothing found to clear")

if __name__ == "__main__":
	lambda_handler(event={}, context={})
