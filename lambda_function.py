import json
import mysql.connector
import datetime 
from botocore.exceptions import ClientError

# resources comes from API Gateway
status_check_path = '/status'
users_path = '/users'
user_path = '/user'
expenses_path = '/expenses'
expense_path = '/expense'
expenses_monthly = '/monthly'
user_expenses = '/user/expenses'

# Credentials for connecting to the database 
mydb= mysql.connector.connect(
    host="tectown-backend-q1-2024.c1s0muoa0qc4.us-east-1.rds.amazonaws.com",
    user="admin",
    password="Tectown1!",
    database="efrem_tectown"
    )
mycursor = mydb.cursor()

def lambda_handler(event, context):
 
 response = None
 # try and catch block to filter the path and method coming   
 try:
  # Variables to hold http method and the resource path
  http_method = event.get('httpMethod')
  path = event.get('path')
  print(path)
  print(http_method)
  
  # If statement for filtering the path and http method 
  
  # Check the service working or not 
  if http_method == 'GET' and path == status_check_path:
    response = build_response(200, 'Service is Operational')
    
  # GET Method with users path
  elif http_method == 'GET' and path == users_path:
    if event['queryStringParameters'] == None:
     response = get_users(5,0)
    else:
     response = get_users(event['queryStringParameters']['limit'],event['queryStringParameters']['offset'])
  # GET Method with expenses path 
  elif http_method == 'GET' and path == expenses_path:
    if event['queryStringParameters'] == None:
     response = get_expenses(5,0)
    else:
     response = get_expenses(event['queryStringParameters']['limit'],event['queryStringParameters']['offset'])
  elif http_method == 'GET' and path == user_path:
    user_id = event['queryStringParameters']['id']
    print(user_id)
    response = get_user(user_id)
  elif http_method == 'POST' and path == user_path:
    
    body =json.loads(event['body'])
    print(body)
    response = save_user(body)
  elif http_method == 'PATCH' and path == user_path:
    body = json.loads(event['body'])
    print(body['updateKey'])
    print(body['updateValue'])
    response = modify_user(body['userId'],body['updateKey'],body['updateValue'])

  elif http_method == 'GET' and path == expenses_monthly:
    if event['queryStringParameters'] == None:
     monthName = None
     response = get_monthly_report(monthName)
    else:
     monthName = event['queryStringParameters']['month']
     response = get_monthly_report(monthName)
  else:
     response = build_response(404, "NOT FOUND")
 # If exception happens 
 except Exception as e:
  response = build_response(400, f"Error Processing Request {e}")
 
 #Close Session. 
 mycursor.close()
 mydb.close()
 
 # Return Value to api  
 return response
    
    
  
###################################### Function to get Users with Limit and Offset ##############################    
def get_users(limit,offset):
    stmt = f"SELECT * From users LIMIT {limit} OFFSET {offset}"
    mycursor.execute(stmt)
    result = mycursor.fetchall()
    if result:
      table_data = []
      for row in result:
        table_data.append({
          'name': row[1],
          'email': row[2],
          'address': row[3],
          'phone_no': row[4]
        })
      
    else:
      # Sql statement to create table users
      sql = " create table users ( \
          userId int auto_increment primary key, \
          full_name varchar(200) NOT NULL, \
          email varchar(200) NOT NULL UNIQUE, \
          Address  varchar(1000), \
          phone_no varchar(200) UNIQUE ) \
          "
      # Execute sql
      mycursor.execute(sql)
      # Print confirmation 
      print("New table User created")

    return build_response(200, table_data)
############################### End of Functon Users #############################################################

###################################### Function to get expenses with Limit and Offset ##############################    
def get_expenses(limit,offset):
  
    stmt = f"SELECT * From expenses LIMIT {limit} OFFSET {offset}"
    
    mycursor.execute(stmt)
    result = mycursor.fetchall()
    
    if result:
      table_data = []
      for row in result:
        print(row[3])
        table_data.append({
          'Category': row[1],
          'Amount': row[2],
          'Expense Date': row[3].strftime("%d-%m-%Y"),
          'Cost': row[4]
        })
      
    else:
      table_data = []
    print(table_data)

    return build_response(200, table_data)
############################### End of Functon expenses #####################################################

##############################Function for Selecting a single User from the database#########################
  
def get_user(id):
    
    stmt = f"SELECT * From users where userId = {id}"
    
    mycursor.execute(stmt)
    row = mycursor.fetchone()
    print(row)
    if row:
      table_data = []
      table_data.append({
          'Full Name': row[1],
          'Email': row[2],
          'Address': row[3],
          'Phone Number': row[4]
        })
      
    else:
      table_data = []
    print(table_data)

    return build_response(200, table_data)
############################### End of a function to find a user  #####################################################

############################### Function To get Monthly consumed amount ###############################################
def get_monthly_report(monthName):
    print(monthName)
    # Sql statement to update table expenses 
    if monthName == None:
     mycursor = mydb.cursor()
     # Sql statement to update table expenses 
     sql = "SELECT \
        users.userId, \
        users.full_name, \
        MONTHNAME(expenses.expenseDate) \
        SUM(expenses.currency) \
        from users INNER JOIN expenses \
        ON users.userID = expenses.user \
        GROUP BY users.userId, MONTHNAME(expenses.expenseDate)"
    else:
     mycursor = mydb.cursor()
     sql = f'SELECT \
        users.userId, \
        users.full_name, \
        MONTHNAME(expenses.expenseDate), \
        SUM(expenses.currency) \
        from users INNER JOIN expenses \
        ON users.userID = expenses.user \
        Where MONTHNAME(expenses.expenseDate) = "{monthName}" \
        GROUP BY users.userId, MONTHNAME(expenses.expenseDate)'
    print(sql)
    # Exceute sql statement 
    mycursor.execute(sql)
    print("Number One")
    # Fetch from the executed sql statement
    myresult = mycursor.fetchall()
    print(myresult)
    result = []
    # Print Confirmation
    for row in myresult:
        result.append({
          'User Id': row[0],
          'Full Name': row[1],
          'Month': row[2],
          'Total': row[3]
        })
    print(result)   
      
   

    return build_response(200, result)
    
############################### End of get_monthly_report##############################################################

############################## Function for for saving User to the database #############################
def save_user(request_body):
  print(request_body)
  print("11111111111111")
  try:
    # To check Wether table users is available or not
    stmt = "SHOW TABLES LIKE 'users'"
    mycursor.execute(stmt)
    result = mycursor.fetchone()

    # To prepare to the value to insert to the database 
    val = []
    if result:
     for x in request_body:
      val.append((x["name"], x["email"], x["address"], x["phone"]),)
      print(val)
     # Sql statement to insert data to the database  
     sql="Insert into users (full_name,email,Address,phone_no) values (%s, %s, %s, %s)"
     print("11111111111111")
     mycursor.executemany(sql,val)
     mydb.commit()  
    # If table Users not found 
    else:
     body = "Table users doesn't found"
    
    
    
    body = {
      'Operation': 'SAVE',
      'Message': 'SUCCESS',
      'Item': request_body
    }
    return build_response(201,body)
  except ClientError as e:
    return build_response(400, e.response['Error']['Message'])


############################## End of function save_user(body)###########################################
  
############################## Function For updater User #################################################
def modify_user(userId, updateKey, updateValue):
  print(updateKey)
  print(updateValue)
  sql = f"UPDATE users SET {updateKey}={updateValue} WHERE userID={userId};"
  print(sql)
  mycursor.execute(sql)
  mydb.commit()
  sql = f"select * from users where userId={userId}"
  result = mycursor.fetchone(sql)

  body = {
      'Operation': 'Update',
      'Message': 'SUCCESS',
      "User": result
      
  }

  return build_response(200, body)



############################## Function for building responses to API #######################################
def build_response(status_code, body):
    return {
     "statusCode": status_code,
	 "body": json.dumps(body),
	 "headers": {
	     "Content-Type": "application/json"
		   }
   	}
   	
################################ End of Build response ####################################################

