import psycopg2
from urllib.parse import urlparse
import io
import zipfile
import boto3
import json

URL='' # SOURCE DATA URL
POSTGRE = '' # DATABASE CONNECTION

# object table
ORDER_LIST = list()
ORDER_DETAIL_LIST = list()
ORDER_PAYMENT_LIST = list()
USER_LIST = list()
USER_DEVICE = list()
ITEM_LIST = list()

# connection table
ORDER_USER_LIST = list()
ORDER_ITEM_LIST = list()


class Order(object):
	def __init__(self,args):
		super(Order, self).__init__()
		self.id = args['id'] # for both PRIMARY KEY 
		self.email = args['email'] 
		self.closed_at = args.get('closed_at',None)
		self.created_at = args.get('created_at',None)
		self.updated_at = args.get('updated_at',None)
		self.order_status_url = args.get('order_status_url', None) # for summary table
		self.number = args.get('number',None)
		self.note = args.get('note',None)
		self.token = args.get('token',None)
		self.gateway = args.get('gateway',None)
		self.test = args.get('test',None)
		self.total_price = args.get('total_price',None) # for summary table
		self.subtotal_price = args.get('subtotal_price',None)
		self.total_weight = args.get('total_weight',None)
		self.total_tax = args.get('total_tax',None)
		self.taxes_included = args.get('taxes_included',None)
		self.currency = args.get('currency',None)
		self.financial_status = args.get('financial_status',None) # for summary table
		self.confirmed = args.get('confirmed',None)
		self.total_discounts = args.get('total_discounts',None)
		self.total_line_items_price = args.get('total_line_items_price',None)
		self.cart_token = args.get('cart_token',None)
		self.buyer_accepts_marketing = args.get('buyer_accepts_marketing',None)
		self.name = args.get('name',None)
		self.referring_site = args.get('referring_site',None)
		self.landing_site = args.get('landing_site',None)
		self.cancelled_at = args.get('cancelled_at',None)
		self.cancel_reason = args.get('cancel_reason',None)
		self.total_price_usd = args.get('total_price_usd',None) # for check out
		self.checkout_token = args.get('checkout_token',None) # for check out
		self.reference = args.get('reference',None) # for check out
		self.processed_at = args.get('processed_at',None) # for checkout
		self.order_number = args.get('order_number',None) # for checkout
		self.processing_method = args.get('processing_method',None) # for checkout
		self.checkout_id = args.get('checkout_id', None)  # for checkout id
		self.source_name = args.get('source_name',None) # for check out
		self.fulfillment_status = args.get('fulfillment_status', None) # for check out
		self.tags = args.get('tags', None)
		self.total_discount = args.get('total_discount',None)
		self.line_items = args.get('line_items',None)

		# for connection purpose
		self.user_id = args.get('user_id')
		self.device_id = args.get('device_id')

		# calculate
		self.items_number = len(self.line_items) # for summary table 
		self.status, self.last_update_time = self.check_status() # for summary table

	def check_status(self):
		'''
		Output: status
		'''
		if self.closed_at:
			return ['Closed',self.closed_at]
		if self.cancelled_at:
			return ['Cancelled',self.cancelled_at]
		last_update_time = self.updated_at if self.updated_at else self.created_at
		return ['In process', last_update_time]

	def return_order_summary(self):
		'''
		Output: return row in order summary table
		'''
		order_row = list()
		order_row.append (self.id) # PRIMARY KEY 
		order_row.append (self.email)
		order_row.append (self.total_price)
		order_row.append (self.financial_status)
		order_row.append (self.status)
		order_row.append (self.last_update_time)
		order_row.append (self.order_status_url)
		order_row.append (self.items_number)
		order_row = [str(x) for x in order_row]
		return order_row

	def return_payment_info(self):
		'''
		Output: return row in payment info table
		'''
		payment_row = list()
		payment_row.append(self.id) # foreign key 
		payment_row.append(self.order_number) # PRIMARY KEY 
		payment_row.append(self.total_price_usd)
		payment_row.append(self.checkout_token)
		payment_row.append(self.reference)
		payment_row.append(self.processed_at)
		payment_row.append(self.source_name)
		payment_row.append(self.fulfillment_status)
		payment_row.append(self.processing_method)
		payment_row.append(self.checkout_id)
		return payment_row

	def return_order_detail(self):
		'''
		Output: return row order detail table
		'''
		order_detail_row = list()
		order_detail_row.append(self.id) # primary key
		order_detail_row.append(self.closed_at)
		order_detail_row.append(self.created_at) 
		order_detail_row.append(self.updated_at)
		order_detail_row.append(self.number)
		order_detail_row.append(self.note)
		order_detail_row.append(self.token)
		order_detail_row.append(self.gateway)
		order_detail_row.append(self.test)
		order_detail_row.append(self.subtotal_price)
		order_detail_row.append(self.total_weight)
		order_detail_row.append(self.total_tax)
		order_detail_row.append(self.taxes_included)
		order_detail_row.append(self.currency)
		order_detail_row.append(self.confirmed)
		order_detail_row.append(self.total_discounts)
		order_detail_row.append(self.total_line_items_price)
		order_detail_row.append(self.cart_token)
		order_detail_row.append(self.buyer_accepts_marketing)
		order_detail_row.append(self.name)
		order_detail_row.append(self.referring_site)
		order_detail_row.append(self.landing_site)
		order_detail_row.append(self.cancelled_at)
		order_detail_row.append(self.cancel_reason)
		order_detail_row.append(self.tags)
		return order_detail_row

	def return_order_item(self):
		'''
		Output: return rows in order item table
		'''
		order_item_rows = list()
		for order_item in self.line_items:
			order_item_row = list()
			order_item_row.append(order_item.get('id')) # foreign key part 1
			order_item_row.append(order_item.get('variant_id',None))
			order_item_row.append(order_item.get('quantity',None))
			order_item_row.append(order_item.get('product_id',None)) # foreign key part 2
			order_item_row.append(self.id) # foreign key 
			order_item_rows.append(order_item_row)
		return order_item_rows 

	def return_order_user(self):
		'''
		Output: return row in order user table
		'''
		order_user = list()
		order_user.append(self.id) # foreign key 
		order_user.append(self.user_id) # foreign key 
		order_user.append(self.device_id) # foreign key 
		return order_user

class User(object):
	def __init__(self,args):
		super(User,self).__init__()

		self.user_id = args['user_id']
		self.location_id = args.get('location_id',None) # for credential check
		self.source_identifier = args.get('source_identifier',None) # for credential check
		self.source_url = args.get('source_url',None) # for credential check
		self.processed_at = args.get('processed_at',None) # for credential check
		self.device_id = args.get('device_id',None) # for credential check
		self.phone = args.get('phone',None)
		self.customer_locale = args.get('customer_locale',None)
		self.app_id = args.get('app_id', None) # for credential check
		self.browser_ip = args.get('browser_ip', None) # for credential check
		self.landing_site_ref = args.get('landing_site_ref',None) # for credential check
		self.contact_email = args.get('contact_email', None)

	def return_user_basic(self):
		'''
		Ourput: row in user basic table
		'''
		user_basic = list()
		user_basic.append(self.user_id)
		user_basic.append(self.phone)
		user_basic.append(self.contact_email)
		return user_basic

	def return_user_device(self):
		'''
		Output: return device for user
		'''
		user_device = list()
		user_device.append(self.user_id) # primary key part 1
		user_device.append(self.location_id)
		user_device.append(self.source_identifier)
		user_device.append(self.source_url)
		user_device.append(self.processed_at)
		user_device.append(self.device_id) # primary key part 2
		user_device.append(self.customer_locale)
		user_device.append(self.app_id)
		user_device.append(self.browser_ip)
		user_device.append(self.landing_site_ref)
		return user_device


class Item(object):
	def __init__(self,args):
		super(Item,self).__init__()
		self.all_items = args['line_items']

	def return_items(self):
		'''
		Output: rows in item table
		'''
		items = list()
		for item in self.all_items:
			item_row = list()
			item_row.append(item.get('id',None)) # primary key part 1
			item_row.append(item.get('variant_id',None))
			item_row.append(item.get('product_id',None)) # primary key part 2
			items.append(item_row)
		return items

def reset():
	global ORDER_LIST
	global ORDER_DETAIL_LIST
	global ORDER_PAYMENT_LIST
	global ORDER_ITEM_LIST
	global ORDER_USER_LIST
	global USER_LIST
	global USER_DEVICE
	global ITEM_LIST

	ORDER_LIST = list()
	ORDER_DETAIL_LIST = list()
	ORDER_PAYMENT_LIST = list()
	ORDER_ITEM_LIST = list()
	ORDER_USER_LIST = list()
	USER_LIST = list()
	USER_DEVICE = list()
	ITEM_LIST = list()

def check_connection():
	try:
		conn = psycopg2.connect(POSTGRE,connect_timeout=3)
		conn.close()
	except:
		print("connection to database failed!")
		exit(0)

def get_zip_file():
	s3 = boto3.resource('s3')
	_, bucket_name, key = urlparse(URL).path.split('/', 2)
	obj = s3.Object(
		bucket_name=bucket_name,
		key=key)
	buffer = io.BytesIO(obj.get()["Body"].read())
	z = zipfile.ZipFile(buffer)
	for filename in z.namelist():
		print ("Process file:",filename,"...",end="")
		reset()
		content = z.read(filename)
		json_parser(content)
		insert_order()
		print ("Done")
		
def json_parser(content):
	global ITEM_LIST
	global ORDER_ITEM_LIST
	obj = json.loads(content)
	orders = obj['orders']
	for order in orders:
		order_object = Order(order)
		ORDER_LIST.append(order_object.return_order_summary())
		ORDER_DETAIL_LIST.append(order_object.return_order_detail())
		ORDER_PAYMENT_LIST.append(order_object.return_payment_info())

		ORDER_USER_LIST.append(order_object.return_order_user())
		ORDER_ITEM_LIST = ORDER_ITEM_LIST + order_object.return_order_item()

		user_object = User(order)
		USER_LIST.append(user_object.return_user_basic())
		USER_DEVICE.append(user_object.return_user_device())

		item_object = Item(order)
		ITEM_LIST = ITEM_LIST + item_object.return_items()

def insert_order():

	conn = psycopg2.connect(POSTGRE)
	cur = conn.cursor()

	db = "sherry_homework.order_summary"
	data_str_insert = b','.join(cur.mogrify('(%s,%s,%s,%s,%s,%s,%s,%s)', row) for row in ORDER_LIST)
	sql_insert = "INSERT INTO " + db + " VALUES "+data_str_insert.decode("utf-8") +" ON CONFLICT (id) DO NOTHING;"
	cur.execute(sql_insert)

	db_summary = "sherry_homework.order_detail"
	data_str_insert_summary = b','.join(cur.mogrify('(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',row) for row in ORDER_DETAIL_LIST)
	sql_insert_detail = "INSERT INTO " + db_summary + " VALUES " + data_str_insert_summary.decode("utf-8") + " ON CONFLICT (id) DO NOTHING;"
	cur.execute(sql_insert_detail)

	db_user = "sherry_homework.user"
	data_str_insert_user = b','.join(cur.mogrify('(%s,%s,%s)',row) for row in USER_LIST)
	sql_insert_user = "INSERT INTO " + db_user + " VALUES " + data_str_insert_user.decode("utf-8") + " ON CONFLICT (user_id) DO NOTHING;"
	cur.execute(sql_insert_user)

	db_user_device = "sherry_homework.user_device"
	data_str_insert_user_device = b','.join(cur.mogrify('(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',row) for row in USER_DEVICE)
	sql_insert_user_device = "INSERT INTO " + db_user_device + " VALUES " + data_str_insert_user_device.decode("utf-8") + " ON CONFLICT (user_id,device_id) DO NOTHING;"
	cur.execute(sql_insert_user_device)

	db_item = "sherry_homework.item"
	data_str_insert_item = b','.join(cur.mogrify('(%s,%s,%s)',row) for row in ITEM_LIST)
	sql_insert_item = "INSERT INTO " + db_item + " VALUES " + data_str_insert_item.decode("utf-8") + " ON CONFLICT (id) DO NOTHING;"
	cur.execute(sql_insert_item)

	db_order_payment = "sherry_homework.order_payment"
	data_str_insert_order_payment = b','.join(cur.mogrify('(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',row) for row in ORDER_PAYMENT_LIST)
	sql_insert_order_payment = "INSERT INTO " + db_order_payment + " VALUES " + data_str_insert_order_payment.decode("utf-8") + " ON CONFLICT (order_id,order_number) DO NOTHING;"
	cur.execute(sql_insert_order_payment)

	db_order_user = "sherry_homework.order_user"
	data_str_insert_order_user = b','.join(cur.mogrify('(%s,%s,%s)',row) for row in ORDER_USER_LIST)
	sql_insert_order_user = "INSERT INTO " + db_order_user + " VALUES " + data_str_insert_order_user.decode("utf-8") + " ON CONFLICT (order_id) DO NOTHING;"
	cur.execute(sql_insert_order_user)

	db_order_item = "sherry_homework.order_item"
	data_str_insert_order_item = b','.join(cur.mogrify('(%s,%s,%s,%s,%s)',row) for row in ORDER_ITEM_LIST)
	sql_insert_order_item = "INSERT INTO " + db_order_item + " VALUES " + data_str_insert_order_item.decode("utf-8")+";"
	cur.execute(sql_insert_order_item)

	conn.commit()
	conn.close()


if __name__ == "__main__":

	with open('credential.txt') as f:
		credentials = [x.strip().split('=') for x in f.readlines()]

	for c in credentials:
		if c[0] == 's3url': URL = c[1]
		if c[0] == 'postgredatabase': POSTGRE = POSTGRE + 'dbname=' + c[1] + ' '
		if c[0] == 'user': POSTGRE = POSTGRE + 'user=' + c[1] + ' '
		if c[0] == 'password': POSTGRE = POSTGRE + 'password=' + c[1] + ' '
		if c[0] == 'host': POSTGRE = POSTGRE + 'host=' + c[1]

	check_connection()
	get_zip_file()