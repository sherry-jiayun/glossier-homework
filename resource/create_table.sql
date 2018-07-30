CREATE TABLE sherry_homework.order_summary(
	id BIGINT,
	email VARCHAR(50),
	total_price DECIMAL,
	financial_status VARCHAR(20),
	status VARCHAR(20),
	last_update_time DATE,
	order_status_url VARCHAR(200),
	items_number INTEGER,
	PRIMARY KEY (id)
);

CREATE TABLE sherry_homework.order_detail(
	id BIGINT references sherry_homework.order_summary(id) ON DELETE CASCADE,
	closed_at DATE,
	created_at DATE,
	updated_at DATE,
	number BIGINT,
	note TEXT,
	token VARCHAR(50),
	gateway VARCHAR(20),
	test BOOLEAN,
	subtotal_price DECIMAL,
	total_weight DECIMAL,
	total_tax DECIMAL,
	taxes_included BOOLEAN,
	currency VARCHAR(10),
	confirmed VARCHAR(10),
	total_discounts DECIMAL,
	total_line_items_price DECIMAL,
	cart_token VARCHAR(50),
	buyer_accepts_marketing BOOLEAN,
	name TEXT,
	referring_site VARCHAR(100),
	landing_site VARCHAR(100),
	cancelled_at VARCHAR(100),
	cancel_reason TEXT,
	tags TEXT,
	
	PRIMARY KEY (id)
);

CREATE TABLE sherry_homework.user(
	user_id BIGINT,
	phone VARCHAR(15),
	contact_email VARCHAR(50),

	PRIMARY KEY (user_id)
);

CREATE TABLE sherry_homework.user_device(
	user_id BIGINT references sherry_homework.user(user_id) ON DELETE CASCADE,
	location_id BIGINT,
	source_identifier VARCHAR(20),
	source_url VARCHAR(100),
	processed_at DATE,
	device_id INTEGER,
	customer_locale VARCHAR(20),
	app_id INTEGER,
	browser_ip VARCHAR(10),
	landing_site_ref VARCHAR(20),
	PRIMARY KEY (user_id,device_id)
);

CREATE TABLE sherry_homework.item(
	id BIGINT,
	variant_id BIGINT,
	product_id BIGINT,
	PRIMARY KEY (id)
);

CREATE TABLE sherry_homework.order_payment(
	order_id BIGINT references sherry_homework.order_summary(id) ON DELETE CASCADE,
	order_number BIGINT,
	total_price_usd DECIMAL,
	checkout_token VARCHAR(20),
	reference VARCHAR(200),
	processed_at DATE,
	source_name VARCHAR(20),
	fulfillment_status VARCHAR(20),
	processing_method  VARCHAR(20),
	checkout_id VARCHAR(20),
	PRIMARY KEY(order_id, order_number)
);

CREATE TABLE sherry_homework.order_user(
	order_id BIGINT references sherry_homework.order_summary(id) ON DELETE CASCADE,
	user_id BIGINT,
	device_id BIGINT,
	PRIMARY KEY (order_id),
	FOREIGN KEY (user_id, device_id) REFERENCES sherry_homework.user_device(user_id,device_id)
);

CREATE TABLE sherry_homework.order_item(
	item_id BIGINT references sherry_homework.item(id) ON DELETE CASCADE,
	variant_id BIGINT,
	quantity INTEGER,
	product_id BIGINT,
	order_id BIGINT references sherry_homework.order_summary(id)
);