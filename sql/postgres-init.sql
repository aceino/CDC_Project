create table if not exists customers( 
    id serial primary key , 
    name varchar(100) ,
    email varchar(100) ,
    address text, 
    created_at timestamp default now(),
    updated_at timestamp default now()
);

alter table customers replica identity full; 

create table if not exists products( 
    id serial primary key, 
    name varchar(100) not null , 
    category varchar(100) not null, 
    price numeric(10,2 ) not null, 
    stock_quantity int default 0, 
    created_at timestamp default now(),
    updated_at timestamp default now()
);

alter table products replica identity full; 

create table if not exists orders ( 
    id serial primary key, 
    customer_id int references customers(id),
    total_amount numeric(10, 2) not null, 
    status varchar(20) not null default 'PENDING', -- DELIVERED, PAID, CANCELLED, SHIPPED
    created_at timestamp default now(), 
    updated_at timestamp default now()
);

alter table orders replica identity full;

create table if not exists order_items( 
    id serial primary key, 
    order_id int references orders(id) on delete cascade, 
    product_id int references products(id) on delete set null, 
    quantity int not null, 
    price numeric(10,2) not null, 
    total_amount numeric(10,2) not null, 
    created_at timestamp default now() 
);

alter table order_items replica identity full;

-- enable the publication 
create publication cdc_publication for table customers,products,orders,order_items;