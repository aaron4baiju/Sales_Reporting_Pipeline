create table web_sales(
    CustomerID int primary key,
    CustomerName varchar(30),
    Region varchar(20),
    Product varchar(50),
    OrderDate Date,
    LastUpdated DateTime
);

create table pos_orders(
    OrderID int primary key,
    CustomerID int,
    CustomerName varchar(30),
    Region varchar(20),
    Product varchar(50),
    Quantity int,
    Price decimal(2),
    OrderDate Date,
    LastUpdated DateTime
)

