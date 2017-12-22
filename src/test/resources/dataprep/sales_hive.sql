CREATE TABLE sales
(
    OrderDate TIMESTAMP,
    Category STRING,
    City STRING,
    Country STRING,
    CustomerName STRING,
    OrderId STRING,
    PostalCode STRING,
    ProductName STRING,
    Quantity STRING,
    Region STRING,
    Segment STRING,
    ShipDate STRING,
    ShipMode STRING,
    State STRING,
    `Sub-Category` STRING,
    ShipStatus STRING,
    orderprofitable STRING,
    SalesAboveTarget STRING,
    latitude STRING,
    longitude STRING,
    Discount DOUBLE,
    Profit DOUBLE,
    Sales DOUBLE,
    DaystoShipActual DOUBLE,
    SalesForecast DOUBLE,
    DaystoShipScheduled DOUBLE,
    SalesperCustomer DOUBLE,
    ProfitRatio DOUBLE
)
row format delimited fields terminated by '\t' stored as textfile;

LOAD DATA INPATH '/tmp/sales_tab_delimeter.csv' OVERWRITE INTO TABLE sales;