--
CREATE TABLE sellers
(
  seller_id     integer     NOT NULL, 
  seller_name   varchar(50) NOT NULL,
  category_id 	varchar(15) NOT NULL,
  location 	varchar(100),
  zip		integer    NOT NULL,
  contact	varchar(50) NOT NULL,
  CONSTRAINT PK_sellers PRIMARY KEY
  (
    seller_id
  )
);


CREATE TABLE items
(
  item_id     integer     NOT NULL, 
  item_name   varchar(50) NOT NULL,
  seller_id   integer 	  NOT NULL,
  price       float   	  NOT NULL,
  category_id    varchar(15),
);
PARTITION TABLE items ON COLUMN item_id;

--PARTITION TABLE items ON COLUMN seller_id;
CREATE INDEX item_id_hash_index ON items(item_id);

CREATE TABLE categories
(
  category_id varchar(15) NOT NULL,
  category_name varchar(40) NOT NULL
);

-- stored procedures
CREATE PROCEDURE FROM CLASS salebroker.procedures.FindNearerSellersByItemId;
--CREATE PROCEDURE FROM CLASS salebroker.procedures.FindNearerSellersByItemName;
CREATE PROCEDURE FROM CLASS salebroker.procedures.deleteFromItems;
PARTITION PROCEDURE FindNearerSellersByItemId ON TABLE items COLUMN item_id;
PARTITION PROCEDURE deleteFromItems ON TABLE items COLUMN item_id;

