CREATE TABLE Reviews (
	rating NUMERIC, 
	verified Boolean,
	votes Integer,
	reviewTime CHAR (20),
	reviewerID CHAR (20),
	asin CHAR (20),
	reviewerName CHAR (300),
	reviewText CHAR (40000),
	summary CHAR (10000),
	style CHAR (20000),

	FOREIGN KEY (asin) REFERENCES Item (asin)
);

CREATE TABLE Item (
	asin CHAR (20) PRIMARY KEY,
	title CHAR (300),
	feature CHAR (10000),
	description CHAR (20000),
	price NUMERIC,
	salesRank CHAR (1000),
	brand CHAR (300),
	categories CHAR (10000)
);


CREATE TABLE AlsoBuy (
	asin CHAR (20),
	alsoBuy CHAR (20),
	FOREIGN KEY (asin) REFERENCES Item (asin),
	FOREIGN KEY (alsoBuy) REFERENCES Item (asin)
);

CREATE TABLE AlsoView (
	asin CHAR (20),
	alsoView CHAR (20),
	FOREIGN KEY (asin) REFERENCES Item (asin),
	FOREIGN KEY (alsoView) REFERENCES Item (asin)
);	