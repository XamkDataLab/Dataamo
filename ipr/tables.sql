-- DA_database.dbo.companies definition

CREATE TABLE companies (
	business_id nvarchar(9) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	company nvarchar(300) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
	company_form varchar(40) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	main_industry nvarchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	postal_code nvarchar(5) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	postal_area nvarchar(50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	operations_in_countries nvarchar(200) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	status nvarchar(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	company_basename nvarchar(300) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	checked date NULL,
	company_registration_date date NULL,
	CONSTRAINT PK_companies PRIMARY KEY (business_id)
);