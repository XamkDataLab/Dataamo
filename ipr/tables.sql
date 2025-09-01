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

-- DA_database.dbo.projects_eura2021 definition

CREATE TABLE projects_eura2021 (
	code varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	group_code varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	fund varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	eakr_jtf_subtype varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	tl varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	et varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	project_name varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	implementing_organization varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	implementing_organization_bid varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	funding_authority varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	status varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	start_date datetime NULL,
	end_date datetime NULL,
	planned_eu_and_state_funding float NULL,
	planned_public_funding float NULL,
	planned_total_funding float NULL,
	actual_eu_and_state_funding float NULL,
	actual_public_funding float NULL,
	actual_total_funding float NULL,
	actual_eu_funding_amount float NULL,
	funding_field varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	region varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	implementation_location_zipcode varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	implementation_location_region varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	beneficiary_zipcode varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	beneficiary_region varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	support_form varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	regional_implementation_mechanism_and_priority varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	economic_activity varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	secondary_theme_esr_only varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	gender_equality varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	baltic_sea_region_strategy varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	project_implementation_summary_according_to_plan varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	project_description_address varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	project_website_address varchar(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
);

-- :TODO: Azure to PostgreSQL conversion
