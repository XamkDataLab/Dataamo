-- dbo.ipr_suomi_dbinfo source

CREATE VIEW ipr_suomi_dbinfo AS
SELECT 
    'Viimeisin yrityksen rekisteröimispäivä' AS indicator, 
    CAST((SELECT MAX(yrityksen_rekisteröimispäivä) FROM yritykset) AS CHAR) AS value
UNION ALL
SELECT 
    'Yrityksiä tietokannassa' AS indicator, 
    CAST((SELECT COUNT(*) FROM yritykset) AS CHAR) AS value;
