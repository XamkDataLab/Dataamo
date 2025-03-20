# Yritysten IPR-omaisuus ja rahoitus

Tietopankki kerää yhteen ajantasaiset tiedot Suomalaisita yrityksistä, niiden IPR-omaisuudesta sekä niiden saamasta EU- ja Business Finland -rahoituksesta.

Tietoja säilytetään relaatiotietokannassa, tällä hetkellä Azure SQL Server -palvelimella. Taulujen tietosisältöjen tarkemmat kuvaukset ovat IPR Suomi -Teams-ympäristössä, DDL-muodossa sekä relaatiokaaviona. Siirto Xamkin oman palvelimen Postgres-kantaan mahdollinen loppuvuonna 2024.

---

## Sisältö

- [IPR-omaisuus](#ipr-omaisuus)
- [TKI-rahoitus](#rahoitus)
- [Datalähteet](#datalähteet)

### IPR-omaisuus

Yritysten perustiedot haetaan YTJ:stä, PRH:n maksetun API-rajapinnan kautta. Koodia tähän on kansiossa /ipr/<br>
Taulu: *companies*<br>

Patentit. Tiedot haetaan lens.org-palvelusta maksetun API-rajapinnan kautta. Koodia tähän on kansiossa /comparepatens/<br>
Taulut: patents (patenttihakemukset), applicants (tietoja hakijoista) sekä families.<br>

Patenteissa ei ole mukana y-tunnusta, mutta se haetaan companies-taulusta yrityksen nimen perusteella osaksi patents-taulua. Yritysten nimien väärinkirjoitettuja muotoja on kerätty erilliseen tauluun. Kerääminen on tehty osittain puoliautomaattisesti rapidfuzz-kirjaston ja ChatGPT-API-rajapinnan kautta sekä lisäksi osittain Excel-avusteisesti ja manuaalisesti luokittelemalla. Koodia aliaksien selvittämiseen löytyy tiedostosta /ipr/find_aliases.py (tulossa).
Taulu: name_variants

Yrityksillä on myös rinnakkaistoiminimiä (seconary names) ja aputoiminimiä (trade names). Nämä on kerätty erillisiin tauluihin ytj-api-koodin avulla.
Taulut:  secondary_names ja trade_names

### TKI-rahoitus

Business Finland -rahoitusta. Osittaista dataa haettu BF:n web-sivuilta.<br>
Taulu: business_finland

### Datalähteet

Yritysten ja patenttien datalähteitä mainittu yllä.

#### Projektidata

EU:n rakennerahaston ESR- ja EAKR-projektit kaudelta 2014-2020<br>
Rakennerahaston yhteyshenkilöltä saatu Excel-tiedosto, tallennettu Dataamon Teams-kansioon, sisältää tiedon yhteishankkeista<br>
Hankekuvaukset lisäksi noudettu palvelusta https://www.eura2014.fi/rrtiepa/<br>
Taulu: projektit_eura_2014<br>
Tietoja ei tarvitse päivittää.

EU:n rakennerahaston ESR+- ja EAKR-projektit kaudelta 2021-2027<br>
EU:n alue- ja rakennepolitiikan hanketietopalvelu ohjelmakaudelle 2021—2027: https://eura2021.fi/tiepa<br>
Taulu: projektit_eura_2021<br>
Tiedot noudettava Excel-muodossa, päivitys noin kahden kuukauden välein.
